use super::helpers::deterministic_graph_id;
use super::reviewed_goal_fixture::*;
use super::*;
use harness_contract::agent_action::{
    AgentActionStatus, AgentActorBinding, AgentActorKind, AgentInviteInput, ArtifactCommitInput,
    TaskClaimInput, TaskOneShotDeliverable, TaskPublishInput, TaskReviewDecision, TaskSubmitInput,
    TeamCreateInput,
};
use harness_contract::skill::{
    SkillAdapterKind, SkillCapabilityProfile, SkillKind, SkillLifecycleStatus, SkillRiskLevel,
};

#[tokio::test]
async fn r1_native_backend_cancel_retains_physical_scope_and_rejects_old_owner() {
    exercise_r1_backend_resource_lifetime_for_test(R1ResourceBackend::Native).await;
}

fn copy_prepared_conclusion(
    prepared: &crate::execution_core::goal::PreparedProgramConclusion,
) -> crate::execution_core::goal::PreparedProgramConclusion {
    crate::execution_core::goal::PreparedProgramConclusion {
        goal: prepared.goal.clone(),
        expected_stream_revision: prepared.expected_stream_revision,
        event: prepared.event.clone(),
        gaps: prepared.gaps.clone(),
        policy_sources: prepared.policy_sources.clone(),
    }
}

fn set_review_input_case(review: &mut harness_contract::goal::ObjectiveReviewRecord, case: &str) {
    match case {
        "missing-review" => unreachable!(),
        "missing-verification" => review.verification = None,
        "missing-inputs" => review.verification.as_mut().unwrap().input_set = None,
        "zero" | "legacy" | "unknown" => {
            review
                .verification
                .as_mut()
                .unwrap()
                .input_set
                .as_mut()
                .unwrap()
                .version = match case {
                "zero" => 0,
                "legacy" => 1,
                _ => u32::MAX,
            };
        }
        "gap" => review.decision = "gap".into(),
        _ => unreachable!(),
    }
}

fn assert_unsupported_review_inputs(
    services: &Arc<RuntimeServices>,
    original: &AgentActionEnvelope,
    donor: &crate::agentic::review_evidence::IndependentReview,
    action_kind: &str,
) {
    let actions = services.agent_action_service();
    let store = services.event_store();
    let program_stream = format!("agentic-program:{}", original.actor.program_id);
    let goal_id = format!(
        "goal:{}",
        original.actor.root_execution_id.as_ref().unwrap()
    );
    let original_goal = services.goal_store().projection(&goal_id).unwrap().unwrap();
    let session_stream = format!("session:{}", original.actor.session_id);
    let host_calls = store
        .list_stream(&session_stream)
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == "tool.invocation.completed")
        .count();
    for case in [
        "missing-verification",
        "missing-inputs",
        "zero",
        "legacy",
        "unknown",
    ] {
        for entry in ["internal", "goal-event"] {
            let mut envelope = original.clone();
            envelope.action_id = format!("unsupported-{action_kind}-{case}-{entry}");
            if let AgentAction::TaskReview(input) = &mut envelope.action {
                let program = actions.project(&envelope.actor.program_id).unwrap();
                assert_eq!(
                    program.tasks[&input.task_ref].status,
                    crate::AgenticTaskStatus::Submitted
                );
                if action_kind != "task-accept" {
                    input.decision = if action_kind == "task-rework" {
                        TaskReviewDecision::Rework
                    } else {
                        TaskReviewDecision::Challenge
                    };
                    input.operator_requirement = Some(original_goal.goal.objective.clone());
                    input.reason = program.tasks[&input.task_ref].acceptance_checks.join("; ");
                }
            }
            let mut proof = donor.verification.clone();
            match case {
                "missing-verification" => {}
                "missing-inputs" => proof.input_set = None,
                _ => {
                    proof.input_set.as_mut().unwrap().version = match case {
                        "zero" => 0,
                        "legacy" => 1,
                        _ => u32::MAX,
                    }
                }
            }
            let proof = (case != "missing-verification").then_some(&proof);
            let before_cursor = store.durable_commit_cursor().unwrap();
            let before_program = store.stream_revision(&program_stream).unwrap();
            let rejected = if entry == "goal-event" {
                let event = crate::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: format!("goal:{goal_id}"),
                        scope: crate::RuntimeEventScope::Program,
                        kind: "goal.revised".into(),
                        status: None,
                        actor: None,
                        refs: vec![],
                        payload: serde_json::json!({"goal":original_goal.goal}),
                    },
                    idempotency_key: Some(envelope.action_id.clone()),
                    schema_version: 1,
                };
                actions
                    .apply_with_goal_event(
                        &envelope,
                        event.event.stream_id.clone(),
                        original_goal.stream_revision,
                        event,
                        proof,
                    )
                    .unwrap()
            } else if let Some(proof) = proof {
                actions.apply_verified_review(&envelope, proof).unwrap()
            } else {
                actions.apply(&envelope).unwrap()
            };
            assert_eq!(
                rejected.status,
                AgentActionStatus::Rejected,
                "{action_kind}/{case}/{entry}"
            );
            assert_eq!(rejected.error.unwrap().code, "unsupported_review_inputs");
            assert_eq!(store.durable_commit_cursor().unwrap(), before_cursor);
            assert_eq!(
                store.stream_revision(&program_stream).unwrap(),
                before_program
            );
            assert_eq!(
                services
                    .goal_store()
                    .projection(&goal_id)
                    .unwrap()
                    .unwrap()
                    .goal,
                original_goal.goal
            );
            let after_calls = store
                .list_stream(&session_stream)
                .unwrap()
                .into_iter()
                .filter(|event| event.kind == "tool.invocation.completed")
                .count();
            assert_eq!(after_calls, host_calls);
        }
    }
}

#[tokio::test]
async fn r1_proof_current_apply_rejects_legacy_and_unknown_inputs() {
    with_native_review_before_apply(|services, original, donor| {
        assert_unsupported_review_inputs(services, original, donor, "objective");
        let actions = services.agent_action_service();
        let mut positive = original.clone();
        positive.action_id = "direct-v2-objective-positive".into();
        let mutation = services
            .goal_store()
            .prepare_agentic_objective_action(
                &positive,
                &donor.producers,
                Some(&donor.verification),
            )
            .unwrap();
        let applied = actions
            .apply_with_goal_event(
                &positive,
                mutation.stream_id,
                mutation.expected_stream_revision,
                mutation.event,
                Some(&donor.verification),
            )
            .unwrap();
        assert_eq!(applied.status, AgentActionStatus::Applied, "{applied:?}");
        let mut no_proof_gap = original.clone();
        no_proof_gap.action_id = "legitimate-gap-without-proof".into();
        if let AgentAction::ObjectiveReview(input) = &mut no_proof_gap.action {
            input.decision = harness_contract::agent_action::ObjectiveReviewDecision::Gap;
            input.reason =
                "gap: the reviewed delivery still lacks its named missing deliverable".into();
        }
        let gap = actions.apply(&no_proof_gap).unwrap();
        assert_eq!(gap.status, AgentActionStatus::Applied, "{gap:?}");
    })
    .await;
    for action_kind in ["task-accept", "task-rework", "task-challenge"] {
        with_native_task_review_before_apply(move |services, original, donor| {
            assert_unsupported_review_inputs(services, original, donor, action_kind);
            let actions = services.agent_action_service();
            let mut positive = original.clone();
            positive.action_id = format!("direct-v2-{action_kind}-positive");
            if let AgentAction::TaskReview(input) = &mut positive.action {
                if action_kind != "task-accept" {
                    input.decision = if action_kind == "task-rework" {
                        TaskReviewDecision::Rework
                    } else {
                        TaskReviewDecision::Challenge
                    };
                    input.operator_requirement = Some(
                        services
                            .goal_store()
                            .get(&format!(
                                "goal:{}",
                                original.actor.root_execution_id.as_ref().unwrap()
                            ))
                            .unwrap()
                            .unwrap()
                            .objective,
                    );
                    input.reason = actions.project(&original.actor.program_id).unwrap().tasks
                        [&input.task_ref]
                        .acceptance_checks
                        .join("; ");
                }
            }
            let applied = actions
                .apply_verified_review(&positive, &donor.verification)
                .unwrap();
            assert_eq!(
                applied.status,
                AgentActionStatus::Applied,
                "{action_kind}: {applied:?}"
            );
            let task_ref = match &positive.action {
                AgentAction::TaskReview(input) => input.task_ref.clone(),
                _ => unreachable!(),
            };
            let current = actions.project(&original.actor.program_id).unwrap();
            if action_kind == "task-accept" {
                assert_eq!(
                    current.tasks[&task_ref].status,
                    crate::AgenticTaskStatus::Accepted
                );
                positive.action_id.push_str(":rereview");
                let cursor = services.event_store().durable_commit_cursor().unwrap();
                let repeated = actions.apply(&positive).unwrap();
                assert_eq!(repeated.error.unwrap().code, "task_already_accepted");
                assert_eq!(
                    services.event_store().durable_commit_cursor().unwrap(),
                    cursor
                );
            } else {
                assert_eq!(
                    current.tasks[&task_ref].status,
                    crate::AgenticTaskStatus::Rework
                );
                assert_eq!(current.tasks[&task_ref].review_generation, 1);
            }
        })
        .await;
    }

    for decision in [
        TaskReviewDecision::Accept,
        TaskReviewDecision::Rework,
        TaskReviewDecision::Challenge,
    ] {
        with_native_task_review_async(
            NativeTaskReviewCase::default(),
            move |services, mut envelope, _donor| {
                Box::pin(async move {
                    let actions = services.agent_action_service();
                    let goal_id = format!(
                        "goal:{}",
                        envelope.actor.root_execution_id.as_ref().unwrap()
                    );
                    let goal = services.goal_store().get(&goal_id).unwrap().unwrap();
                    let task_ref = match &mut envelope.action {
                        AgentAction::TaskReview(input) => {
                            input.decision = decision;
                            if decision != TaskReviewDecision::Accept {
                                input.operator_requirement = Some(goal.objective.clone());
                                input.reason =
                                    actions.project(&envelope.actor.program_id).unwrap().tasks
                                        [&input.task_ref]
                                        .acceptance_checks
                                        .join("; ");
                            }
                            input.task_ref.clone()
                        }
                        _ => unreachable!(),
                    };
                    let before = actions.project(&envelope.actor.program_id).unwrap();
                    assert_eq!(
                        before.tasks[&task_ref].status,
                        crate::AgenticTaskStatus::Submitted
                    );
                    envelope.action_id = format!("public-task-{decision:?}");
                    let applied = services.submit_agent_action(&envelope).await.unwrap();
                    assert_eq!(applied.status, AgentActionStatus::Applied, "{applied:?}");
                    let after = actions.project(&envelope.actor.program_id).unwrap();
                    assert_eq!(
                        after.tasks[&task_ref].status,
                        if decision == TaskReviewDecision::Accept {
                            crate::AgenticTaskStatus::Accepted
                        } else {
                            crate::AgenticTaskStatus::Rework
                        }
                    );
                    assert_eq!(
                        after.tasks[&task_ref].review_generation,
                        before.tasks[&task_ref].review_generation
                            + u64::from(decision != TaskReviewDecision::Accept)
                    );
                    assert_public_exact_replay(&services, &envelope, &applied).await;
                    if decision == TaskReviewDecision::Accept {
                        envelope.action_id.push_str(":new-action");
                        let repeated = services.submit_agent_action(&envelope).await.unwrap_err();
                        assert!(
                            repeated.starts_with(
                                "review attempt is not the active Review attempt for Task "
                            ),
                            "{repeated}"
                        );
                        assert!(repeated.contains(&task_ref), "{repeated}");
                        let rejected = actions.apply(&envelope).unwrap();
                        assert_eq!(rejected.status, AgentActionStatus::Rejected);
                        assert_eq!(rejected.error.unwrap().code, "task_already_accepted");
                    }
                })
            },
        )
        .await;
    }
    with_native_review_async(|services, mut envelope, _donor| {
        Box::pin(async move {
            envelope.action_id = "public-objective-gap-without-proof".into();
            if let AgentAction::ObjectiveReview(input) = &mut envelope.action {
                input.decision = harness_contract::agent_action::ObjectiveReviewDecision::Gap;
                input.reason = "gap: the independent objective review has not yet been accepted".into();
            } else {
                unreachable!()
            }
            let goal_id = format!(
                "goal:{}",
                envelope.actor.root_execution_id.as_ref().unwrap()
            );
            assert_eq!(
                services
                    .goal_store()
                    .get(&goal_id)
                    .unwrap()
                    .unwrap()
                    .completion,
                harness_contract::goal::GoalCompletion::Open
            );
            let applied = services.submit_agent_action(&envelope).await.unwrap();
            assert_eq!(applied.status, AgentActionStatus::Applied, "{applied:?}");
            let goal = services.goal_store().get(&goal_id).unwrap().unwrap();
            let review = goal.reviews.last().unwrap();
            assert_eq!(review.decision, "gap");
            assert!(review.verification.is_none());
            assert_public_exact_replay(&services, &envelope, &applied).await;
        })
    })
    .await;
}

async fn assert_public_exact_replay(
    services: &Arc<RuntimeServices>,
    envelope: &AgentActionEnvelope,
    applied: &harness_contract::agent_action::AgentActionObservation,
) {
    let store = services.event_store();
    let goal_id = format!(
        "goal:{}",
        envelope.actor.root_execution_id.as_ref().unwrap()
    );
    let cursor = store.durable_commit_cursor().unwrap();
    let program = services
        .agent_action_service()
        .project(&envelope.actor.program_id)
        .unwrap();
    let goal = services.goal_store().projection(&goal_id).unwrap().unwrap();
    let activity = native_review_activity(services, envelope);
    let replay = services.submit_agent_action(envelope).await.unwrap();
    assert_eq!(replay.status, AgentActionStatus::Applied);
    assert!(replay.duplicate);
    assert_eq!(replay.receipt_id, applied.receipt_id);
    assert_eq!(replay.changed_refs, applied.changed_refs);
    assert_eq!(replay.revision, applied.revision);
    assert_eq!(store.durable_commit_cursor().unwrap(), cursor);
    assert_eq!(
        services
            .agent_action_service()
            .project(&envelope.actor.program_id)
            .unwrap(),
        program
    );
    let after = services.goal_store().projection(&goal_id).unwrap().unwrap();
    assert_eq!(after.goal, goal.goal);
    assert_eq!(after.stream_revision, goal.stream_revision);
    assert_eq!(native_review_activity(services, envelope), activity);
}

fn append_historical_a_for_proof_negative(
    services: &Arc<RuntimeServices>,
    program: &crate::AgenticProgramProjection,
    mut prepared: crate::execution_core::goal::PreparedProgramConclusion,
    event_case: &str,
) -> harness_contract::goal::GoalContract {
    let terminal = prepared.goal.terminal.as_ref().unwrap();
    let verdict = crate::agentic::program::AgenticObjectiveVerdictProjection {
        goal_id: prepared.goal.id.clone(),
        goal_revision: prepared.goal.revision,
        terminal_fence: terminal.terminal_fence.clone(),
        authority_revision: terminal.authority_revision,
        kind: terminal.kind,
    };
    prepared.event.event.payload["goal"] = serde_json::to_value(&prepared.goal).unwrap();
    let stream = format!("agentic-program:{}", program.program_id);
    let mut payload = serde_json::json!({"program_id":program.program_id,"verdict":verdict});
    if event_case == "payload" {
        payload["verdict"]["goal_revision"] = serde_json::json!(u64::MAX);
    }
    if event_case == "fence" {
        payload["verdict"]["terminal_fence"] = serde_json::json!("wrong-fence");
    }
    let event = crate::RuntimeTransactionEventInput {
        event: crate::RuntimeEventInput {
            stream_id: stream.clone(),
            scope: crate::RuntimeEventScope::Program,
            kind: if event_case == "kind" {
                "test.not_a_verdict"
            } else {
                "agentic.objective_verdict_bound"
            }
            .into(),
            status: Some("verified".into()),
            actor: Some("historical_fixture_only".into()),
            refs: vec![],
            payload,
        },
        idempotency_key: Some(if event_case == "missing-key" {
            "historical-a-without-current-key".into()
        } else {
            format!("program-conclusion:{}", terminal.authority_revision)
        }),
        schema_version: 1,
    };
    let history = crate::RuntimeTransactionEventInput {
        event: crate::RuntimeEventInput {
            stream_id: stream.clone(),
            scope: crate::RuntimeEventScope::Program,
            kind: "agentic.objective_verdict_bound".into(),
            status: Some("verified".into()),
            actor: Some("historical_fixture_only".into()),
            refs: vec![],
            payload: serde_json::json!({"program_id":program.program_id,"verdict":verdict}),
        },
        idempotency_key: Some("historical-projection-verdict".into()),
        schema_version: 1,
    };
    services
        .event_store()
        .append_transaction(crate::AppendTransactionRequest {
            transaction_id: format!("historical-a-fixture:{event_case}"),
            expected_streams: vec![
                crate::ExpectedStreamRevision {
                    stream_id: stream,
                    expected_revision: program.revision,
                },
                crate::ExpectedStreamRevision {
                    stream_id: prepared.event.event.stream_id.clone(),
                    expected_revision: prepared.expected_stream_revision,
                },
            ],
            events: vec![prepared.event, event, history],
        })
        .unwrap();
    prepared.goal
}

#[tokio::test]
async fn r1_proof_current_a_and_b_reject_unsupported_required_reviews() {
    for case in [
        "missing-review",
        "missing-verification",
        "missing-inputs",
        "zero",
        "legacy",
        "unknown",
        "gap",
    ] {
        with_native_goal_before_a(move |services, program, prepared, _| {
            let intent = prepared.goal.user_intent_criterion_id.clone().unwrap();
            let selected = prepared
                .goal
                .reviews
                .iter()
                .rposition(|review| review.criterion_ref == intent)
                .unwrap();
            let mut bypass = copy_prepared_conclusion(&prepared);
            if case == "missing-review" {
                bypass
                    .goal
                    .reviews
                    .retain(|review| review.criterion_ref != intent);
            } else {
                set_review_input_case(&mut bypass.goal.reviews[selected], case);
            }
            bypass.event.event.payload["goal"] = serde_json::to_value(&bypass.goal).unwrap();
            let cursor = services.event_store().durable_commit_cursor().unwrap();
            let error = services
                .agent_action_service()
                .commit_program_conclusion(program, bypass)
                .err()
                .expect("direct mutable prepared must not bypass final required review");
            assert!(
                error.to_string().contains("review") || error.to_string().contains("proof"),
                "{error}"
            );
            assert_eq!(
                services.event_store().durable_commit_cursor().unwrap(),
                cursor
            );
            let current = services
                .goal_store()
                .get(&prepared.goal.id)
                .unwrap()
                .unwrap();
            services
                .goal_store()
                .revise(
                    &current.id,
                    current.revision,
                    current.user_sequence + 1,
                    "latest review negative fixture",
                    |goal| {
                        if case == "missing-review" {
                            goal.reviews.retain(|review| review.criterion_ref != intent);
                        } else {
                            let mut latest = goal
                                .reviews
                                .iter()
                                .rev()
                                .find(|review| review.criterion_ref == intent)
                                .unwrap()
                                .clone();
                            latest.review_id.push_str(":latest-negative");
                            set_review_input_case(&mut latest, case);
                            goal.reviews.push(latest);
                        }
                        vec!["reviews".into()]
                    },
                )
                .unwrap();
            let current_program = services
                .agent_action_service()
                .project(&program.program_id)
                .unwrap();
            let rejected = services
                .goal_store()
                .prepare_program_conclusion(&current_program)
                .unwrap();
            assert!(rejected
                .gaps
                .iter()
                .any(|gap| gap.starts_with("original_intent_review_required:")));
            assert!(rejected.goal.terminal.is_none());
            assert!(services
                .agent_action_service()
                .project(&program.program_id)
                .unwrap()
                .objective_verdict
                .is_none());
        })
        .await;
        with_native_goal_before_a(move |services, program, mut prepared, _| {
            let intent = prepared.goal.user_intent_criterion_id.clone().unwrap();
            if case == "missing-review" {
                prepared
                    .goal
                    .reviews
                    .retain(|review| review.criterion_ref != intent);
            } else {
                let review = prepared
                    .goal
                    .reviews
                    .iter_mut()
                    .rev()
                    .find(|review| review.criterion_ref == intent)
                    .unwrap();
                set_review_input_case(review, case);
            }
            let goal = append_historical_a_for_proof_negative(services, program, prepared, "proof");
            assert_native_sources_b_rejected(services, &goal, "committed A");
        })
        .await;
    }
    for case in ["missing-verification", "missing-inputs", "unknown"] {
        with_native_goal_before_a(move |services, program, mut prepared, _| {
            let mut extra = prepared.goal.criteria[0].clone();
            extra.id = "additional-accepted-condition".into();
            extra.status = harness_contract::goal::AcceptanceStatus::Satisfied;
            prepared.goal.criteria.push(extra);
            let mut review = prepared.goal.reviews.last().unwrap().clone();
            review.criterion_ref = "additional-accepted-condition".into();
            review.review_id = "additional-accepted-review".into();
            set_review_input_case(&mut review, case);
            prepared.goal.reviews.push(review);
            let goal = append_historical_a_for_proof_negative(services, program, prepared, "proof");
            let history = services.session_history_reader();
            let error = crate::agentic::review_evidence::committed_proof_sources(
                services.event_store(),
                &goal,
                crate::agentic::review_evidence::ResultContentReaders {
                    artifacts: Some(services.artifact_store().as_ref()),
                    history: history.as_deref(),
                    process_inputs: None,
                },
            )
            .err()
            .expect("all accepted criteria participate, not merely any-v2");
            assert!(
                error.contains("additional-accepted-condition") || error.contains("proof identity"),
                "{error}"
            );
            assert_native_sources_b_rejected(services, &goal, "committed A");
        })
        .await;
    }
    with_native_extra_before_a(|services, program, prepared, _| {
        for status in [
            harness_contract::goal::AcceptanceStatus::Open,
            harness_contract::goal::AcceptanceStatus::Waived,
        ] {
            let mut bypass = copy_prepared_conclusion(&prepared);
            let criterion = bypass
                .goal
                .criteria
                .iter_mut()
                .find(|criterion| criterion.id == "additional-accepted-condition")
                .unwrap();
            criterion.status = status;
            criterion.waiver = None;
            bypass.event.event.payload["goal"] = serde_json::to_value(&bypass.goal).unwrap();
            let cursor = services.event_store().durable_commit_cursor().unwrap();
            let error = services
                .agent_action_service()
                .commit_program_conclusion(program, bypass)
                .err()
                .expect("mutable prepared cannot hide an accepted condition");
            assert!(
                error.to_string().contains("required review or waiver"),
                "{error}"
            );
            assert_eq!(
                services.event_store().durable_commit_cursor().unwrap(),
                cursor
            );
        }
        let selected = crate::agentic::review_evidence::required_review_conditions(&prepared.goal);
        assert!(selected.contains("additional-accepted-condition"));
        assert_eq!(selected.len(), 3);
        let intent = prepared.goal.user_intent_criterion_id.as_ref().unwrap();
        assert!(prepared
            .goal
            .obligations
            .iter()
            .any(|obligation| obligation.required && &obligation.obligation_id == intent));
        assert!(prepared
            .goal
            .obligations
            .iter()
            .any(|obligation| !obligation.required
                && obligation.obligation_id == "additional-accepted-condition"));
        assert!(!selected.contains("unrelated-optional"));
        assert!(!selected.contains("waived-additional"));
        assert!(prepared.gaps.is_empty());
        let after = services
            .agent_action_service()
            .commit_program_conclusion(program, prepared)
            .unwrap();
        let goal = services
            .goal_store()
            .get(&after.objective_verdict.as_ref().unwrap().goal_id)
            .unwrap()
            .unwrap();
        assert_native_sources_final_b(services, services.commit_service(), &goal, false);
    })
    .await;
}

#[tokio::test]
async fn r1_proof_exact_a_identity_survives_later_nonproof_events() {
    for writer in ["conclusion", "bound"] {
        with_native_goal_before_a(move |services, program, prepared, _| {
            let actions = services.agent_action_service();
            let after = if writer == "conclusion" {
                actions
                    .commit_program_conclusion(program, prepared)
                    .unwrap()
            } else {
                let goal = prepared.goal.clone();
                services
                    .event_store()
                    .append_batch_if_revision(
                        prepared.event.event.stream_id.clone(),
                        prepared.expected_stream_revision,
                        "goal-half-for-current-bound-writer",
                        vec![prepared.event],
                    )
                    .unwrap();
                actions
                    .bind_objective_verdict(&program.program_id, &goal)
                    .unwrap()
            };
            let verdict = after.objective_verdict.as_ref().unwrap();
            let goal = services
                .goal_store()
                .get(&verdict.goal_id)
                .unwrap()
                .unwrap();
            let own = format!("agentic-program:{}", program.program_id);
            let key = if writer == "conclusion" {
                format!("program-conclusion:{}", verdict.authority_revision)
            } else {
                format!("agentic-objective-verdict:{}", verdict.terminal_fence)
            };
            let original = services
                .event_store()
                .event_by_idempotency_key(&own, &key)
                .unwrap()
                .unwrap();
            for kind in ["agentic.task_lease_renewed", "test.unrelated_noise"] {
                services
                    .event_store()
                    .append(crate::RuntimeEventInput {
                        stream_id: own.clone(),
                        scope: crate::RuntimeEventScope::Program,
                        kind: kind.into(),
                        status: None,
                        actor: None,
                        refs: vec![],
                        payload: serde_json::json!({}),
                    })
                    .unwrap();
            }
            let previous = services
                .session_execution_policy("session-dispatch")
                .unwrap();
            let next = harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Cautious,
                previous.revision + 1,
                harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
            );
            services
                .session_execution_policy_control("session-dispatch")
                .unwrap()
                .replace(next)
                .unwrap();
            assert_eq!(
                services
                    .event_store()
                    .event_by_idempotency_key(&own, &key)
                    .unwrap()
                    .unwrap(),
                original
            );
            assert_native_sources_final_b(services, services.commit_service(), &goal, false);
            assert_eq!(services.goal_store().get(&goal.id).unwrap().unwrap(), goal);
        })
        .await;
    }
    for case in ["goal-revision", "goal-turn", "goal-program"] {
        with_native_goal_before_a(move |services, program, prepared, _| {
            let after = services
                .agent_action_service()
                .commit_program_conclusion(program, prepared)
                .unwrap();
            let goal = services
                .goal_store()
                .get(&after.objective_verdict.as_ref().unwrap().goal_id)
                .unwrap()
                .unwrap();
            let mut changed = goal.clone();
            if case == "goal-revision" {
                changed.revision += 1;
            } else if case == "goal-turn" {
                changed.execution_binding.as_mut().unwrap().turn_id = "foreign-turn".into();
            } else {
                changed
                    .execution_binding
                    .as_mut()
                    .unwrap()
                    .agentic_program_id = "foreign-program".into();
            }
            services
                .event_store()
                .append(crate::RuntimeEventInput {
                    stream_id: format!("goal:{}", goal.id),
                    scope: crate::RuntimeEventScope::Program,
                    kind: "goal.revised".into(),
                    status: None,
                    actor: Some("historical_binding_negative".into()),
                    refs: vec![],
                    payload: serde_json::json!({"goal":changed}),
                })
                .unwrap();
            assert_native_sources_b_rejected(
                services,
                &goal,
                "Goal terminal disagrees with Program verdict",
            );
        })
        .await;
    }
    for case in ["kind", "payload", "fence", "missing-key"] {
        with_native_goal_before_a(move |services, program, prepared, _| {
            let goal = append_historical_a_for_proof_negative(services, program, prepared, case);
            assert_native_sources_b_rejected(
                services,
                &goal,
                if case == "missing-key" {
                    "identity is missing"
                } else {
                    "exact Program event"
                },
            );
        })
        .await;
    }
    with_native_goal_before_a(|services, program, prepared, _| {
        let after = services
            .agent_action_service()
            .commit_program_conclusion(program, prepared)
            .unwrap();
        let verdict = after.objective_verdict.as_ref().unwrap();
        let goal = services
            .goal_store()
            .get(&verdict.goal_id)
            .unwrap()
            .unwrap();
        let stream = format!("agentic-program:{}", program.program_id);
        let original = services
            .event_store()
            .event_by_idempotency_key(
                &stream,
                &format!("program-conclusion:{}", verdict.authority_revision),
            )
            .unwrap()
            .unwrap();
        services
            .event_store()
            .append_batch_if_revision(
                &stream,
                after.revision,
                "distinct-second-current-a",
                vec![crate::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: stream.clone(),
                        scope: original.scope,
                        kind: original.kind,
                        status: original.status,
                        actor: original.actor,
                        refs: original.refs,
                        payload: original.payload,
                    },
                    idempotency_key: Some(format!(
                        "agentic-objective-verdict:{}",
                        verdict.terminal_fence
                    )),
                    schema_version: 1,
                }],
            )
            .unwrap();
        assert_native_sources_b_rejected(services, &goal, "conflicting committed A");
    })
    .await;
    for kind in [
        harness_contract::goal::ObjectiveTerminalKind::PartiallySatisfied,
        harness_contract::goal::ObjectiveTerminalKind::Blocked,
        harness_contract::goal::ObjectiveTerminalKind::Failed,
        harness_contract::goal::ObjectiveTerminalKind::Cancelled,
    ] {
        with_native_goal_before_a(move |services, program, mut prepared, _| {
            prepared.goal.completion = match kind {
                harness_contract::goal::ObjectiveTerminalKind::PartiallySatisfied => {
                    harness_contract::goal::GoalCompletion::Partial
                }
                harness_contract::goal::ObjectiveTerminalKind::Blocked => {
                    harness_contract::goal::GoalCompletion::Blocked
                }
                harness_contract::goal::ObjectiveTerminalKind::Failed => {
                    harness_contract::goal::GoalCompletion::Failed
                }
                _ => harness_contract::goal::GoalCompletion::Cancelled,
            };
            prepared.goal.terminal.as_mut().unwrap().kind = kind;
            let (event_kind, status) = match kind {
                harness_contract::goal::ObjectiveTerminalKind::PartiallySatisfied => {
                    ("goal.partial", "partial")
                }
                harness_contract::goal::ObjectiveTerminalKind::Blocked => {
                    ("goal.blocked", "blocked")
                }
                harness_contract::goal::ObjectiveTerminalKind::Failed => ("goal.failed", "failed"),
                _ => ("goal.cancelled", "cancelled"),
            };
            prepared.event.event.kind = event_kind.into();
            prepared.event.event.status = Some(status.into());
            prepared.goal.reviews.clear();
            prepared.policy_sources.clear();
            prepared.event.event.payload["goal"] = serde_json::to_value(&prepared.goal).unwrap();
            let after = services
                .agent_action_service()
                .commit_program_conclusion(program, prepared)
                .unwrap();
            let goal = services
                .goal_store()
                .get(&after.objective_verdict.as_ref().unwrap().goal_id)
                .unwrap()
                .unwrap();
            assert_native_sources_final_b(services, services.commit_service(), &goal, false);
        })
        .await;
    }

    with_native_goal_before_a(|services, program, prepared, _| {
        let after = services
            .agent_action_service()
            .commit_program_conclusion(program, prepared)
            .unwrap();
        let goal = services
            .goal_store()
            .get(&after.objective_verdict.as_ref().unwrap().goal_id)
            .unwrap()
            .unwrap();
        let (expected, release) = services
            .commit_service()
            .observe_terminal_append_for_test("turn-terminal:native-cold-b")
            .unwrap();
        let store = services.event_store().clone();
        let source = format!("session:{}", program.session_id);
        let writer = std::thread::spawn(move || {
            let cut = expected
                .blocking_recv()
                .expect("actual B pre-append observer must fire");
            let revision = cut
                .iter()
                .find(|cut| cut.stream_id == source)
                .unwrap()
                .expected_revision;
            assert_eq!(store.stream_revision(&source).unwrap(), revision);
            store
                .append(crate::RuntimeEventInput {
                    stream_id: source.clone(),
                    scope: crate::RuntimeEventScope::Session,
                    kind: "test.after_b_material_cut".into(),
                    status: None,
                    actor: None,
                    refs: vec![],
                    payload: serde_json::json!({}),
                })
                .unwrap();
            assert_eq!(store.stream_revision(&source).unwrap(), revision + 1);
            release.send(()).unwrap();
            (source, revision)
        });
        assert_native_sources_b_rejected(services, &goal, "revision");
        let (source, expected_revision) = writer.join().unwrap();
        assert_eq!(
            services.event_store().stream_revision(&source).unwrap(),
            expected_revision + 1
        );
        assert_eq!(services.goal_store().get(&goal.id).unwrap().unwrap(), goal);
    })
    .await;
    exercise_native_sources_with_changed_anchor_source().await;
}

#[tokio::test]
async fn r1_process_backend_cancel_retains_physical_scope_and_rejects_old_owner() {
    exercise_r1_backend_resource_lifetime_for_test(R1ResourceBackend::Process).await;
}

#[tokio::test]
async fn r1_backend_effect_receipt_cannot_confirm_unknown_process_exit() {
    exercise_r1_backend_unknown_exit_for_test().await;
}

#[tokio::test]
async fn r1_process_envelope_rw_max_is_clamped_to_readonly_off() {
    let fixture = PhysicalProcessFixture::new(
        crate::config::AgentExecutorSandboxProfile::WorkspaceReadWrite,
        "physical-max",
    )
    .await;
    let worker = fixture
        .dispatch("physical-max", vec!["read:.".into()], false, false, true)
        .await;
    let fact = worker.lifecycle().semantic_fact();
    assert!(worker.lifecycle().no_direct_effects());
    assert_eq!(
        fact["envelope"]["command_digest"],
        fixture.spec.command_digest
    );
    assert_eq!(fact["envelope"]["workspace_writable"], false);
    assert_eq!(fact["envelope"]["network_enabled"], false);
    let sentinel = fixture
        .services
        .workspace_root()
        .join("out/physical-sentinel.txt");
    assert_eq!(
        worker
            .probe(serde_json::json!({"kind":"read","path":"out/physical-sentinel.txt"}))
            .await["value"],
        "initial sentinel"
    );
    assert_eq!(worker.probe(serde_json::json!({"kind":"write","path":"out/physical-sentinel.txt","value":"forbidden direct"})).await["ok"], false);
    assert_eq!(
        std::fs::read_to_string(sentinel).unwrap(),
        "initial sentinel"
    );
    let script = fixture
        .services
        .workspace_root()
        .join(".cowd/fixtures/r1-worker.py");
    let approved_script = std::fs::read(&script).unwrap();
    assert_eq!(worker.probe(serde_json::json!({"kind":"write","path":".cowd/fixtures/r1-worker.py","value":"forbidden replacement"})).await["ok"], false);
    assert_eq!(std::fs::read(&script).unwrap(), approved_script);
    let edits = fixture.edit_calls();
    assert!(worker
        .tool(
            "edit",
            serde_json::json!({"path":"out/a","content":"forbidden Host write"})
        )
        .await
        .is_err());
    assert_eq!(
        fixture.edit_calls(),
        edits,
        "Host authorization rejection is separate from OS denial"
    );
    let scratch = worker.directory().join("private-control.txt");
    assert_eq!(
        worker
            .probe(serde_json::json!({"kind":"write","path":scratch,"value":"private control"}))
            .await["ok"],
        true
    );
    assert_eq!(
        std::fs::read_to_string(&scratch).unwrap(),
        "private control"
    );
    worker.finish_readonly_control().await;
    assert!(!worker.directory().exists());
}

#[tokio::test]
async fn r1_process_envelope_host_write_survives_physical_readonly() {
    if std::env::var("COWD_AUTO_CHECKPOINT").ok().as_deref() != Some("1") {
        let output = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "agentic::execution::tests::r1_process_envelope_host_write_survives_physical_readonly", "--nocapture"])
            .env("COWD_AUTO_CHECKPOINT", "1").output().await.unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("R1_REAL_CHECKPOINT_P2"));
        return;
    }
    fn checkpoint_files(
        root: &std::path::Path,
    ) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
        fn collect(
            root: &std::path::Path,
            path: &std::path::Path,
            files: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
        ) {
            if !path.exists() {
                return;
            }
            for entry in std::fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    collect(root, &path, files);
                } else {
                    files.insert(
                        path.strip_prefix(root).unwrap().to_path_buf(),
                        std::fs::read(path).unwrap(),
                    );
                }
            }
        }
        let mut files = std::collections::BTreeMap::new();
        collect(root, &root.join(".cowd/checkpoints"), &mut files);
        files
    }
    for (name, scopes) in [
        (
            "physical-original-dual-scope",
            vec!["read:.", "write:out/a"],
        ),
        (
            "physical-missing-target",
            vec!["read:.", "write:.cowd/checkpoints"],
        ),
    ] {
        let denied = PhysicalProcessFixture::new(
            crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
            name,
        )
        .await;
        let worker = denied
            .dispatch(
                name,
                scopes.into_iter().map(str::to_string).collect(),
                true,
                false,
                true,
            )
            .await;
        let before = checkpoint_files(denied.services.workspace_root());
        let calls = denied.edit_calls();
        let error = worker
            .tool(
                "edit",
                serde_json::json!({"action":"write","path":"out/a","content":"unauthorized"}),
            )
            .await
            .unwrap_err();
        assert!(error.contains("resource lease"), "{name}: {error}");
        assert_eq!(denied.edit_calls(), calls);
        assert_eq!(checkpoint_files(denied.services.workspace_root()), before);
        assert_eq!(
            std::fs::read_to_string(denied.services.workspace_root().join("out/a")).unwrap(),
            "initial narrow target"
        );
        worker.finish().await;
    }
    let fixture = PhysicalProcessFixture::new(
        crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
        "physical-narrow-write",
    )
    .await;
    let worker = fixture
        .dispatch(
            "physical-narrow-write",
            vec![
                "read:.".into(),
                "write:out/a".into(),
                "write:.cowd/checkpoints".into(),
            ],
            true,
            false,
            true,
        )
        .await;
    assert_eq!(
        worker
            .packet
            .resource_scopes
            .iter()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([
            "program:program-dispatch",
            "read:.",
            "session:session-dispatch",
            "write:out/a",
            "write:.cowd/checkpoints"
        ])
    );
    assert!(worker
        .packet
        .binding
        .as_ref()
        .unwrap()
        .effective_capabilities
        .contains(&harness_contract::agent::AgentCapability::Write));
    assert!(worker
        .packet
        .allowed_tools
        .iter()
        .any(|tool| tool == "edit"));
    assert!(worker.lifecycle().no_direct_effects());
    assert_eq!(
        worker
            .probe(serde_json::json!({"kind":"write","path":"out/a","value":"direct bypass"}))
            .await["ok"],
        false
    );
    assert_eq!(
        std::fs::read_to_string(fixture.services.workspace_root().join("out/a")).unwrap(),
        "initial narrow target"
    );
    std::fs::create_dir_all(fixture.services.workspace_root().join(".cowd/checkpoints")).unwrap();
    let checkpoints_before = checkpoint_files(fixture.services.workspace_root());
    std::fs::write(
        fixture
            .services
            .workspace_root()
            .join("out/snapshot-sibling"),
        "must not be copied",
    )
    .unwrap();
    std::fs::write(
        fixture.services.workspace_root().join(".env"),
        "sensitive sentinel must not be copied",
    )
    .unwrap();
    assert_eq!(worker.probe(serde_json::json!({"kind":"write","path":".cowd/checkpoints/direct-probe.txt","value":"forbidden child checkpoint"})).await["ok"], false);
    assert_eq!(
        checkpoint_files(fixture.services.workspace_root()),
        checkpoints_before
    );
    worker
        .tool(
            "edit",
            serde_json::json!({"path":"out/a","content":"actual narrow Host write"}),
        )
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(fixture.services.workspace_root().join("out/a")).unwrap(),
        "actual narrow Host write"
    );
    let checkpoints_after = checkpoint_files(fixture.services.workspace_root());
    assert_ne!(
        checkpoints_after, checkpoints_before,
        "real Tools auto-checkpoint must have occurred"
    );
    assert!(checkpoints_after
        .iter()
        .any(|(path, bytes)| path.ends_with("out/a") && bytes == b"initial narrow target"));
    assert!(checkpoints_after.iter().any(|(path, bytes)| {
        if path.ends_with(".manifest.json") && !checkpoints_before.contains_key(path) {
            let manifest: serde_json::Value = serde_json::from_slice(bytes).unwrap();
            assert_eq!(manifest["scopes"], serde_json::json!(["out/a"]));
            assert_eq!(manifest["files"], serde_json::json!(["out/a"]));
            true
        } else {
            false
        }
    }));
    for (path, bytes) in &checkpoints_after {
        if !checkpoints_before.contains_key(path) {
            assert!(
                !path.ends_with("snapshot-sibling") && !path.ends_with(".env"),
                "{path:?}"
            );
            assert_ne!(bytes.as_slice(), b"must not be copied");
            assert_ne!(bytes.as_slice(), b"sensitive sentinel must not be copied");
        }
    }
    for target in [
        "out/b",
        ".",
        "../escape",
        "/tmp/r1-unauthorized",
        ".cowd/outside",
    ] {
        let calls = fixture.edit_calls();
        assert!(worker.tool("edit", serde_json::json!({"action":"write","path":target,"content":"forbidden sibling"})).await.is_err(), "{target}");
        assert_eq!(fixture.edit_calls(), calls);
        assert_eq!(
            checkpoint_files(fixture.services.workspace_root()),
            checkpoints_after
        );
        assert_eq!(
            std::fs::read_to_string(fixture.services.workspace_root().join("out/a")).unwrap(),
            "actual narrow Host write"
        );
    }
    let receipts = fixture
        .services
        .commit_service()
        .load_delegated_agent_tool_receipts(
            worker.packet.graph_id(),
            worker.packet.node_id(),
            worker.packet.attempt,
        )
        .unwrap();
    assert!(receipts
        .iter()
        .any(|receipt| receipt.outcome.tool_name == "edit"
            && receipt.outcome.status == crate::RuntimeToolExecutionStatus::Executed));
    let reader = fixture
        .dispatch(
            "physical-narrow-independent",
            vec!["read:.".into()],
            false,
            false,
            true,
        )
        .await;
    assert_ne!(reader.packet.agent_id(), worker.packet.agent_id());
    assert!(reader
        .read("physical-independent-read", "out/a")
        .await
        .contains("actual narrow Host write"));
    reader.assert_host_read("physical-independent-read");
    reader.finish().await;
    #[cfg(unix)]
    {
        let outside = tempfile::tempdir().unwrap();
        let outside_target = outside.path().join("outside.txt");
        std::fs::write(&outside_target, "outside unchanged").unwrap();
        let target = fixture.services.workspace_root().join("out/a");
        std::fs::remove_file(&target).unwrap();
        std::os::unix::fs::symlink(&outside_target, &target).unwrap();
        assert!(worker
            .tool(
                "edit",
                serde_json::json!({"action":"write","path":"out/a","content":"symlink escape"})
            )
            .await
            .is_err());
        assert_eq!(
            std::fs::read_to_string(&outside_target).unwrap(),
            "outside unchanged"
        );
        assert_eq!(
            checkpoint_files(fixture.services.workspace_root()),
            checkpoints_after
        );
        std::fs::remove_file(&target).unwrap();
        std::fs::write(&target, "actual narrow Host write").unwrap();
    }
    worker.finish().await;
    println!("R1_REAL_CHECKPOINT_P2: initial three scopes, concrete dual-scope denial, actual checkpoint/target/receipt and independent Process read");
}

#[tokio::test]
async fn r1_process_envelope_opaque_write_blocks_goal_after_reap() {
    exercise_r1_proof_scenario(R1ProofScenario::PhysicalOpaque, DelegatedEffectCase::Review).await;
}

#[tokio::test]
async fn r1_process_envelope_network_policy_intersection_controls_direct_nonce() {
    const CASE: &str = "COWD_PHYSICAL_NETWORK_CASE";
    if let Ok(case) = std::env::var(CASE) {
        let profile = if case == "host-only-profile" {
            crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly
        } else {
            crate::config::AgentExecutorSandboxProfile::WorkspaceReadOnly
        };
        let fixture = PhysicalProcessFixture::new(profile, &format!("physical-net-{case}")).await;
        let network = case != "missing-capability";
        let scopes = if case == "missing-scope" {
            vec!["read:.".into()]
        } else if case == "domain-scope" {
            vec!["read:.".into(), "network:example.invalid".into()]
        } else {
            vec!["read:.".into(), "network:*".into()]
        };
        let worker = fixture.dispatch(&case, scopes, false, network, true).await;
        let expected = matches!(case.as_str(), "allow" | "presence-false");
        assert_eq!(
            worker.lifecycle().semantic_fact()["envelope"]["network_enabled"],
            expected
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let nonce = uuid::Uuid::new_v4().to_string();
        let response = worker.probe(serde_json::json!({"kind":"nonce","port":listener.local_addr().unwrap().port(),"nonce":nonce})).await;
        let received = tokio::time::timeout(std::time::Duration::from_millis(700), async {
            use tokio::io::AsyncReadExt;
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            socket.read_to_end(&mut bytes).await.unwrap();
            bytes
        })
        .await;
        if expected {
            assert_eq!(received.unwrap(), nonce.as_bytes());
            assert_eq!(response["ok"], true);
        } else {
            assert!(
                received.is_err(),
                "off envelope must deny actual child TCP, not merely Host tool calls: {case}"
            );
            assert_eq!(response["ok"], false);
        }
        if case == "host-only-profile" {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let host_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!(
                "http://{}/physical-host-network",
                host_listener.local_addr().unwrap()
            );
            let body = format!("actual Host-only network nonce {nonce}");
            let expected_body = body.clone();
            let server = tokio::spawn(async move {
                let (mut socket, _) = host_listener.accept().await.unwrap();
                let mut request = [0u8; 4096];
                let bytes = socket.read(&mut request).await.unwrap();
                assert!(
                    String::from_utf8_lossy(&request[..bytes]).contains("/physical-host-network")
                );
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).as_bytes()).await.unwrap();
            });
            let output = worker
                .tool(
                    "web_fetch",
                    serde_json::json!({"url":url,"prompt":"Return the complete local nonce text"}),
                )
                .await
                .unwrap();
            assert!(output.contains(&expected_body), "physically-off worker must retain its actually authorized Host network capability: {output}");
            server.await.unwrap();
            let receipts = fixture
                .services
                .commit_service()
                .load_delegated_agent_tool_receipts(
                    worker.packet.graph_id(),
                    worker.packet.node_id(),
                    worker.packet.attempt,
                )
                .unwrap();
            assert!(receipts
                .iter()
                .any(|receipt| receipt.outcome.tool_name == "web_fetch"
                    && receipt.outcome.status == crate::RuntimeToolExecutionStatus::Executed));
        }
        println!("PHYSICAL_NETWORK_CASE {case}: actual_child_nonce={expected}");
        worker.finish().await;
        return;
    }
    for case in [
        "allow",
        "presence-false",
        "missing-private",
        "allow-list",
        "block-list",
        "ask",
        "deny",
        "invalid-mode",
        "missing-capability",
        "missing-scope",
        "domain-scope",
        "host-only-profile",
    ] {
        let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
        command.args(["--exact", "agentic::execution::tests::r1_process_envelope_network_policy_intersection_controls_direct_nonce", "--nocapture"]);
        command
            .env(CASE, case)
            .env("COWD_NETWORK_DOMAIN_MODE", "allow")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost")
            .env_remove("COWD_NETWORK_DOMAIN_ALLOW")
            .env_remove("COWD_NETWORK_DOMAIN_BLOCK")
            .env(
                "COWD_ALLOW_PRIVATE_NETWORK",
                if case == "presence-false" {
                    "false"
                } else {
                    "1"
                },
            );
        match case {
            "missing-private" => {
                command.env_remove("COWD_ALLOW_PRIVATE_NETWORK");
            }
            "allow-list" => {
                command.env("COWD_NETWORK_DOMAIN_ALLOW", "example.invalid");
            }
            "block-list" => {
                command.env("COWD_NETWORK_DOMAIN_BLOCK", "example.invalid");
            }
            "ask" | "deny" | "invalid-mode" => {
                command.env("COWD_NETWORK_DOMAIN_MODE", case);
            }
            _ => {}
        }
        let output = command.output().await.unwrap();
        assert!(
            output.status.success(),
            "{case}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .contains(&format!("PHYSICAL_NETWORK_CASE {case}:")),
            "exact subprocess selector must run the actual case, not select zero tests"
        );
    }
}

#[tokio::test]
async fn r1_process_envelope_narrow_read_rejected_before_spawn() {
    for scopes in [vec!["read:out/a".into()], vec!["read:out".into()]] {
        let fixture = PhysicalProcessFixture::new(
            crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
            "physical-narrow-read",
        )
        .await;
        let worker = fixture
            .dispatch("physical-narrow-read", scopes.clone(), false, false, false)
            .await;
        worker.wait_terminal().await;
        let exit = crate::agent_runtime::confirmed_run_exit(
            fixture.services.event_store(),
            &worker.packet,
        )
        .unwrap()
        .expect("real adapter must record NotStarted");
        assert!(worker.lifecycle().no_direct_effects());
        assert_eq!(worker.lifecycle().semantic_fact()["not_started"], true);
        let reason = worker.terminal_failure();
        assert!(reason.contains("physical_grant"), "{reason}");
        assert!(!worker.directory().join("started.json").exists());
        assert_eq!(
            std::fs::read_to_string(fixture.services.workspace_root().join("out/a")).unwrap(),
            "initial narrow target"
        );
        assert!(!exit.1.is_empty());
        worker.withdraw().await;
        println!("PHYSICAL_NARROW_READ adapter NotStarted {scopes:?}");
    }
    let ambiguous = PhysicalProcessFixture::new(
        crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
        "physical-root-ambiguity",
    )
    .await;
    for repository in ["repository-a", "repository-b"] {
        std::fs::create_dir_all(
            ambiguous
                .services
                .workspace_root()
                .join(repository)
                .join(".git"),
        )
        .unwrap();
    }
    let worker = ambiguous
        .dispatch(
            "physical-root-ambiguity",
            vec!["read:.".into()],
            false,
            false,
            false,
        )
        .await;
    worker.wait_terminal().await;
    assert!(worker.terminal_failure().contains("physical_grant"));
    assert!(crate::agent_runtime::confirmed_run_exit(
        ambiguous.services.event_store(),
        &worker.packet
    )
    .unwrap()
    .is_some());
    assert!(!worker.directory().join("started.json").exists());
    worker.withdraw().await;
    for scopes in [
        Vec::<String>::new(),
        vec!["session:session-dispatch".into()],
        vec!["read:../outside".into()],
        vec!["read:/foreign-workspace".into()],
    ] {
        let fixture = PhysicalProcessFixture::new(
            crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
            "physical-early-layer",
        )
        .await;
        match fixture.try_dispatch("physical-early-layer", scopes.clone(), false, false, false).await {
            Err(error) => println!("PHYSICAL_NARROW_READ earlier production admission, not OS evidence: {scopes:?}: {error}"),
            Ok(worker) => {
                worker.wait_terminal().await;
                let reason = worker.terminal_failure();
                assert!(!reason.is_empty());
                assert!(!worker.directory().join("started.json").exists());
                println!("PHYSICAL_NARROW_READ actual immutable packet terminal rejection: {scopes:?}: {reason}");
                worker.withdraw().await;
            }
        }
    }
    let fixture = PhysicalProcessFixture::new(
        crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
        "physical-pure-boundary",
    )
    .await;
    let worker = fixture
        .dispatch(
            "physical-pure-boundary",
            vec!["read:.".into()],
            false,
            false,
            true,
        )
        .await;
    for scopes in [
        Vec::<String>::new(),
        vec!["session:session-dispatch".into()],
        vec!["read:../outside".into()],
        vec!["read:/foreign-workspace".into()],
    ] {
        let mut pure_negative = worker.packet.clone();
        pure_negative.resource_scopes = scopes.clone();
        let error = fixture
            .services
            .agent_runtime()
            .validate_process_fixture_boundary(&pure_negative)
            .unwrap_err();
        assert!(error.contains("physical_grant"), "{error}");
        println!("PHYSICAL_NARROW_READ pure unpersisted boundary negative, not a production Task/OS claim: {scopes:?}");
    }
    worker.finish().await;
    exercise_physical_native_narrow_read().await;
}

#[tokio::test]
async fn r1_process_envelope_all_lineage_attempts_remain_in_proof_cut() {
    exercise_r1_proof_scenario(
        R1ProofScenario::PhysicalLineage,
        DelegatedEffectCase::Review,
    )
    .await;
}

#[tokio::test]
async fn r1_process_envelope_host_only_early_goal_and_actual_cold_b() {
    exercise_physical_cold_optional_goal(R1ProofScenario::OptionalSharedCheckpoint).await;
    exercise_physical_cold_optional_goal(R1ProofScenario::PhysicalAfterA).await;
}

#[tokio::test]
async fn r1_process_envelope_scratch_is_private_until_confirmed_exit() {
    use std::sync::atomic::Ordering;
    let fixture = PhysicalProcessFixture::new(
        crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
        "physical-private-scratch",
    )
    .await;
    let first = fixture
        .dispatch("private-first", vec!["read:.".into()], false, false, true)
        .await;
    let second = fixture
        .dispatch("private-second", vec!["read:.".into()], false, false, true)
        .await;
    assert_ne!(first.directory(), second.directory());
    for (reader, other) in [(&first, &second), (&second, &first)] {
        assert_eq!(
            reader
                .probe(
                    serde_json::json!({"kind":"read","path":other.directory().join("started.json")})
                )
                .await["ok"],
            false
        );
        assert_eq!(reader.probe(serde_json::json!({"kind":"write","path":other.directory().join("cross-run.txt"),"value":"forbidden"})).await["ok"], false);
        assert!(!other.directory().join("cross-run.txt").exists());
    }
    let gate = fixture
        .services
        .agent_runtime()
        .process_fixture_exit_gate(false);
    first.finish_only().await;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !gate.entered.load(Ordering::Acquire) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        first.directory().exists(),
        "result and kill request do not authorize scratch cleanup"
    );
    assert!(crate::agent_runtime::confirmed_run_exit(
        fixture.services.event_store(),
        &first.packet
    )
    .unwrap()
    .is_none());
    gate.release();
    first.wait_exit().await;
    assert!(!first.directory().exists());
    assert!(second.directory().exists());
    second.finish().await;
    assert!(!second.directory().exists());
    let unknown_gate = fixture
        .services
        .agent_runtime()
        .process_fixture_exit_gate(true);
    let unknown = fixture
        .dispatch("private-unknown", vec!["read:.".into()], false, false, true)
        .await;
    unknown.finish_only().await;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !unknown_gate.entered.load(Ordering::Acquire) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    unknown_gate.release();
    unknown.wait_terminal().await;
    assert!(
        unknown.directory().exists(),
        "driver join failure preserves quarantined scratch"
    );
    assert!(crate::agent_runtime::confirmed_run_exit(
        fixture.services.event_store(),
        &unknown.packet
    )
    .unwrap()
    .is_none());
}

#[tokio::test]
async fn r1_process_cleanup_failure_preserves_reaped_and_reports_residue() {
    use std::sync::atomic::Ordering;
    let fixture = PhysicalProcessFixture::new(
        crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
        "cleanup-remove-failure",
    )
    .await;
    let failed = fixture
        .dispatch("cleanup-failed", vec!["read:.".into()], false, false, true)
        .await;
    let other = fixture
        .dispatch("cleanup-other", vec!["read:.".into()], false, false, true)
        .await;
    let driver_error = observe_process_cleanup_driver(&failed.packet);
    let exact = failed.directory();
    let isolated = exact.with_extension("quarantine");
    let gate = fixture
        .services
        .agent_runtime()
        .process_fixture_exit_gate(false);
    failed.finish_only().await;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !gate.entered.load(Ordering::Acquire) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(crate::agent_runtime::confirmed_run_exit(
        fixture.services.event_store(),
        &failed.packet
    )
    .unwrap()
    .is_none());
    std::fs::rename(&exact, &isolated).unwrap();
    std::fs::write(&exact, b"exact run sentinel").unwrap();
    gate.release();
    failed.wait_exit().await;
    failed.wait_terminal().await;
    let witness =
        crate::agent_runtime::confirmed_run_exit(fixture.services.event_store(), &failed.packet)
            .unwrap()
            .expect("actual wait/join remains exact Reaped despite cleanup failure");
    assert!(format!("{witness:?}").contains("ProcessReaped"));
    let failure = driver_error
        .recv_timeout(std::time::Duration::from_secs(10))
        .unwrap();
    assert!(failure.contains("scratch cleanup failed"), "{failure}");
    assert!(
        failure.contains("NotADirectory") || failure.to_lowercase().contains("not a directory"),
        "{failure}"
    );
    assert!(failure.contains(&exact.display().to_string()), "{failure}");
    assert!(failure.contains(failed.packet.run_id()), "{failure}");
    assert_eq!(std::fs::read(&exact).unwrap(), b"exact run sentinel");
    assert!(isolated.is_dir());
    assert!(other.directory().is_dir());
    std::fs::remove_file(&exact).unwrap();
    std::fs::rename(&isolated, &exact).unwrap();
    retry_process_owned_scratch(&failed.packet, &exact).unwrap();
    assert!(!exact.exists());
    assert!(!isolated.exists());
    assert!(other.directory().is_dir());
    other.finish().await;
    assert!(!other.directory().exists());
}

#[test]
fn claim_heartbeat_follows_only_nonterminal_physical_graphs() {
    let mut graph = ExecutionGraph::new("heartbeat activity");
    graph
        .node_statuses
        .insert("agent".to_string(), ExecutionNodeStatus::Running);
    assert!(!agentic_graph_is_terminal(&graph));
    graph
        .node_statuses
        .insert("agent".to_string(), ExecutionNodeStatus::Completed);
    assert!(agentic_graph_is_terminal(&graph));
}

#[tokio::test]
async fn heartbeat_waits_for_the_agents_real_claim_and_stops_at_graph_terminal() {
    let services = Arc::new(RuntimeServices::in_memory().expect("runtime"));
    let actions = services.agent_action_service();
    let team = actions
        .apply(&root(
            "heartbeat-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Heartbeat Team".to_string(),
                mission: "own a real claim".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let member = actions
        .apply(&root(
            "heartbeat-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Worker".to_string(),
                mission: "claim autonomously".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("agent")
        .changed_refs[0]
        .clone();
    let task = actions
        .apply(&root(
            "heartbeat-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Real claim".to_string(),
                objective: "prove ownership".to_string(),
                acceptance: "claim is fenced to the Agent execution".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("task")
        .changed_refs[0]
        .clone();
    let execution_id = "heartbeat-agent-execution";
    let mut graph = ExecutionGraph::new("heartbeat Agent graph");
    graph.id = execution_id.to_string();
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::AgentTask,
        AgentTaskExecutor::KIND,
        "test-packet",
    );
    node.id = "heartbeat-agent-node".to_string();
    node.idempotency_key = "heartbeat-agent-node:1".to_string();
    graph
        .node_statuses
        .insert(node.id.clone(), ExecutionNodeStatus::Planned);
    graph.nodes.push(node);
    services
        .commit_service()
        .register_graph(graph)
        .expect("register graph");
    let projection = actions.project("program-dispatch").expect("projection");
    let actor = agentic_claim_actor(
        &projection,
        &projection.agents[&member],
        &task,
        execution_id,
    );
    assert_eq!(
        agentic_claim_heartbeat_state(
            Arc::downgrade(services.as_ref()),
            &task,
            execution_id,
            &actor,
        )
        .await,
        AgenticClaimDriverState::AwaitingAgentClaim
    );

    actions
        .apply(&agent(
            "agent-owned-heartbeat-claim",
            &team,
            &member,
            execution_id,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task.clone(),
                reason: Some("I accept this work".to_string()),
            }),
        ))
        .expect("real Agent claim");
    assert!(matches!(
        agentic_claim_heartbeat_state(
            Arc::downgrade(services.as_ref()),
            &task,
            execution_id,
            &actor,
        )
        .await,
        AgenticClaimDriverState::Owned { .. }
    ));

    let graph = services
        .graph_state_store()
        .load(execution_id)
        .expect("graph");
    services
        .commit_service()
        .apply_command(
            &graph,
            &ExecutionGraphCommand::Cancel {
                expected_revision: graph.revision,
                reason: "worker ended".to_string(),
            },
        )
        .expect("terminalize graph");
    assert_eq!(
        agentic_claim_heartbeat_state(
            Arc::downgrade(services.as_ref()),
            &task,
            execution_id,
            &actor,
        )
        .await,
        AgenticClaimDriverState::Stop
    );
}

#[tokio::test]
async fn heartbeat_guard_aborts_its_task_when_the_worker_scope_ends() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct DropSignal(Arc<AtomicBool>);
    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let task_dropped = Arc::clone(&dropped);
    let task = tokio::spawn(async move {
        let _signal = DropSignal(task_dropped);
        std::future::pending::<()>().await;
    });
    tokio::task::yield_now().await;
    drop(AgenticClaimHeartbeatGuard { task });
    for _ in 0..16 {
        if dropped.load(Ordering::SeqCst) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        dropped.load(Ordering::SeqCst),
        "ending the physical worker scope must cancel its heartbeat future"
    );
}

#[tokio::test]
async fn root_actor_inherits_the_immutable_graph_delegation_scope() {
    let services = RuntimeServices::in_memory().expect("runtime");
    let session_id = "session-root-scope";
    let turn_id = "turn-root-scope";
    let graph_id = "root-scope-execution";
    let objective = format!(
        "{}完整目标必须保留末尾的独立评审与只读约束",
        "明确要求 ".repeat(60)
    );
    let mut graph = ExecutionGraph::new(&objective);
    graph.id = graph_id.to_string();
    graph.lineage = Some(ExecutionGraphLineage {
        session_id: session_id.to_string(),
        turn_id: turn_id.to_string(),
        root_task_id: "task-root-scope".to_string(),
        task_id: "task-root-scope".to_string(),
        generation: 1,
    });
    let mut model = ExecutionNodeSpec::new(
        ExecutionNodeKind::InlineModel,
        "inline_model",
        "payload:root-scope",
    );
    model.id = format!("{graph_id}:model");
    model.idempotency_key = format!("{graph_id}:model:1");
    let model_id = model.id.clone();
    let mut guard = ExecutionNodeSpec::new(
        ExecutionNodeKind::Verify,
        "compile_target_guard",
        "payload:scope-guard",
    );
    guard.id = format!("{graph_id}:resource-constraint");
    guard.idempotency_key = format!("{graph_id}:resource-constraint:1");
    guard.resource_scopes = vec!["workspace:.".to_string(), "network:*".to_string()];
    graph.nodes = vec![guard, model];
    services
        .commit_service()
        .register_graph(graph)
        .expect("register root graph");

    let objective_id = harness_contract::agent_action::root_objective_id(session_id, turn_id);
    let trusted = AgentActorBinding {
        objective_id: objective_id.clone(),
        program_id: harness_contract::agent_action::program_id_for_objective(&objective_id),
        session_id: session_id.to_string(),
        turn_id: turn_id.to_string(),
        root_execution_id: Some(graph_id.to_string()),
        required_team_count: 0,
        exact_team_count: None,
        objective_summary: "autonomous scoped work".to_string(),
        model_lease: "model:test".to_string(),
        permission_ceiling: Some(PermissionMode::DangerFullAccess),
        resource_scopes: Vec::new(),
        actor_id: format!("root:{session_id}"),
        kind: AgentActorKind::Root,
        execution_id: None,
        team_id: None,
        agent_id: None,
    };
    let resolved = services
        .resolve_agent_action_actor(
            &ExecutionParentBinding {
                execution_id: graph_id.to_string(),
                node_id: model_id,
            },
            Some(trusted),
        )
        .await
        .expect("resolve root actor");

    assert_eq!(resolved.resource_scopes, ["network:*", "workspace:."]);
    assert_eq!(resolved.objective_summary, objective);
}

#[tokio::test]
async fn task_publish_admits_real_agent_graph_with_human_display_identity() {
    exercise_reviewed_goal_delivery(false, false, DelegatedEffectCase::None).await;
}

#[tokio::test]
async fn issue_disposition_gates_independent_reviewed_delivery_through_the_same_goal_verifier() {
    exercise_reviewed_goal_delivery(true, false, DelegatedEffectCase::None).await;
}

#[tokio::test]
async fn independent_agent_reviews_actual_root_effect_only_after_reading_affected_file() {
    exercise_reviewed_goal_delivery(false, true, DelegatedEffectCase::None).await;
}

#[tokio::test]
async fn delegated_writer_artifact_requires_target_read_and_rejects_late_writes() {
    exercise_reviewed_goal_delivery(false, false, DelegatedEffectCase::Review).await;
}

#[tokio::test]
async fn delegated_writer_withdrawal_fences_new_effects_before_process_cancellation() {
    exercise_reviewed_goal_delivery(false, false, DelegatedEffectCase::Withdraw).await;
}

async fn exercise_reviewed_goal_delivery(
    with_disclosure: bool,
    with_effect: bool,
    delegated_case: DelegatedEffectCase,
) {
    exercise_reviewed_goal_delivery_kind(with_disclosure, with_effect, delegated_case, false).await;
}

#[tokio::test]
async fn one_shot_text_delivery_uses_the_same_independent_review_and_goal_verifier() {
    exercise_reviewed_goal_delivery_kind(false, false, DelegatedEffectCase::None, true).await;
}

#[tokio::test]
async fn proven_text_goal_is_satisfied_while_original_workers_physically_drain() {
    exercise_proven_goal_while_workers_drain(DelegatedEffectCase::None, true).await;
}

#[tokio::test]
async fn r1_proof_unrelated_note_preserves_current_verdict() {
    exercise_r1_proof_scenario(R1ProofScenario::UnrelatedNote, DelegatedEffectCase::None).await;
}

#[tokio::test]
async fn r1_proof_execution_epoch_does_not_retroactively_revoke_acceptance() {
    exercise_r1_proof_scenario(R1ProofScenario::ExecutionEpoch, DelegatedEffectCase::Review).await;
}

#[tokio::test]
async fn r1_proof_related_result_and_policy_changes_require_new_review() {
    exercise_reviewed_goal_delivery_kind(false, true, DelegatedEffectCase::None, false).await;
    exercise_r1_proof_scenario(R1ProofScenario::AcceptancePolicy, DelegatedEffectCase::None).await;
}

#[tokio::test]
async fn r1_proof_material_enumeration_race_requires_full_retry() {
    exercise_r1_proof_scenario(
        R1ProofScenario::MaterialEnumerationRace,
        DelegatedEffectCase::Review,
    )
    .await;
}

#[tokio::test]
async fn r1_proof_partial_dependency_and_producer_only_reads_do_not_refresh_reviewer() {
    exercise_r1_proof_scenario(
        R1ProofScenario::PartialDependency,
        DelegatedEffectCase::Review,
    )
    .await;
}

#[tokio::test]
async fn r1_optional_unknown_bound_tool_children_require_exact_parent_and_attempt() {
    for scenario in [
        R1ProofScenario::OptionalForeignChild,
        R1ProofScenario::OptionalWrongChildAttempt,
        R1ProofScenario::OptionalMixedChild,
    ] {
        exercise_r1_proof_scenario(scenario, DelegatedEffectCase::Review).await;
    }
}

#[tokio::test]
async fn r1_proof_primary_influence_from_reviewer_material_is_rejected() {
    exercise_r1_proof_scenario(
        R1ProofScenario::ReviewerInfluence,
        DelegatedEffectCase::None,
    )
    .await;
}

#[tokio::test]
async fn r1_proof_transitive_materials_close_producer_and_approval_sources() {
    exercise_r1_proof_scenario(
        R1ProofScenario::TransitiveDependency,
        DelegatedEffectCase::None,
    )
    .await;
}

#[tokio::test]
async fn r1_optional_unknown_disjoint_scope_remains_visible() {
    for scenario in [
        R1ProofScenario::OptionalForgedB,
        R1ProofScenario::OptionalForeignB,
    ] {
        exercise_r1_proof_scenario(scenario, DelegatedEffectCase::Review).await;
    }
    exercise_r1_proof_scenario(
        R1ProofScenario::OptionalSharedCheckpoint,
        DelegatedEffectCase::Review,
    )
    .await;
    exercise_r1_proof_scenario(
        R1ProofScenario::OptionalSourceRace,
        DelegatedEffectCase::Review,
    )
    .await;
}

#[tokio::test]
async fn r1_proof_legacy_replay_does_not_recompute_policy() {
    for unknown_version in [false, true] {
        let host = Arc::new(ReviewReadHost::default());
        let services = RuntimeServices::in_memory_with_tool_host(host.clone()).unwrap();
        Box::pin(exercise_reviewed_goal_proof_scenario(
            services,
            host,
            false,
            false,
            DelegatedEffectCase::None,
            true,
            PhysicalDrainTiming::BeforeGoal,
            R1ProofScenario::LegacyReplay(unknown_version),
        ))
        .await;
    }
}

#[tokio::test]
async fn r1_optional_unknown_missing_or_overlapping_scope_blocks_satisfied() {
    for scenario in [
        R1ProofScenario::OptionalOverlap,
        R1ProofScenario::OptionalMissingIdentity,
        R1ProofScenario::OptionalHardlink,
        R1ProofScenario::OptionalBroad,
        R1ProofScenario::OptionalMixedDomain,
        R1ProofScenario::OptionalNotRetiring,
    ] {
        exercise_r1_proof_scenario(scenario, DelegatedEffectCase::Review).await;
    }
}

async fn exercise_r1_proof_scenario(
    scenario: R1ProofScenario,
    delegated_case: DelegatedEffectCase,
) {
    let host = Arc::new(ReviewReadHost::default());
    let services = RuntimeServices::in_memory_with_tool_host(host.clone()).unwrap();
    Box::pin(exercise_reviewed_goal_proof_scenario(
        services,
        host,
        false,
        false,
        delegated_case,
        delegated_case == DelegatedEffectCase::None,
        PhysicalDrainTiming::AfterGoal,
        scenario,
    ))
    .await;
}

#[tokio::test]
async fn proven_effect_goal_revokes_future_mutation_before_physical_worker_drain() {
    exercise_proven_goal_while_workers_drain(DelegatedEffectCase::Review, false).await;
}

#[tokio::test]
async fn proven_required_effect_cannot_be_reviewed_until_the_actual_write_receipt_settles() {
    exercise_proven_goal_while_workers_drain(DelegatedEffectCase::PendingReview, false).await;
}

async fn exercise_proven_goal_while_workers_drain(
    delegated_case: DelegatedEffectCase,
    direct_text: bool,
) {
    let host = Arc::new(ReviewReadHost::default());
    let services = RuntimeServices::in_memory_with_tool_host(host.clone()).unwrap();
    Box::pin(exercise_reviewed_goal_delivery_with_drain(
        services,
        host,
        false,
        false,
        delegated_case,
        direct_text,
        PhysicalDrainTiming::AfterGoal,
    ))
    .await;
}

async fn exercise_reviewed_goal_delivery_kind(
    with_disclosure: bool,
    with_effect: bool,
    delegated_case: DelegatedEffectCase,
    direct_text: bool,
) {
    let read_host = Arc::new(ReviewReadHost::default());
    let services = RuntimeServices::in_memory_with_tool_host(read_host.clone()).expect("runtime");
    exercise_reviewed_goal_delivery_on(
        services,
        read_host,
        with_disclosure,
        with_effect,
        delegated_case,
        direct_text,
    )
    .await;
}

#[test]
fn dispatch_single_flight_is_owned_by_each_runtime_instance() {
    let left = RuntimeServices::in_memory().expect("left runtime");
    let right = RuntimeServices::in_memory().expect("right runtime");
    let key = "same-program|same-task|execute|0".to_string();
    let _left = DispatchFlight::acquire(left.agentic_dispatch_flights(), key.clone())
        .expect("left first flight");
    for _ in 0..128 {
        assert!(
            DispatchFlight::acquire(left.agentic_dispatch_flights(), key.clone()).is_none(),
            "a losing admission must not drop the winner's reservation"
        );
    }
    assert!(
        DispatchFlight::acquire(right.agentic_dispatch_flights(), key.clone()).is_some(),
        "one Runtime instance must never suppress another instance's reconciler"
    );
    drop(_left);
    assert!(DispatchFlight::acquire(left.agentic_dispatch_flights(), key).is_some());
}

#[tokio::test]
async fn market_offer_prefers_available_member_and_exit_reconciles_busy_work() {
    use harness_contract::agent_action::{
        AgentActionStatus, AgentAttemptMode, MessagePublishInput, TaskAttemptDispatchInput,
    };
    let services = RuntimeServices::in_memory().unwrap();
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    let team = actions
        .apply(&root(
            "market-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Readers".into(),
                mission: "inspect evidence".into(),
                objective: None,
            }),
        ))
        .unwrap()
        .changed_refs[0]
        .clone();
    for index in 0..2 {
        actions.apply(&root(&format!("market-member-{index}"), AgentAction::AgentInvite(serde_json::from_value(serde_json::json!({
            "team_ref":team,"role":"Reader","mission":"read evidence","required_capabilities":["read"]
        })).unwrap()))).unwrap();
    }
    let mut tasks = Vec::new();
    let mut publications = Vec::new();
    for index in 0..3 {
        let publication = root(&format!("market-task-{index}"), AgentAction::TaskPublish(serde_json::from_value(serde_json::json!({
            "team_ref":team,"title":"Evidence","objective":"inspect evidence","acceptance":"source checked","required_capabilities":["read"]
        })).unwrap()));
        tasks.push(actions.apply(&publication).unwrap().changed_refs[0].clone());
        publications.push(publication);
    }
    let initial = actions.project("program-dispatch").unwrap();
    let mut ranked =
        helpers::eligible_members(&initial, &initial.tasks[&tasks[0]], DispatchMode::Execute);
    ranked.sort_by_key(|member| {
        helpers::member_dispatch_rank(
            &initial,
            member,
            &initial.tasks[&tasks[0]],
            DispatchMode::Execute,
        )
    });
    let volunteer = ranked[1].agent_id.clone();
    let message: MessagePublishInput = serde_json::from_value(serde_json::json!({
        "topic_ref":"topic:program-dispatch","summary":"I can contribute the requested evidence",
        "intent":{"kind":"offer","task_ref":tasks[0]}
    }))
    .unwrap();
    let forged = actions
        .apply(&root(
            "root-fake-offer",
            AgentAction::MessagePublish(message.clone()),
        ))
        .unwrap();
    assert_eq!(
        forged.error.unwrap().code,
        "offer_requires_bound_team_agent"
    );
    let offer = agent(
        "own-offer",
        &team,
        &volunteer,
        "bound-previous-source-run",
        AgentAction::MessagePublish(message),
    );
    assert_eq!(
        actions.apply(&offer).unwrap().status,
        AgentActionStatus::Applied
    );
    let context = AgenticDispatchContext {
        session_id: "session-dispatch".into(),
        turn_id: "turn-dispatch".into(),
        model_lease: "test".into(),
        permission_ceiling: PermissionMode::ReadOnly,
        resource_scopes: vec!["workspace:.".into()],
    };
    // This admission test authors decline/exit itself. Keep each real Runner
    // backend pending so an unconfigured model cannot fail the opportunity
    // concurrently with those assertions.
    struct MarketPendingWorker {
        task_ref: String,
    }
    #[async_trait::async_trait]
    impl crate::execution_core::graph::executors::AgentTaskBackend for MarketPendingWorker {
        async fn execute(
            &self,
            _: AgentTaskPacket,
        ) -> Result<harness_contract::agent::AgentReturnPacket, String> {
            std::future::pending().await
        }

        async fn cancel(&self, _: &AgentTaskPacket) -> Result<(), String> {
            Ok(())
        }

        fn cancellation_finalized(&self, packet: &AgentTaskPacket) {
            assert_eq!(packet.task_id(), self.task_ref);
        }
    }
    struct MarketWorkerResolver(Vec<Arc<MarketPendingWorker>>);
    impl crate::execution_core::graph::executors::AgentTaskBackendResolver for MarketWorkerResolver {
        fn resolve(
            &self,
            packet: &AgentTaskPacket,
        ) -> Option<Arc<dyn crate::execution_core::graph::executors::AgentTaskBackend>> {
            self.0
                .iter()
                .find(|hold| packet.task_id() == hold.task_ref)
                .map(|hold| {
                    hold.clone()
                        as Arc<dyn crate::execution_core::graph::executors::AgentTaskBackend>
                })
        }
    }
    services
        .agent_task_executor()
        .install_resolver(Arc::new(MarketWorkerResolver(
            tasks
                .iter()
                .map(|task_ref| {
                    Arc::new(MarketPendingWorker {
                        task_ref: task_ref.clone(),
                    })
                })
                .collect(),
        )));
    let first = services
        .dispatch_agentic_followups(&offer, context.clone())
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(
        first[0].agent_ref, volunteer,
        "an eligible volunteer precedes deterministic ranking"
    );
    let second = services
        .dispatch_agentic_followups(&publications[1], context.clone())
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_ne!(
        second[0].agent_ref, volunteer,
        "an admitted but unclaimed opportunity is busy"
    );
    assert!(services
        .dispatch_agentic_followups(&publications[2], context.clone())
        .await
        .unwrap()
        .is_empty());
    assert!(services
        .dispatch_agentic_followups(&offer, context.clone())
        .await
        .unwrap()
        .is_empty());
    let mut occupied = root(
        "duplicate-member-dispatch",
        AgentAction::TaskAttemptDispatch(TaskAttemptDispatchInput {
            task_ref: tasks[2].clone(),
            execution_id: "forbidden-second-run".into(),
            agent_ref: volunteer.clone(),
            membership_id: format!("membership:{volunteer}:{team}"),
            mode: AgentAttemptMode::Execute,
            generation: 0,
        }),
    );
    occupied.actor.kind = AgentActorKind::Supervisor;
    occupied.actor.actor_id = "runtime.program-supervisor".into();
    assert_eq!(
        actions.apply(&occupied).unwrap().error.unwrap().code,
        "agent_has_active_opportunity"
    );
    let decline = agent(
        "volunteer-declines-original",
        &team,
        &volunteer,
        &first[0].graph_id,
        AgentAction::MessagePublish(
            serde_json::from_value(serde_json::json!({
                "topic_ref":"topic:program-dispatch","summary":"The other source is a better fit",
                "intent":{"kind":"decline","task_ref":tasks[0]}
            }))
            .unwrap(),
        ),
    );
    assert_eq!(
        actions.apply(&decline).unwrap().status,
        AgentActionStatus::Applied
    );
    let resumed = services
        .dispatch_ready_agentic_work("program-dispatch")
        .await
        .unwrap();
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].task_ref, tasks[2]);
    assert_eq!(resumed[0].agent_ref, volunteer);
    assert!(services
        .dispatch_ready_agentic_work("program-dispatch")
        .await
        .unwrap()
        .is_empty());
    let projection = actions.project("program-dispatch").unwrap();
    assert_eq!(
        projection.tasks[&tasks[0]].status,
        AgenticTaskStatus::Published
    );
    assert!(projection.tasks[&tasks[0]].active_attempts.is_empty());
    assert!(projection
        .tasks
        .values()
        .all(|task| task.claimant.is_none() && task.failed_attempts == 0));
}

#[tokio::test]
async fn typed_decline_dispatches_next_member_once_and_exhaustion_stays_quiet() {
    use harness_contract::agent_action::{AgentActionStatus, MessagePublishInput};
    let services = Arc::new(RuntimeServices::in_memory().unwrap());
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    let team = actions
        .apply(&root(
            "decline-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Candidates".into(),
                mission: "inspect evidence".into(),
                objective: None,
            }),
        ))
        .unwrap()
        .changed_refs[0]
        .clone();
    for index in 0..2 {
        actions.apply(&root(&format!("decline-member-{index}"), AgentAction::AgentInvite(
            serde_json::from_value(serde_json::json!({"team_ref":team,"role":"Reader","mission":"read evidence","required_capabilities":["read"]})).unwrap()
        ))).unwrap();
    }
    let publish = root("decline-task", AgentAction::TaskPublish(
        serde_json::from_value(serde_json::json!({"team_ref":team,"title":"Evidence","objective":"read evidence","acceptance":"evidence inspected","required_capabilities":["read"]})).unwrap()
    ));
    let task = actions.apply(&publish).unwrap().changed_refs[0].clone();
    let context = AgenticDispatchContext {
        session_id: "session-dispatch".into(),
        turn_id: "turn-dispatch".into(),
        model_lease: "test".into(),
        permission_ceiling: PermissionMode::ReadOnly,
        resource_scopes: vec!["workspace:.".into()],
    };
    let first = services
        .dispatch_agentic_followups(&publish, context.clone())
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    let mut receipt = first[0].clone();
    let mut visited = BTreeSet::new();
    for index in 0..2 {
        assert!(visited.insert(receipt.agent_ref.clone()));
        let input: MessagePublishInput = serde_json::from_value(serde_json::json!({
            "topic_ref":"topic:program-dispatch", "summary":"Another contributor is better suited",
            "intent":{"task_ref":task,"kind":"decline"}
        }))
        .unwrap();
        let decline = agent(
            &format!("decline-{index}"),
            &team,
            &receipt.agent_ref,
            &receipt.graph_id,
            AgentAction::MessagePublish(input),
        );
        assert_eq!(
            actions.apply(&decline).unwrap().status,
            AgentActionStatus::Applied
        );
        let declined_revision = actions.project("program-dispatch").unwrap().revision;
        let graph = services
            .graph_state_store()
            .load(&receipt.graph_id)
            .unwrap();
        let packet: AgentTaskPacket = serde_json::from_str(&graph.nodes[0].payload_ref).unwrap();
        services
            .settle_abandoned_agentic_attempt(&packet, "declined worker returned")
            .await
            .unwrap();
        assert_eq!(
            actions.project("program-dispatch").unwrap().revision,
            declined_revision,
            "semantic decline must not become a backend failure on return"
        );
        let next = services
            .dispatch_agentic_followups(&decline, context.clone())
            .await
            .unwrap();
        assert_eq!(next.len(), usize::from(index == 0));
        assert!(actions.apply(&decline).unwrap().duplicate);
        assert!(services
            .dispatch_agentic_followups(&decline, context.clone())
            .await
            .unwrap()
            .is_empty());
        if let Some(next) = next.first() {
            receipt = next.clone();
        }
    }
    let projection = actions.project("program-dispatch").unwrap();
    assert_eq!(projection.tasks[&task].status, AgenticTaskStatus::Published);
    assert_eq!(projection.tasks[&task].failed_attempts, 0);
    assert!(projection.tasks[&task].active_attempts.is_empty());
    assert!(helpers::eligible_members(
        &projection,
        &projection.tasks[&task],
        DispatchMode::Execute
    )
    .is_empty());
    assert!(services
        .recover_agentic_programs_on_startup()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        actions.project("program-dispatch").unwrap().revision,
        projection.revision
    );
}

#[tokio::test]
async fn dispatch_tries_admissible_members_without_duplicate_model_work() {
    let services = Arc::new(RuntimeServices::in_memory().unwrap());
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    let team = actions
        .apply(&root(
            "candidate-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Candidates".into(),
                mission: "read evidence".into(),
                objective: None,
            }),
        ))
        .unwrap()
        .changed_refs[0]
        .clone();
    for index in 0..2 {
        actions
            .apply(&root(
                &format!("candidate-{index}"),
                AgentAction::AgentInvite(AgentInviteInput {
                    team_ref: team.clone(),
                    role: "Reader".into(),
                    mission: "read evidence".into(),
                    required_capabilities: vec!["read".into()],
                    existing_agent_ref: None,
                    definition_ref: None,
                    model_profile_ref: None,
                    expertise_hints: vec![],
                    execution_requirements: vec![],
                }),
            ))
            .unwrap();
    }
    let task = actions
        .apply(&root(
            "candidate-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team,
                title: "Read evidence".into(),
                objective: "read evidence".into(),
                acceptance: "evidence inspected".into(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".into()],
                depends_on: vec![],
                obligation_refs: vec![],
                purpose: Default::default(),
                execution_requirements: vec![],
                expertise_hints: vec![],
            }),
        ))
        .unwrap()
        .changed_refs[0]
        .clone();
    let mut projection = actions.project("program-dispatch").unwrap();
    let mut ordered =
        helpers::eligible_members(&projection, &projection.tasks[&task], DispatchMode::Execute);
    ordered.sort_by_key(|member| {
        helpers::member_dispatch_rank(
            &projection,
            member,
            &projection.tasks[&task],
            DispatchMode::Execute,
        )
    });
    let invalid = ordered[0].agent_id.clone();
    let valid = ordered[1].agent_id.clone();
    drop(ordered);
    projection
        .agents
        .get_mut(&invalid)
        .unwrap()
        .model_profile_ref = Some("missing-profile".into());
    let context = AgenticDispatchContext {
        session_id: "session-dispatch".into(),
        turn_id: "turn-dispatch".into(),
        model_lease: "test".into(),
        permission_ceiling: PermissionMode::ReadOnly,
        resource_scopes: vec!["workspace:.".into()],
    };
    let trigger = root(
        "candidate-wake",
        AgentAction::StateInspect(harness_contract::agent_action::StateInspectInput {
            query: None,
            wait_for_workers: false,
            scope_ref: None,
            after_revision: None,
            page_cursor: None,
            entry_ref: None,
        }),
    );
    let receipts = services
        .dispatch_agentic_task(
            &projection,
            &task,
            DispatchMode::Execute,
            &context,
            &trigger,
        )
        .await
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].agent_ref, valid);
    assert!(services
        .dispatch_agentic_task(
            &projection,
            &task,
            DispatchMode::Execute,
            &context,
            &trigger
        )
        .await
        .unwrap()
        .is_empty());
    // Publishing discussion alone must not launch an idle worker. A typed
    // request for a ready Task enters the existing dispatcher once.
    let followup_task = actions
        .apply(&root(
            "typed-wake-task",
            AgentAction::TaskPublish(
                serde_json::from_value(serde_json::json!({
                    "team_ref":projection.tasks[&task].team_id,"title":"new evidence",
                    "objective":"resolve the new cited gap","acceptance":"source checked",
                    "required_capabilities":["read"]
                }))
                .unwrap(),
            ),
        ))
        .unwrap()
        .changed_refs[0]
        .clone();
    for (label, intent, expected) in [
        ("discussion", None, 0),
        (
            "request-help",
            Some(harness_contract::agent_action::TaskIntentKind::RequestHelp),
            1,
        ),
        (
            "repeat-help",
            Some(harness_contract::agent_action::TaskIntentKind::RequestHelp),
            0,
        ),
    ] {
        let message = root(
            label,
            AgentAction::MessagePublish(harness_contract::agent_action::MessagePublishInput {
                topic_ref: format!("topic:{}", projection.program_id),
                summary: Some("A new contribution is available".into()),
                content_ref: None,
                refs: vec![followup_task.clone()],
                recipients: vec![],
                issue_dispositions: vec![],
                intent: intent.map(|kind| harness_contract::agent_action::TaskIntent {
                    task_ref: followup_task.clone(),
                    kind,
                    reason_ref: None,
                    requested_capability_refs: vec![],
                }),
            }),
        );
        assert_eq!(
            actions.apply(&message).unwrap().status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        assert_eq!(
            services
                .dispatch_agentic_followups(&message, context.clone())
                .await
                .unwrap()
                .len(),
            expected,
            "{label}"
        );
    }
    projection.agents.get_mut(&valid).unwrap().model_profile_ref =
        Some("also-missing-profile".into());
    // Occupied work is a no-op, even when a stale caller's semantic snapshot
    // contains bad profiles. Test capability rejection at admission itself.
    for member in [&invalid, &valid] {
        let error = super::admission::resolve_agentic_execution_admission(
            &services,
            &projection.agents[member],
            &projection.tasks[&task],
            &context,
            DispatchMode::Execute,
        )
        .unwrap_err();
        assert!(error.contains("profile"), "{error}");
    }
}

#[test]
fn admission_intersects_multiple_definition_skills_and_custom_tools() {
    let services = RuntimeServices::in_memory().expect("runtime");
    let mut entry = services
        .agent_runtime()
        .catalog()
        .all()
        .into_iter()
        .find(|entry| entry.capabilities == vec!["read".to_string()])
        .expect("read definition");
    entry.skill_refs = vec!["skill.one".to_string(), "skill.two@2".to_string()];
    let profile = |skill_id: &str, version: Option<&str>| SkillCapabilityProfile {
        skill_id: skill_id.to_string(),
        name: skill_id.to_string(),
        version: version.map(str::to_string),
        source_root: format!("/skills/{skill_id}"),
        package_fingerprint: format!("digest:{skill_id}"),
        kind: SkillKind::Workflow,
        lifecycle_status: SkillLifecycleStatus::UsablePrompt,
        adapters: vec![SkillAdapterKind::PromptOnly],
        risk_level: SkillRiskLevel::Low,
        entrypoints: Vec::new(),
        inspection_summary: vec!["custom analysis".to_string()],
        structured_dependencies: Vec::new(),
    };
    let catalog = RuntimeSkillCatalog::new(
        vec![profile("skill.one", None), profile("skill.two", Some("2"))],
        vec![
            crate::RuntimeSkillPromptAsset {
                skill_id: "skill.one".to_string(),
                version: None,
                content: "one".to_string(),
                source_ref: "skill://one".to_string(),
                tool_refs: vec!["custom_data_lookup".to_string()],
            },
            crate::RuntimeSkillPromptAsset {
                skill_id: "skill.two".to_string(),
                version: Some("2".to_string()),
                content: "two".to_string(),
                source_ref: "skill://two@2".to_string(),
                tool_refs: vec!["custom_report_reader".to_string()],
            },
        ],
    );
    let (skills, skill_tools) = effective_skill_grants(&entry, &catalog);
    assert_eq!(skills, vec!["skill.one", "skill.two@2"]);
    assert_eq!(
        skill_tools,
        BTreeSet::from([
            "custom_data_lookup".to_string(),
            "custom_report_reader".to_string(),
        ])
    );

    let resolved = resolve_agent_capability(AgentCapabilityRequest {
        role_id: "custom-reader".to_string(),
        allowed_capabilities: vec!["read".to_string()],
        evidence_duties: Vec::new(),
    });
    let mut requested = resolved.allowed_tools.clone();
    requested.extend(AGENT_ACTION_TOOL_IDS.iter().map(|tool| (*tool).to_string()));
    requested.extend(skill_tools.iter().cloned());
    let snapshot = AgenticToolHostSnapshot {
        tools: requested.clone(),
    };
    let (allowed, missing_capabilities, _missing_tool_alternatives) =
        intersect_agentic_tools(&resolved, &requested, &skill_tools, Some(&snapshot))
            .expect("custom tools are admitted from the active skill and host intersection");
    assert!(allowed.contains(&"custom_data_lookup".to_string()));
    assert!(allowed.contains(&"custom_report_reader".to_string()));
    assert!(missing_capabilities.is_empty());
}

#[test]
fn semantic_capability_hints_translate_without_becoming_physical_authority() {
    let services = RuntimeServices::in_memory().expect("runtime");
    let actions = services.agent_action_service();
    let team = actions
        .apply(&root(
            "unsupported-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Specialists".to_string(),
                mission: "exercise a workspace extension".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    actions
        .apply(&root(
            "unsupported-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Specialist".to_string(),
                mission: "research evidence and implement a verified Python experiment".to_string(),
                required_capabilities: vec![
                    "custom_domain_operation".to_string(),
                    "web-research".to_string(),
                    "python".to_string(),
                    "verification".to_string(),
                ],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("the semantic action contract must not own the capability catalog");
    let task = root(
        "unsupported-task",
        AgentAction::TaskPublish(TaskPublishInput {
            team_ref: team,
            title: "Run domain operation".to_string(),
            objective: "prove semantic labels are translated by Runtime".to_string(),
            acceptance: "a least-privilege physical capability set is derived".to_string(),
            criterion_source: Default::default(),
            acceptance_checks: Vec::new(),
            required_capabilities: vec!["evidence-gathering".to_string()],
            depends_on: Vec::new(),

            obligation_refs: Vec::new(),
            purpose: Default::default(),
            execution_requirements: Vec::new(),
            expertise_hints: Vec::new(),
        }),
    );
    let task_ref = actions.apply(&task).expect("task").changed_refs[0].clone();
    let projection = actions.project("program-dispatch").expect("projection");
    let member = projection.agents.values().next().expect("member");
    let task = projection.tasks.get(&task_ref).expect("task");
    let translated = super::admission::required_capabilities(member, task, DispatchMode::Execute);

    assert_eq!(translated, ["network", "read", "search", "test", "write"]);
    assert!(!translated.contains(&"custom_domain_operation".to_string()));
    let selected = super::admission::select_catalog_entry(
        &services.agent_runtime().catalog().all(),
        member,
        task,
        &translated,
    )
    .expect("a general autonomous definition composes the standard physical effects");
    assert_eq!(selected.name, "Autonomous");

    let mut reviewer = member.clone();
    reviewer.required_capabilities = vec!["read".into()];
    reviewer.execution_requirements = vec!["test".into()];
    reviewer.definition_ref = Some("builtin/cowd/execute".into());
    let mut producer_task = task.clone();
    producer_task.required_capabilities = vec!["network".into(), "write".into()];
    producer_task.execution_requirements = vec!["web-research".into()];
    let review =
        super::admission::required_capabilities(&reviewer, &producer_task, DispatchMode::Review);
    assert_eq!(review, ["read", "test"]);
    super::admission::select_catalog_entry(
        &services.agent_runtime().catalog().all(),
        &reviewer,
        &producer_task,
        &review,
    )
    .expect("reviewer need not reproduce network research to review its evidence");
    let execute =
        super::admission::required_capabilities(&reviewer, &producer_task, DispatchMode::Execute);
    assert!(execute.contains(&"network".into()) && execute.contains(&"write".into()));
}

#[test]
fn dispatch_rank_spreads_independent_tasks_to_idle_members() {
    let services = RuntimeServices::in_memory().expect("runtime");
    let actions = services.agent_action_service();
    let team = actions
        .apply(&root(
            "load-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Parallel workers".to_string(),
                mission: "execute independent work concurrently".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let busy = actions
        .apply(&root(
            "busy-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Manifest inspector".to_string(),
                mission: "inspect package manifests".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("busy agent")
        .changed_refs[0]
        .clone();
    let idle = actions
        .apply(&root(
            "idle-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Exclude verifier".to_string(),
                mission: "verify exclusion rules".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("idle agent")
        .changed_refs[0]
        .clone();
    let first_task = actions
        .apply(&root(
            "first-load-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Inspect manifests".to_string(),
                objective: "inspect package metadata".to_string(),
                acceptance: "report evidence".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("first task")
        .changed_refs[0]
        .clone();
    let second_task = actions
        .apply(&root(
            "second-load-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Verify exclusions".to_string(),
                objective: "verify exclusion rules".to_string(),
                acceptance: "report evidence".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("second task")
        .changed_refs[0]
        .clone();
    actions
        .apply(&agent(
            "busy-claim",
            &team,
            &busy,
            "busy-execution",
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: first_task,
                reason: None,
            }),
        ))
        .expect("claim first task");

    let projection = actions.project("program-dispatch").expect("projection");
    let task = &projection.tasks[&second_task];
    assert!(
        member_dispatch_rank(
            &projection,
            &projection.agents[&idle],
            task,
            DispatchMode::Execute,
        ) < member_dispatch_rank(
            &projection,
            &projection.agents[&busy],
            task,
            DispatchMode::Execute,
        ),
        "an idle eligible member must outrank a member already executing independent work"
    );
}

#[tokio::test]
async fn startup_reconcile_admits_committed_unstarted_work_once() {
    verify_startup_reconciliation(false).await;
}

#[tokio::test]
async fn startup_reconcile_isolates_a_corrupt_program_without_duplicate_work() {
    verify_startup_reconciliation(true).await;
}

async fn verify_startup_reconciliation(with_corrupt_program: bool) {
    let services = Arc::new(RuntimeServices::in_memory().expect("runtime"));
    // This test reconciles an active graph twice. An unconfigured model may
    // otherwise fail between the two calls and legitimately admit a retry,
    // which is not duplicate admission of the still-running attempt.
    services.agent_task_executor().install_resolver(Arc::new(
        crate::agentic::coordination::tests::ControlledAgenticWorker,
    ));
    if with_corrupt_program {
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                stream_id: "agentic-program:aaa-corrupt".into(),
                scope: crate::RuntimeEventScope::Program,
                kind: "agentic.action_applied".into(),
                status: None,
                actor: None,
                refs: vec![],
                payload: serde_json::json!({"envelope": "not an action envelope"}),
            })
            .unwrap();
    }
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    let team = actions
        .apply(&root(
            "recovery-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Recovery".to_string(),
                mission: "resume without model polling".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    actions
        .apply(&root(
            "recovery-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Recovery worker".to_string(),
                mission: "resume durable work".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("agent");
    actions
        .apply(&root(
            "recovery-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team,
                title: "Resume me".to_string(),
                objective: "prove startup reconciliation".to_string(),
                acceptance: "a real graph is admitted".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("task");

    let first = services.recover_agentic_programs_on_startup().await;
    let graph_id = if with_corrupt_program {
        let error = first.expect_err("partial recovery must remain a reported failure");
        assert!(error.contains("aaa-corrupt"), "{error}");
        let projection = actions.project("program-dispatch").unwrap();
        let attempts = projection
            .tasks
            .values()
            .flat_map(|task| task.active_attempts.values())
            .collect::<Vec<_>>();
        assert_eq!(
            attempts.len(),
            1,
            "healthy work must still be physically admitted"
        );
        attempts[0].execution_id.clone()
    } else {
        let first = first.expect("first recovery");
        assert_eq!(first.len(), 1);
        first[0].graph_id.clone()
    };
    let task_ref = actions
        .project("program-dispatch")
        .unwrap()
        .tasks
        .keys()
        .next()
        .unwrap()
        .clone();
    let graph = services
        .graph_state_store()
        .load(&graph_id)
        .expect("admitted Agent-first graph");
    let packet = serde_json::from_str::<AgentTaskPacket>(&graph.nodes[0].payload_ref)
        .expect("compiled Agent-first packet");
    assert!(
        packet.acceptance.is_empty() && packet.output_acceptance.is_empty(),
        "natural-language Program Task acceptance must stay in the Program review contract, not become an unsatisfiable physical Agent terminal field"
    );
    assert!(graph.nodes[0].acceptance.criteria.is_empty());
    let second = services.recover_agentic_programs_on_startup().await;
    if with_corrupt_program {
        assert!(second.unwrap_err().contains("aaa-corrupt"));
        let projection = actions.project("program-dispatch").unwrap();
        let attempts = projection
            .tasks
            .values()
            .flat_map(|task| task.active_attempts.values())
            .collect::<Vec<_>>();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].execution_id, graph_id);
        assert_eq!(
            services
                .event_store()
                .stream_revision("agentic-program:aaa-corrupt")
                .unwrap(),
            1
        );
    } else {
        assert!(
            second.expect("idempotent recovery").is_empty(),
            "active graph must suppress duplicate paid work"
        );
    }

    // An unresolved Program must not authorize new work after its root ended.
    let mut projection = actions.project("program-dispatch").expect("projection");
    projection.root_execution_id = Some("cancelled-dispatch-root".into());
    let context = AgenticDispatchContext {
        session_id: "session-dispatch".into(),
        turn_id: "turn-dispatch".into(),
        model_lease: "test".into(),
        permission_ceiling: PermissionMode::ReadOnly,
        resource_scopes: Vec::new(),
    };
    let trigger = root(
        "root-fence-inspect",
        AgentAction::StateInspect(harness_contract::agent_action::StateInspectInput {
            query: None,
            scope_ref: None,
            after_revision: None,
            page_cursor: None,
            entry_ref: None,
            wait_for_workers: false,
        }),
    );
    let missing = services
        .dispatch_agentic_task(
            &projection,
            &task_ref,
            DispatchMode::Review,
            &context,
            &trigger,
        )
        .await
        .expect_err("a missing bound root must never fall back to unbound dispatch");
    assert!(missing.contains("agentic_dispatch_root_unavailable"));
    let mut root_graph = ExecutionGraph::new("cancelled root");
    root_graph.id = "cancelled-dispatch-root".into();
    crate::test_support::attach_execution_graph_lineage(&mut root_graph);
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::AgentTask,
        AgentTaskExecutor::KIND,
        "test-packet",
    );
    node.id = "cancelled-root-node".into();
    node.idempotency_key = "cancelled-root-node:1".into();
    root_graph
        .node_statuses
        .insert(node.id.clone(), ExecutionNodeStatus::Planned);
    root_graph.nodes.push(node);
    services
        .commit_service()
        .register_graph(root_graph)
        .expect("register root");
    let root_graph = services
        .graph_state_store()
        .load("cancelled-dispatch-root")
        .expect("root");
    services
        .commit_service()
        .apply_command(
            &root_graph,
            &ExecutionGraphCommand::Cancel {
                expected_revision: root_graph.revision,
                reason: "user cancelled".into(),
            },
        )
        .expect("cancel root");
    for mode in [DispatchMode::Execute, DispatchMode::Review] {
        assert!(services
            .dispatch_agentic_task(&projection, &task_ref, mode, &context, &trigger,)
            .await
            .expect("terminal root is not an admission error")
            .is_empty());
    }
}

#[tokio::test]
async fn startup_reconcile_fails_closed_after_partial_dispatch_without_duplicating_admitted_work() {
    let services = Arc::new(RuntimeServices::in_memory().expect("runtime"));
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    let team = actions
        .apply(&root(
            "partial-recovery-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Partial recovery".to_string(),
                mission: "prove startup admission remains closed on one failed Task".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let member = actions
        .apply(&root(
            "partial-recovery-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Recovery worker".to_string(),
                mission: "run every admissible task".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("agent")
        .changed_refs[0]
        .clone();
    let valid_task = actions
        .apply(&root(
            "partial-recovery-valid-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Admissible read".to_string(),
                objective: "admit one idempotent graph".to_string(),
                acceptance: "the graph exists once".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("valid task")
        .changed_refs[0]
        .clone();
    let invalid_task = actions
        .apply(&root(
            "partial-recovery-invalid-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team,
                title: "Unavailable connector action".to_string(),
                objective: "remain durable until a matching Agent definition exists".to_string(),
                acceptance: "startup must stay closed".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["connector_action".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("durable invalid task")
        .changed_refs[0]
        .clone();

    let first_error = services
        .recover_agentic_programs_on_startup()
        .await
        .expect_err("one failed Task must fail the whole startup reconciliation pass");
    assert!(first_error.contains("startup dispatch reconciliation failed"));
    assert!(first_error.contains(&invalid_task));
    assert!(first_error.contains("after 1 idempotent dispatch receipt(s)"));

    let projection = actions.project("program-dispatch").expect("projection");
    let valid_graph_id = deterministic_graph_id(
        &projection.program_id,
        &valid_task,
        &member,
        DispatchMode::Execute,
        projection.tasks[&valid_task].claim_generation,
    );
    let before_retry = services
        .graph_state_store()
        .load(&valid_graph_id)
        .expect("the independent valid Task was durably admitted");

    let retry_error = services
        .recover_agentic_programs_on_startup()
        .await
        .expect_err("the unresolved Task must keep startup admission closed");
    assert!(retry_error.contains(&invalid_task));
    let after_retry = services
        .graph_state_store()
        .load(&valid_graph_id)
        .expect("the admitted graph remains available");
    assert_eq!(
        after_retry.id, before_retry.id,
        "retry must retain the deterministic graph identity"
    );
    assert_eq!(
        services
            .event_reader()
            .list_stream(&valid_graph_id)
            .expect("deterministic graph event stream")
            .iter()
            .filter(|event| event.kind == "execution_graph.planned")
            .count(),
        1,
        "retry must not register a second graph or duplicate paid work"
    );
}

#[tokio::test]
async fn action_followups_surface_partial_dispatch_failure_without_duplicating_admitted_work() {
    let services = Arc::new(RuntimeServices::in_memory().expect("runtime"));
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    let team = actions
        .apply(&root(
            "partial-action-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Partial action".to_string(),
                mission: "surface every post-commit dispatch failure".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let valid_task = actions
        .apply(&root(
            "partial-action-valid-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Admissible read".to_string(),
                objective: "admit one deterministic graph".to_string(),
                acceptance: "the graph exists once".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("valid task")
        .changed_refs[0]
        .clone();
    let invalid_task = actions
        .apply(&root(
            "partial-action-invalid-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Unavailable connector action".to_string(),
                objective: "remain durable until a matching Agent definition exists".to_string(),
                acceptance: "the caller observes the deferred dispatch".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["connector_action".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("durable invalid task")
        .changed_refs[0]
        .clone();
    let invite = root(
        "partial-action-agent",
        AgentAction::AgentInvite(AgentInviteInput {
            team_ref: team,
            role: "Read worker".to_string(),
            mission: "run admissible work".to_string(),
            required_capabilities: vec!["read".to_string()],
            existing_agent_ref: None,
            definition_ref: None,
            model_profile_ref: None,
            expertise_hints: Vec::new(),
            execution_requirements: Vec::new(),
        }),
    );
    let member = actions.apply(&invite).expect("agent").changed_refs[0].clone();
    let context = AgenticDispatchContext {
        session_id: "session-dispatch".to_string(),
        turn_id: "turn-dispatch".to_string(),
        model_lease: "test".to_string(),
        permission_ceiling: PermissionMode::ReadOnly,
        resource_scopes: Vec::new(),
    };

    let error = services
        .dispatch_agentic_followups(&invite, context.clone())
        .await
        .expect_err("one failed followup must be visible even when independent work was admitted");
    assert!(error.contains(&invalid_task));
    assert!(error.contains("1 idempotent dispatch receipt(s)"));

    let projection = actions.project("program-dispatch").expect("projection");
    let graph_id = deterministic_graph_id(
        &projection.program_id,
        &valid_task,
        &member,
        DispatchMode::Execute,
        projection.tasks[&valid_task].claim_generation,
    );
    let before_retry = services
        .graph_state_store()
        .load(&graph_id)
        .expect("valid Task graph");
    let retry_error = services
        .dispatch_agentic_followups(&invite, context)
        .await
        .expect_err("the unresolved followup remains visible on retry");
    assert!(retry_error.contains(&invalid_task));
    let after_retry = services
        .graph_state_store()
        .load(&graph_id)
        .expect("valid Task graph after retry");
    assert_eq!(after_retry.id, before_retry.id);
    assert_eq!(
        services
            .event_reader()
            .list_stream(&graph_id)
            .unwrap()
            .iter()
            .filter(|event| event.kind == "execution_graph.planned")
            .count(),
        1,
        "worker progress may advance revision; retry must not register another graph"
    );
    assert_eq!(
        services
            .event_reader()
            .list_stream("agentic-program:program-dispatch")
            .unwrap()
            .iter()
            .filter(|event| event.kind == "agentic.action_applied"
                && event.payload["envelope"]["action"]["kind"] == "task_attempt_dispatch"
                && event.payload["envelope"]["action"]["input"]["task_ref"] == valid_task)
            .count(),
        1,
        "retry must not admit another physical attempt for the same task opportunity"
    );
}

#[tokio::test]
async fn startup_reconcile_releases_missing_graph_claim_without_waiting_for_lease() {
    let services = Arc::new(RuntimeServices::in_memory().expect("runtime"));
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    let team = actions
        .apply(&root(
            "orphan-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Recovery".to_string(),
                mission: "recover orphaned reservations".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let member = actions
        .apply(&root(
            "orphan-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Recovery worker".to_string(),
                mission: "resume immediately".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("agent")
        .changed_refs[0]
        .clone();
    let task = actions
        .apply(&root(
            "orphan-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Orphaned reservation".to_string(),
                objective: "recover it".to_string(),
                acceptance: "new graph admitted".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("task")
        .changed_refs[0]
        .clone();
    let missing_execution = "agentic-graph:missing-after-crash";
    assert_eq!(
        actions
            .apply(&agent(
                "orphan-claim",
                &team,
                &member,
                missing_execution,
                AgentAction::TaskClaim(TaskClaimInput {
                    task_ref: task.clone(),
                    reason: None,
                }),
            ))
            .expect("claim")
            .status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );

    let recovered = services
        .recover_agentic_programs_on_startup()
        .await
        .expect("recovery");
    assert_eq!(recovered.len(), 1);
    assert_ne!(recovered[0].graph_id, missing_execution);
    let projection = actions.project("program-dispatch").expect("projection");
    assert_eq!(projection.tasks[&task].status, AgenticTaskStatus::Published);
    assert_eq!(projection.tasks[&task].claim_execution_id, None);
    assert_eq!(projection.tasks[&task].claimant, None);
}

#[tokio::test]
async fn cross_team_reviewer_resolves_own_identity_and_can_accept() {
    with_native_cross_team_before_a(|services, program, prepared, _| {
        let native = program
            .tasks
            .values()
            .find(|task| task.title == "Native immutable source proof")
            .unwrap();
        assert_eq!(native.status, AgenticTaskStatus::Accepted);
        assert_ne!(native.claimant, native.reviewed_by);
        assert!(prepared.gaps.is_empty());
        let after = services
            .agent_action_service()
            .commit_program_conclusion(program, prepared)
            .unwrap();
        assert_eq!(after.status, crate::AgenticProgramStatus::Verified);
    })
    .await;
}

#[tokio::test]
async fn low_risk_root_reviews_artifact_and_tool_content_without_creating_work_orders() {
    exercise_low_risk_root_review(false, false).await;
}

#[tokio::test]
async fn low_risk_root_verifies_local_writes_without_an_extra_model_or_work_order() {
    exercise_low_risk_root_review(true, false).await;
}

#[tokio::test]
async fn external_decision_is_read_and_reviewed_before_the_original_goal_can_complete() {
    exercise_low_risk_root_review(false, true).await;
}

async fn exercise_low_risk_root_review(local_effect: bool, external_decision: bool) {
    use harness_contract::agent_action::{
        AgentActionStatus, ObjectiveCompleteRequestInput, ObjectiveReviewDecision,
        ObjectiveReviewInput,
    };
    use harness_contract::goal::{GoalCompletion, GoalContract};
    let host = Arc::new(ReviewReadHost::default());
    let services = RuntimeServices::in_memory_with_tool_host(host.clone()).unwrap();
    host.services.set(Arc::downgrade(&services)).unwrap();
    install_real_process_fixture(&services, &host).await;
    services
        .goal_store()
        .create(GoalContract {
            id: "goal:root-agentic-execution".into(),
            session_id: "session-dispatch".into(),
            objective: "dispatch objective".into(),
            criteria: vec![harness_contract::goal::AcceptanceCriterion {
                id: "reviewed-delivery".into(),
                statement: "Deliver checked source-backed work".into(),
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
                objective_id: "objective-dispatch".into(),
                session_id: "session-dispatch".into(),
                turn_id: "turn-dispatch".into(),
                root_execution_id: "root-agentic-execution".into(),
                agentic_program_id: "program-dispatch".into(),
            }),
            spec_revision: 1,
            spec_digest: "dispatch-test".into(),
            review_refs: vec![],
            waiting: None,
            participation_requirement: None,
            obligations: vec![],
            recovery: None,
            terminal: None,
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
            reviews: Vec::new(),
        })
        .unwrap();
    let bind = |id: &str, action: AgentAction| {
        let mut envelope = root(id, action);
        envelope.actor.root_execution_id = Some("root-agentic-execution".into());
        envelope.actor.required_team_count = 0;
        envelope
    };
    let content = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/plain".into(),
                visibility_scope: "session:session-dispatch".into(),
                expected_bytes: None,
                original_name: None,
            },
            b"The current result was read and checked.",
        )
        .await
        .unwrap();
    let artifact = services
        .submit_agent_action(&bind(
            "root-content",
            AgentAction::ArtifactCommit(ArtifactCommitInput {
                content_ref: content.selector.clone(),
                kind: "answer".into(),
                title: "Direct answer".into(),
                relates_to: vec![],
            }),
        ))
        .await
        .unwrap();
    assert_eq!(artifact.status, AgentActionStatus::Applied, "{artifact:?}");
    let review = |id: &str, refs: Vec<String>| {
        bind(
            id,
            AgentAction::ObjectiveReview(ObjectiveReviewInput {
                criterion_ref: "reviewed-delivery".into(),
                decision: ObjectiveReviewDecision::Satisfied,
                result_refs: refs,
                evidence_refs: vec![content.selector.clone()],
                reason_ref: content.selector.clone(),
                reason: String::new(),
            }),
        )
    };
    assert!(services
        .submit_agent_action(&review(
            "missing-read",
            vec![artifact.changed_refs[0].clone()]
        ))
        .await
        .is_err());
    let (raw, runtime) =
        root_reads_generic_results(&services, host, &content.selector, true, local_effect).await;
    let raw = raw.unwrap();
    let result_artifact = if local_effect {
        let publication = services
            .submit_agent_action(&bind(
                "root-summary-after-write",
                AgentAction::ArtifactCommit(ArtifactCommitInput {
                    content_ref: content.selector.clone(),
                    kind: "answer".into(),
                    title: "Explanation after local write".into(),
                    relates_to: vec![],
                }),
            ))
            .await
            .unwrap();
        assert_eq!(publication.status, AgentActionStatus::Applied);
        publication.changed_refs[0].clone()
    } else {
        artifact.changed_refs[0].clone()
    };
    let mut references = vec![result_artifact, content.selector.clone(), raw];
    if external_decision {
        use harness_contract::policy::*;
        let mut graph = ExecutionGraph::new("external decision source");
        graph.id = "root-agentic-execution".into();
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: "session-dispatch".into(),
            turn_id: "turn-dispatch".into(),
            root_task_id: "task-root-dispatch".into(),
            task_id: "task-root-dispatch".into(),
            generation: 1,
        });
        let node =
            ExecutionNodeSpec::new(ExecutionNodeKind::Approval, "approval", "decision source");
        let reference =
            crate::execution_core::graph::executors::graph_approval_id(&graph.id, &node.id);
        graph.nodes.push(node);
        services.commit_service().register_graph(graph).unwrap();
        let source = ApprovalSource {
            kind: ApprovalSourceKind::Session,
            session_id: Some("session-dispatch".into()),
            agent_id: None,
            team_id: None,
            mission_id: None,
            resource_ref: None,
            review_ref: None,
            application: None,
        };
        services
            .approval_queue()
            .submit_scoped(
                &reference,
                SubmitApprovalRequest {
                    context: ApprovalContext::owned(
                        &source,
                        "operator decision",
                        "root-agentic-execution",
                    ),
                    source,
                    action: "operator decision".into(),
                    summary: "Operator decides whether the proposed action may run".into(),
                    risk: harness_contract::core::TaskRisk::Medium,
                    domain: ApprovalDomain::Execution,
                    blocks_execution: true,
                    evidence_refs: vec![],
                    timeout_policy: ApprovalTimeoutPolicy::AutoDeny,
                },
            )
            .unwrap();
        assert!(services
            .approval_result_content("session-dispatch", &reference)
            .await
            .is_err());
        services
            .approval_queue()
            .decide_surface_human(
                "operator:test",
                ApprovalDecisionCommand {
                    approval_id: reference.clone(),
                    approved: false,
                    skip: false,
                    reason: "Operator declined the requested action".into(),
                    scope: ApprovalGrantScope::Once,
                    actor: ApprovalDecisionActor {
                        kind: ApprovalDecisionActorKind::Human,
                        actor_id: "replaced-by-authenticated-surface".into(),
                    },
                    evidence_refs: vec![],
                },
            )
            .unwrap();
        assert!(services
            .approval_result_content("foreign-session", &reference)
            .await
            .is_err());
        let (_, decided, _) = services
            .approval_result_content("session-dispatch", &reference)
            .await
            .unwrap();
        assert!(
            !decided.decision.unwrap().approved,
            "denial is an authentic result, not approval to execute"
        );
        references.push(reference.clone());
        assert!(services
            .submit_agent_action(&review("external-unread", references.clone()))
            .await
            .is_err());
        let activity = serde_json::from_value(serde_json::json!({
            "root_execution_id":"root-agentic-execution", "session_id":"session-dispatch", "turn_id":"turn-dispatch",
            "root_task_id":"task-root-dispatch", "task_id":"task-root-dispatch", "activity_id":"root-agentic-execution:model",
            "revision":1, "fence":1, "generation":1
        })).unwrap();
        let _scope = runtime.cowd_bus().unwrap().enter_execution_with_activity(
            crate::CowdExecutionContext {
                execution_id: "root-agentic-execution".into(),
                session_id: "session-dispatch".into(),
                turn_id: "turn-dispatch".into(),
            },
            Some(activity),
        );
        let read = runtime
            .execute_tool_batch_step(
                &[crate::conversation::ModelToolCall {
                    binding: None,
                    id: "read-current-external-decision".into(),
                    name: "evidence_retrieve".into(),
                    input: serde_json::json!({"evidence_ref":reference}).to_string(),
                    depends_on: vec![],
                }],
                &crate::SharedPrompter::none(),
                5,
            )
            .await
            .unwrap();
        assert_eq!(read.failed, 0, "{:?}", read.messages);
    }
    if local_effect {
        assert!(services
            .submit_agent_action(&review("receipt-only", references.clone()))
            .await
            .unwrap_err()
            .contains("review_requires_effect_observation"));
        std::fs::write(
            services.workspace_root().join("unrelated-review.txt"),
            "unrelated",
        )
        .unwrap();
        root_review_target_tool(
            &runtime,
            "root-unrelated-observation",
            "read_file",
            "unrelated-review.txt",
            "",
        )
        .await;
        assert!(services
            .submit_agent_action(&review("wrong-target", references.clone()))
            .await
            .unwrap_err()
            .contains("review_requires_effect_observation"));
        root_review_target_tool(
            &runtime,
            "root-current-local-target",
            "read_file",
            "review-effect.txt",
            "",
        )
        .await;
    }
    let reviewed = services
        .submit_agent_action(&review("root-review", references.clone()))
        .await
        .unwrap();
    assert_eq!(reviewed.status, AgentActionStatus::Applied, "{reviewed:?}");
    let goal = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    let proof = goal.reviews.last().unwrap().verification.as_ref().unwrap();
    assert!(!proof.independence_required);
    assert!(proof.review_policy_digest.is_some());
    for reference in &references {
        assert!(
            proof.reads.iter().any(|read| {
                (&read.result_ref == reference || &read.content_ref == reference)
                    && !read.receipt_refs.is_empty()
            }),
            "requested result must retain an authenticated read: {reference}"
        );
    }
    let authored = proof
        .reads
        .iter()
        .find(|read| read.content_ref == content.selector)
        .expect("the authored content remains covered through its artifact alias");
    assert_eq!(authored.sha256, content.sha256);
    assert_eq!(
        authored.bytes,
        b"The current result was read and checked.".len() as u64
    );
    if external_decision {
        assert_eq!(proof.result_source_revisions.len(), 1);
        assert!(proof.reads.iter().any(
            |read| read.result_kind == harness_contract::goal::GoalResultKind::ExternalDecision
        ));
        assert!(proof
            .producer_execution_ids
            .iter()
            .any(|id| id.starts_with("approval-decision:")));
    }
    if local_effect {
        assert!(
            proof.reads.iter().all(|read| read.result_kind
                == harness_contract::goal::GoalResultKind::ToolEffect
                && !read.effect_observation_refs.is_empty()),
            "Root explanation and content aliases retain the Root's actual effects"
        );
        let prior_digest = proof.effect_manifest_digest.clone();
        write_root_review_effect(
            &runtime,
            "root-local-write-again",
            "actually changed after review",
        )
        .await;
        let request = services
            .submit_agent_action(&bind(
                "stale-self-review-completion",
                AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                    blocked: false,
                    blocked_reason: None,
                    human_review_required: false,
                    recommended_next_actions: Vec::new(),
                    result_refs: references.clone(),
                    evidence_refs: vec![content.selector.clone()],
                    unresolved: vec![],
                }),
            ))
            .await
            .unwrap();
        assert_eq!(request.status, AgentActionStatus::Applied);
        let supervisor = crate::execution_core::goal::ObjectiveSupervisor::new(Arc::clone(
            services.goal_store(),
        ));
        crate::agentic::supervision::reconcile_completion_request(
            &services.agent_action_service(),
            &supervisor,
            "program-dispatch",
        )
        .unwrap();
        assert_eq!(
            services
                .goal_store()
                .get("goal:root-agentic-execution")
                .unwrap()
                .unwrap()
                .completion,
            GoalCompletion::Open
        );
        assert_eq!(
            services
                .agent_action_service()
                .project("program-dispatch")
                .unwrap()
                .status,
            crate::AgenticProgramStatus::Open
        );
        assert!(services
            .submit_agent_action(&review("stale-local-observation", references.clone()))
            .await
            .unwrap_err()
            .contains("review_requires_effect_observation"));
        root_review_target_tool(
            &runtime,
            "root-current-local-target-again",
            "read_file",
            "review-effect.txt",
            "",
        )
        .await;
        assert_eq!(
            services
                .submit_agent_action(&review("fresh-local-review", references.clone()))
                .await
                .unwrap()
                .status,
            AgentActionStatus::Applied
        );
        let current = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        assert_ne!(
            current
                .reviews
                .last()
                .unwrap()
                .verification
                .as_ref()
                .unwrap()
                .effect_manifest_digest,
            prior_digest
        );
    } else {
        assert!(proof.reads.iter().any(|read| matches!(
            read.result_kind,
            harness_contract::goal::GoalResultKind::StructuredData
                | harness_contract::goal::GoalResultKind::Content
        )));
    }
    let orphan = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/plain".into(),
                visibility_scope: "session:session-dispatch".into(),
                expected_bytes: None,
                original_name: None,
            },
            b"Unpublished orphan without a Tool or Program producer",
        )
        .await
        .unwrap();
    assert!(services
        .submit_agent_action(&review("unpublished-result", vec![orphan.selector]))
        .await
        .unwrap_err()
        .contains("Runtime-resolved producer"));
    let foreign = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/plain".into(),
                visibility_scope: "session:another-session".into(),
                expected_bytes: None,
                original_name: None,
            },
            b"Another session's private result",
        )
        .await
        .unwrap();
    assert!(services
        .submit_agent_action(&review("foreign-result", vec![foreign.selector]))
        .await
        .unwrap_err()
        .contains("not readable"));
    let required_policy_sources = std::iter::once("session:session-dispatch".to_string())
        .chain(
            references
                .iter()
                .filter(|reference| reference.starts_with("approval:v1:"))
                .map(|reference| format!("approval:{reference}")),
        )
        .collect::<Vec<_>>();
    let requested = services
        .submit_agent_action(&bind(
            "root-complete",
            AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                blocked: false,
                blocked_reason: None,
                human_review_required: false,
                recommended_next_actions: Vec::new(),
                result_refs: references,
                evidence_refs: vec![content.selector],
                unresolved: vec![],
            }),
        ))
        .await
        .unwrap();
    assert_eq!(
        requested.status,
        AgentActionStatus::Applied,
        "{requested:?}"
    );
    let before = services
        .agent_action_service()
        .project("program-dispatch")
        .unwrap();
    let prepared = services
        .goal_store()
        .prepare_program_conclusion(&before)
        .unwrap();
    assert!(prepared.gaps.is_empty(), "{:?}", prepared.gaps);
    for source in required_policy_sources {
        assert!(
            prepared
                .policy_sources
                .iter()
                .any(|revision| { revision.stream_id == source && revision.expected_revision > 0 }),
            "completion must fence the original policy source: {source}"
        );
    }
    // Negative concurrency injection only: no effect or positive read is invented.
    services
        .event_store()
        .append(crate::RuntimeEventInput {
            stream_id: "session:session-dispatch".into(),
            scope: crate::RuntimeEventScope::Session,
            kind: "test.concurrent_policy_source_change".into(),
            status: None,
            actor: Some("test.concurrent_writer".into()),
            refs: vec![],
            payload: serde_json::json!({}),
        })
        .unwrap();
    assert!(services
        .agent_action_service()
        .commit_program_conclusion(&before, prepared)
        .is_err());
    assert_eq!(
        services
            .agent_action_service()
            .project("program-dispatch")
            .unwrap()
            .revision,
        before.revision
    );
    assert!(services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap()
        .terminal
        .is_none());
    let supervisor =
        crate::execution_core::goal::ObjectiveSupervisor::new(Arc::clone(services.goal_store()));
    crate::agentic::supervision::reconcile_completion_request(
        &services.agent_action_service(),
        &supervisor,
        "program-dispatch",
    )
    .unwrap();
    let final_program = services
        .agent_action_service()
        .project("program-dispatch")
        .unwrap();
    assert!(
        final_program.tasks.is_empty()
            && final_program.teams.is_empty()
            && final_program.agents.is_empty()
    );
    assert_eq!(final_program.status, crate::AgenticProgramStatus::Verified);
    if local_effect {
        let target = services.workspace_root().join("review-effect.txt");
        let before = std::fs::read(&target).unwrap();
        let _scope = runtime.cowd_bus().unwrap().enter_execution_with_activity(
            crate::CowdExecutionContext {
                execution_id: "root-agentic-execution".into(),
                session_id: "session-dispatch".into(),
                turn_id: "turn-dispatch".into(),
            },
            None,
        );
        let late = runtime.execute_tool_batch_step(&[crate::conversation::ModelToolCall {
            binding: None,
            id: "late-root-write-after-verification".into(),
            name: "edit".into(),
            input: serde_json::json!({"path":"review-effect.txt","content":"forbidden late mutation"}).to_string(),
            depends_on: vec![],
        }], &crate::SharedPrompter::none(), 99).await.unwrap();
        assert_eq!(late.failed, 1, "{:?}", late.messages);
        assert_eq!(std::fs::read(target).unwrap(), before);
        assert_eq!(
            services
                .agent_action_service()
                .project("program-dispatch")
                .unwrap()
                .status,
            crate::AgenticProgramStatus::Verified
        );
    }
    assert_eq!(
        services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap()
            .completion,
        GoalCompletion::Satisfied
    );
    // Negative corruption fixtures only; the successful path above used the
    // actual ConversationRuntime strategy writer without injected evidence.
    let goal = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    let mut independent_goal = goal.clone();
    independent_goal
        .obligations
        .push(harness_contract::goal::ObjectiveObligation {
            obligation_id: "explicit-independent-verification".into(),
            required: true,
            success_predicate: "The original requirement needs another verifier".into(),
            producer: Default::default(),
            evidence_requirement: harness_contract::goal::ObjectiveEvidenceRequirement {
                required_artifact_kinds: vec![],
                independent_verifier_required: true,
                reread_required: true,
            },
            state: harness_contract::goal::ObjectiveObligationState::Open,
            artifact_refs: vec![],
            evidence_refs: vec![],
            reread_receipts: vec![],
            verifier_decision: None,
            diagnostic_code: None,
        });
    assert!(crate::agentic::review_evidence::root_self_review_policy(
        services.event_store(),
        &final_program,
        &independent_goal
    )
    .unwrap()
    .is_none());
    let strategy = services
        .event_store()
        .list_stream("session:session-dispatch")
        .unwrap()
        .into_iter()
        .rev()
        .find(|event| {
            event.kind.starts_with("runtime.strategy.")
                && event.payload["execution_graph_ref"] == "root-agentic-execution"
        })
        .unwrap();
    for (label, field, value) in [
        (
            "higher-risk",
            "risk",
            serde_json::to_value(harness_contract::core::TaskRisk::High).unwrap(),
        ),
        ("legacy-missing-risk", "risk", serde_json::Value::Null),
        (
            "verifier-required",
            "modifiers",
            serde_json::to_value(vec![
                harness_contract::core::ExecutionModifier::WithVerifier,
            ])
            .unwrap(),
        ),
        (
            "reviewer-required",
            "modifiers",
            serde_json::to_value(vec![
                harness_contract::core::ExecutionModifier::WithReviewer,
            ])
            .unwrap(),
        ),
        (
            "approval-required",
            "gates",
            serde_json::to_value(vec![harness_contract::core::ExecutionPolicyGate::Approval])
                .unwrap(),
        ),
        ("legacy-missing-gates", "gates", serde_json::Value::Null),
    ] {
        let mut payload = strategy.payload.clone();
        payload[field] = value;
        payload["test_fault"] = label.into();
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                stream_id: strategy.stream_id.clone(),
                scope: strategy.scope,
                kind: "runtime.strategy.test_fault_injection".into(),
                status: strategy.status.clone(),
                actor: strategy.actor.clone(),
                refs: strategy.refs.clone(),
                payload,
            })
            .unwrap();
        assert!(
            crate::agentic::review_evidence::root_self_review_policy(
                services.event_store(),
                &final_program,
                &goal
            )
            .unwrap()
            .is_none(),
            "{label}"
        );
    }
}

// ---- W2: one-shot Task deliverable ---------------------------------------------

async fn one_shot_fixture() -> (
    std::sync::Arc<RuntimeServices>,
    String,
    String,
    String,
    String,
) {
    let services = RuntimeServices::in_memory().expect("runtime");
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    let team = actions
        .apply(&root(
            "one-shot-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Solo".to_string(),
                mission: "produce".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let author = actions
        .apply(&root(
            "one-shot-author",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Author".to_string(),
                mission: "produce evidence".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("author")
        .changed_refs[0]
        .clone();
    let task = actions
        .apply(&root(
            "one-shot-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "One-shot result".to_string(),
                objective: "produce durable evidence".to_string(),
                acceptance: "independent review accepts the delivered body".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("task")
        .changed_refs[0]
        .clone();
    let execution = "one-shot-execution".to_string();
    actions
        .apply(&agent(
            "one-shot-claim",
            &team,
            &author,
            &execution,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task.clone(),
                reason: None,
            }),
        ))
        .expect("claim");
    (services, team, author, task, execution)
}

#[tokio::test]
async fn tool_contract_milestone_persists_once_and_reports_conflicting_writes() {
    let (services, _, author, task_ref, _) = one_shot_fixture().await;
    let projection = services
        .agent_action_service()
        .project("program-dispatch")
        .expect("program projection");
    let task = &projection.tasks[&task_ref];
    let member = &projection.agents[&author];
    let mut admission = super::admission::resolve_agentic_execution_admission(
        &services,
        member,
        task,
        &AgenticDispatchContext {
            session_id: "session-dispatch".to_string(),
            turn_id: "turn-dispatch".to_string(),
            model_lease: "test".to_string(),
            permission_ceiling: PermissionMode::ReadOnly,
            resource_scopes: Vec::new(),
        },
        DispatchMode::Execute,
    )
    .expect("bounded worker remains admissible");
    admission.missing_capabilities = vec!["network".to_string()];
    admission.missing_tool_alternatives = vec!["web_search".to_string()];
    for _ in 0..2 {
        super::dispatch::record_tool_contract_milestone(
            &services,
            &projection,
            task,
            member,
            DispatchMode::Execute,
            &admission,
        )
        .expect("persist the same contract idempotently");
    }
    let milestones = services
        .event_store()
        .all_events(256)
        .expect("durable events")
        .into_iter()
        .filter(|event| event.kind == "agentic.tool_contract")
        .collect::<Vec<_>>();
    assert_eq!(milestones.len(), 1);
    assert_eq!(milestones[0].payload["program_id"], "program-dispatch");
    assert_eq!(milestones[0].payload["task_ref"], task_ref);
    assert_eq!(milestones[0].payload["missing_capabilities"][0], "network");

    admission.missing_capabilities.push("write".to_string());
    let error = super::dispatch::record_tool_contract_milestone(
        &services,
        &projection,
        task,
        member,
        DispatchMode::Execute,
        &admission,
    )
    .expect_err("a rejected append must be visible to the dispatch caller");
    assert!(error.contains("conflicts"), "{error}");
    assert!(!admission
        .effective_capabilities
        .contains(&AgentCapability::Network));
    assert!(!admission
        .allowed_tools
        .iter()
        .any(|tool| tool == "web_search"));
}

#[tokio::test]
async fn one_shot_task_deliverable_expands_to_the_canonical_artifact_path() {
    let (services, team, author, task, execution) = one_shot_fixture().await;
    let actions = services.agent_action_service();
    let content = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/markdown".to_string(),
                visibility_scope: "session:session-dispatch".to_string(),
                expected_bytes: None,
                original_name: Some("one-shot.md".to_string()),
            },
            b"one-shot deliverable body",
        )
        .await
        .expect("content");
    let submit = agent(
        "one-shot-submit",
        &team,
        &author,
        &execution,
        AgentAction::TaskSubmit(TaskSubmitInput {
            task_ref: task.clone(),
            artifact_refs: Vec::new(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            deliverable: Some(TaskOneShotDeliverable {
                block_index: 0,
                kind: "report".to_string(),
                title: "One-shot report".to_string(),
                resolved_content_ref: Some(content.selector.clone()),
            }),
        }),
    );
    let observation = services
        .submit_agent_action(&submit)
        .await
        .expect("one-shot submit");
    assert_eq!(
        observation.status,
        AgentActionStatus::Applied,
        "{:?}",
        observation.error
    );
    let program = actions.project("program-dispatch").expect("program");
    let projection = program.tasks.get(&task).expect("task projection");
    assert_eq!(projection.status, crate::AgenticTaskStatus::Submitted);
    assert_eq!(
        projection.artifact_refs.len(),
        1,
        "one-shot deliverable binds exactly one artifact"
    );
    let artifact_ref = &projection.artifact_refs[0];
    assert!(artifact_ref.starts_with("artifact:"));
    assert_eq!(
        program
            .artifacts
            .get(artifact_ref)
            .expect("registered artifact")
            .content_ref,
        content.selector
    );
}

#[tokio::test]
async fn one_shot_deliverable_is_ignored_when_artifact_refs_are_present() {
    let (services, team, author, task, execution) = one_shot_fixture().await;
    let actions = services.agent_action_service();
    let explicit = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/markdown".to_string(),
                visibility_scope: "session:session-dispatch".to_string(),
                expected_bytes: None,
                original_name: Some("explicit.md".to_string()),
            },
            b"explicit body",
        )
        .await
        .expect("content");
    let committed = actions
        .apply(&agent(
            "one-shot-explicit-commit",
            &team,
            &author,
            &execution,
            AgentAction::ArtifactCommit(ArtifactCommitInput {
                content_ref: explicit.selector.clone(),
                kind: "report".to_string(),
                title: "Explicit report".to_string(),
                relates_to: Vec::new(),
            }),
        ))
        .expect("commit")
        .changed_refs[0]
        .clone();
    let submit = agent(
        "one-shot-submit-with-refs",
        &team,
        &author,
        &execution,
        AgentAction::TaskSubmit(TaskSubmitInput {
            task_ref: task.clone(),
            artifact_refs: vec![committed.clone()],
            evidence_refs: vec![explicit.selector.clone()],
            unresolved: Vec::new(),
            deliverable: Some(TaskOneShotDeliverable {
                block_index: 0,
                kind: "ignored".to_string(),
                title: "Ignored".to_string(),
                resolved_content_ref: Some("artifact://not-used".to_string()),
            }),
        }),
    );
    services
        .submit_agent_action(&submit)
        .await
        .expect("submit with refs");
    let program = actions.project("program-dispatch").expect("program");
    let projection = program.tasks.get(&task).expect("task projection");
    assert_eq!(
        projection.artifact_refs,
        vec![committed],
        "artifact_refs stays authoritative"
    );
}

#[tokio::test]
async fn one_shot_deliverable_without_a_resolved_ref_is_rejected() {
    let (services, team, author, task, execution) = one_shot_fixture().await;
    let submit = agent(
        "one-shot-unresolved",
        &team,
        &author,
        &execution,
        AgentAction::TaskSubmit(TaskSubmitInput {
            task_ref: task.clone(),
            artifact_refs: Vec::new(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            deliverable: Some(TaskOneShotDeliverable {
                block_index: 0,
                kind: "report".to_string(),
                title: "Unresolved".to_string(),
                resolved_content_ref: None,
            }),
        }),
    );
    let observation = services
        .submit_agent_action(&submit)
        .await
        .expect("submit returns an observation");
    assert_eq!(
        observation.status,
        AgentActionStatus::Rejected,
        "an unresolved one-shot deliverable must be rejected by authorization"
    );
    assert!(
        observation
            .error
            .as_ref()
            .is_some_and(|error| error.code == "task_submission_has_no_artifact"),
        "unexpected rejection: {:?}",
        observation.error
    );
    let guidance = &observation.error.as_ref().expect("rejection error").message;
    assert!(guidance.contains("task_submit.deliverable"));
    assert!(guidance.contains("same Task, Agent, and attempt"));
    assert!(guidance.contains("file or multipart deliverables"));
}

#[tokio::test]
async fn r1_proof_program_registration_preserves_tool_publisher_and_committer() {
    for logical in [true, false] {
        for encoding in [None] {
            exercise_program_publication_case(ProgramPublicationCase {
                logical,
                encoding,
                probe: ProgramPublicationProbe::Metadata,
            })
            .await;
        }
    }
}

#[tokio::test]
async fn r1_proof_program_registered_legacy_publication_never_becomes_strong() {
    for logical in [true, false] {
        for encoding in [Some("legacy")] {
            exercise_program_publication_case(ProgramPublicationCase {
                logical,
                encoding,
                probe: ProgramPublicationProbe::Independent,
            })
            .await;
        }
    }
}

#[tokio::test]
async fn r1_proof_program_registered_bad_raw_anchor_never_falls_back() {
    for logical in [true, false] {
        for encoding in [
            Some("version"),
            Some("tool_call_id"),
            Some("ref_id"),
            Some("visibility_scope"),
            Some("sha256"),
            Some("byte_count"),
            Some("selector"),
            Some("media_type"),
        ] {
            exercise_program_publication_case(ProgramPublicationCase {
                logical,
                encoding,
                probe: ProgramPublicationProbe::Independent,
            })
            .await;
        }
    }
}

#[tokio::test]
async fn r1_proof_program_registered_original_publisher_cannot_review() {
    for logical in [true, false] {
        for encoding in [None] {
            exercise_program_publication_case(ProgramPublicationCase {
                logical,
                encoding,
                probe: ProgramPublicationProbe::Publisher,
            })
            .await;
        }
    }
}

#[tokio::test]
async fn r1_proof_program_registered_independent_read_review_survives_cold_b() {
    for logical in [true, false] {
        for encoding in [None] {
            exercise_program_publication_case(ProgramPublicationCase {
                logical,
                encoding,
                probe: ProgramPublicationProbe::Independent,
            })
            .await;
        }
    }
}

#[tokio::test]
async fn r1_proof_program_registered_recursive_primary_keeps_publisher_influence() {
    for logical in [true, false] {
        for encoding in [None, Some("legacy"), Some("version")] {
            exercise_program_publication_case(ProgramPublicationCase {
                logical,
                encoding,
                probe: ProgramPublicationProbe::Recursive,
            })
            .await;
        }
    }
}

#[tokio::test]
async fn r1_proof_program_registered_anchor_loss_after_a_rejected_by_actual_b() {
    for logical in [true, false] {
        for encoding in [None] {
            exercise_program_publication_case(ProgramPublicationCase {
                logical,
                encoding,
                probe: ProgramPublicationProbe::AnchorLoss,
            })
            .await;
        }
    }
}

#[tokio::test]
async fn r1_read_delivery_process_authorized_review_preserves_strong_grade() {
    exercise_real_process_grade_contract().await;
    exercise_r1_proof_scenario(
        R1ProofScenario::ReadDelivery(RealProcessProofCase::Authorized),
        DelegatedEffectCase::Review,
    )
    .await;
}

#[tokio::test]
async fn r1_read_delivery_process_failed_or_partial_send_cannot_promote() {
    exercise_r1_proof_scenario(
        R1ProofScenario::ReadDelivery(RealProcessProofCase::Preview),
        DelegatedEffectCase::Review,
    )
    .await;
    exercise_r1_proof_scenario(
        R1ProofScenario::ReadDelivery(RealProcessProofCase::FailedSend),
        DelegatedEffectCase::Review,
    )
    .await;
}

#[tokio::test]
async fn r1_read_delivery_mechanical_oracle_is_not_model_observation() {
    exercise_real_process_grade_contract().await;
    exercise_r1_proof_scenario(
        R1ProofScenario::ReadDelivery(RealProcessProofCase::Mechanical),
        DelegatedEffectCase::Review,
    )
    .await;
}

#[tokio::test]
async fn r1_read_delivery_pages_require_same_version_attempt_and_complete_extent() {
    exercise_r1_proof_scenario(
        R1ProofScenario::ReadDelivery(RealProcessProofCase::Pages),
        DelegatedEffectCase::Review,
    )
    .await;
}

#[tokio::test]
async fn r1_read_delivery_late_delivery_does_not_refresh_old_acquisition() {
    exercise_r1_proof_scenario(
        R1ProofScenario::ReadDelivery(RealProcessProofCase::LateDelivery),
        DelegatedEffectCase::Review,
    )
    .await;
}

#[tokio::test]
async fn r1_proof_native_raw_anchor_source_change_rejected_by_actual_b() {
    exercise_native_sources_with_changed_anchor_source().await;
}

#[tokio::test]
async fn r1_proof_native_raw_content_anchor_survives_missing_session_mapping() {
    exercise_native_sources_with_cold_readers().await;
}

#[tokio::test]
async fn r1_read_delivery_proof_and_goal_cas_bind_actual_delivery_sources() {
    exercise_r1_proof_scenario(
        R1ProofScenario::ReadDelivery(RealProcessProofCase::Sources),
        DelegatedEffectCase::Review,
    )
    .await;
    exercise_native_sources_with_cold_readers().await;
}

#[tokio::test]
async fn r1_read_delivery_legacy_missing_seal_never_reconstructs_current_files() {
    exercise_r1_proof_scenario(
        R1ProofScenario::ReadDelivery(RealProcessProofCase::Authorized),
        DelegatedEffectCase::Review,
    )
    .await;
    exercise_r1_proof_scenario(
        R1ProofScenario::ReadDelivery(RealProcessProofCase::Legacy),
        DelegatedEffectCase::Review,
    )
    .await;
}

#[tokio::test]
async fn r1_optional_unknown_command_ack_does_not_settle_physical_attempt() {
    exercise_r1_proof_scenario(
        R1ProofScenario::ReadDelivery(RealProcessProofCase::CommandAck),
        DelegatedEffectCase::Review,
    )
    .await;
}

#[tokio::test]
async fn r1_optional_unknown_restarted_run_requires_exact_exit_evidence() {
    exercise_r1_proof_scenario(
        R1ProofScenario::ReadDelivery(RealProcessProofCase::Restart),
        DelegatedEffectCase::Review,
    )
    .await;
}

#[tokio::test]
async fn r1_optional_unknown_process_reap_and_effect_settlement_are_independent() {
    exercise_r1_proof_scenario(
        R1ProofScenario::ReadDelivery(RealProcessProofCase::ReapAndEffect),
        DelegatedEffectCase::Review,
    )
    .await;
}

#[tokio::test]
async fn r1_proof_effect_observations_reject_wrong_target_or_old_acquisition() {
    exercise_native_sources_with_cold_readers().await;
    exercise_r1_proof_scenario(
        R1ProofScenario::ReadDelivery(RealProcessProofCase::Effects),
        DelegatedEffectCase::Review,
    )
    .await;
    exercise_r1_proof_scenario(
        R1ProofScenario::ReadDelivery(RealProcessProofCase::LateDelivery),
        DelegatedEffectCase::Review,
    )
    .await;
}
