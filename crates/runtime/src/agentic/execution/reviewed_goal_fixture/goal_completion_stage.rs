{
    let completion_envelope = bind_root(root(
        "experience-complete",
        AgentAction::ObjectiveCompleteRequest(
            harness_contract::agent_action::ObjectiveCompleteRequestInput {
                blocked: false,
                blocked_reason: None,
                human_review_required: false,
                recommended_next_actions: Vec::new(),
                result_refs: reviewed_delivery_refs.clone(),
                evidence_refs: vec![content.selector.clone()],
                unresolved: vec![],
            },
        ),
    ));
    let completion = services
        .submit_agent_action(&completion_envelope)
        .await
        .unwrap();
    if completion.status == harness_contract::agent_action::AgentActionStatus::Rejected {
        eprintln!(
            "R1_COMPLETION_REFUSAL_WITNESS original_error={:?}; diagnostics={}",
            completion.error,
            r1_goal_proof_diagnostic(&services, fixture_program_id.as_str()),
        );
    }
    if proof_scenario.invalid_child() {
        assert_eq!(
            completion.status,
            harness_contract::agent_action::AgentActionStatus::Rejected,
            "{completion:?}"
        );
        assert_eq!(
            completion.error.as_ref().unwrap().code,
            "objective_not_verifiable"
        );
        let optional = optional_effect.take().unwrap();
        optional.assert_pending(&services);
        optional
            .finish_rejected_child(&services, proof_scenario)
            .await;
        assert!(services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap()
            .terminal
            .is_none());
        assert!(action_service
            .project(fixture_program_id.as_str())
            .unwrap()
            .objective_verdict
            .is_none());
        println!(
            "R1_BOUND_CHILD_REJECTED {}",
            serde_json::json!({"reason":completion.error,"half_a":false,"held_effect_released_only_after_refusal":true})
        );
        services
            .cancel_execution_tree(&review_graph.id, "invalid child fixture complete")
            .await
            .unwrap();
        services
            .cancel_execution_tree(packet.graph_id(), "invalid child fixture complete")
            .await
            .unwrap();
        return None;
    }
    if proof_scenario == R1ProofScenario::OptionalNotRetiring {
        assert_eq!(
            completion.status,
            harness_contract::agent_action::AgentActionStatus::Rejected,
            "{completion:?}"
        );
        let error = completion.error.as_ref().unwrap();
        assert_eq!(error.code, "objective_not_verifiable");
        assert!(
            error.message.contains("execution_cleanup_pending"),
            "{error:?}"
        );
        let optional = optional_effect.unwrap();
        let before = action_service.project(fixture_program_id.as_str()).unwrap();
        assert_eq!(
            before.tasks[&optional.task_ref].status,
            crate::AgenticTaskStatus::Claimed
        );
        assert!(before.tasks[&optional.task_ref]
            .pending_retirement
            .is_none());
        assert!(before.objective_verdict.is_none());
        optional.assert_pending(&services);
        let goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        assert_eq!(goal.completion, GoalCompletion::Open);
        assert!(goal.terminal.is_none());
        let retirement = bind_root(root(
            "r1-negative-explicit-retirement-after-refusal",
            AgentAction::TaskWithdraw(harness_contract::agent_action::TaskWithdrawInput {
                task_ref: optional.task_ref.clone(),
                reason_ref: content.selector.clone(),
                evidence_refs: vec![content.selector.clone()],
            }),
        ));
        let retired = services.submit_agent_action(&retirement).await.unwrap();
        assert_eq!(
            retired.status,
            harness_contract::agent_action::AgentActionStatus::Applied,
            "{retired:?}"
        );
        optional.assert_pending(&services);
        optional
            .finish_after_retirement(&services, Some(retirement))
            .await;
        services
            .cancel_execution_tree(&review_graph.id, "R1 nonretiring negative complete")
            .await
            .unwrap();
        services
            .cancel_execution_tree(packet.graph_id(), "R1 nonretiring negative complete")
            .await
            .unwrap();
        assert_eq!(services.goal_store().get(&goal.id).unwrap().unwrap(), goal);
        println!(
            "R1_OPTIONAL_NOT_RETIRING_REJECTED {}",
            serde_json::json!({"goal":goal.id,"reason":error.message})
        );
        return None;
    }
    assert_eq!(
        completion.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{completion:?}"
    );
    #[cfg(test)]
    if matches!(
        proof_scenario,
        R1ProofScenario::Mechanical
            | R1ProofScenario::MechanicalApproval
            | R1ProofScenario::MechanicalExtra
            | R1ProofScenario::MechanicalCrossTeam
    ) {
        let current = action_service.project(fixture_program_id.as_str()).unwrap();
        let actual = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        assert_eq!(actual.completion, GoalCompletion::Open);
        assert!(actual.terminal.is_none());
        let prepared = services
            .goal_store()
            .prepare_program_conclusion(&current)
            .unwrap();
        assert!(prepared.gaps.is_empty(), "{:?}", prepared.gaps);
        assert_eq!(prepared.goal.completion, GoalCompletion::Satisfied);
        for condition in
            crate::agentic::review_evidence::required_review_conditions(&prepared.goal)
        {
            let review = prepared
                .goal
                .reviews
                .iter()
                .rev()
                .find(|review| review.criterion_ref == condition)
                .unwrap();
            assert_eq!(
                review
                    .verification
                    .as_ref()
                    .unwrap()
                    .input_set
                    .as_ref()
                    .unwrap()
                    .version,
                2
            );
        }
        assert!(prepared
            .source_frame
            .sources()
            .any(|source| source.stream_id
                == format!("agentic-program:{}", current.program_id)
                && source.expected_revision == current.revision));
        NATIVE_A_PROBE.with(|slot| {
            slot.borrow_mut().take().unwrap()(
                &services.0,
                &current,
                prepared,
                &completion_envelope,
            )
        });
        services.shutdown_maintenance().await;
        return None;
    }
    let objective_supervisor = crate::execution_core::goal::ObjectiveSupervisor::new(
        Arc::clone(services.goal_store()),
    );
    if proof_scenario == R1ProofScenario::ReadDelivery(RealProcessProofCase::Effects) {
        assert_real_process_effect_proof_conclusion_guards(
            &services,
            fixture_program_id.as_str(),
        );
    }
    if proof_scenario == R1ProofScenario::OptionalSourceRace {
        let before = action_service.project(fixture_program_id.as_str()).unwrap();
        let prepared = services
            .goal_store()
            .prepare_program_conclusion(&before)
            .unwrap();
        assert_eq!(
            prepared.goal.completion,
            GoalCompletion::Satisfied,
            "{:?}",
            prepared.goal
        );
        let optional = optional_effect.take().unwrap();
        optional.assert_pending(&services);
        let source = optional.index.clone();
        let before_revision = services.event_store().stream_revision(&source).unwrap();
        assert!(
            prepared.source_frame.sources().any(|expected| {
                expected.stream_id == source && expected.expected_revision == before_revision
            }),
            "prepared A must carry the actual optional source fence"
        );
        let receipt_services = services.0.clone();
        let receipt_program_id = fixture_program_id.clone();
        optional.finish_after_receipt(&services, None, None, move || {
        let services = ReviewedFixtureServices(receipt_services);
        let action_service = services.agent_action_service();
        let fixture_program_id = receipt_program_id;
        let after_revision = services.event_store().stream_revision(&source).unwrap();
        assert!(after_revision > before_revision);
        let before_attempt = action_service.project(fixture_program_id.as_str()).unwrap();
        let goal_before = services.goal_store().get("goal:root-agentic-execution")
            .unwrap().unwrap();
        assert!(before_attempt.objective_verdict.is_none(), "{before_attempt:?}");
        assert_eq!(goal_before.completion, GoalCompletion::Open);
        assert!(goal_before.terminal.is_none());
        let error = action_service.commit_program_conclusion(&before, prepared)
            .err().expect("the original A transaction must reject a changed actual source cut");
        match &error {
            crate::AgentActionServiceError::Store(crate::RuntimeEventStoreError::StaleRevision {
                stream_id, expected, actual,
            }) => {
                assert_eq!(stream_id, &source);
                assert_eq!(*expected, before_revision);
                assert_eq!(*actual, after_revision);
            }
            other => panic!("original A must reject its actual optional source fence: {other}"),
        }
        let after = action_service.project(fixture_program_id.as_str()).unwrap();
        assert!(after.objective_verdict.is_none(), "{after:?}");
        assert_eq!(after.revision, before_attempt.revision);
        let goal = services.goal_store().get("goal:root-agentic-execution")
            .unwrap().unwrap();
        assert_eq!(goal, goal_before);
        println!("R1_PREPARED_A_SOURCE_RACE {}", serde_json::json!({
            "source":source,"before_revision":before_revision,"after_revision":after_revision,
            "error":error.to_string(),"goal":goal.id,"half_terminal":false,
            "race_scope":"actual physical receipt before Task cleanup and completion reconciliation",
        }));
    }).await;
    }
    let bounded_committed = if proof_scenario.bounded_process() {
        use crate::execution_core::graph::{ScopeLockMode, ScopeLockRequest, ScopedResource};
        let before_goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        let before_program = action_service.project(fixture_program_id.as_str()).unwrap();
        assert_eq!(before_goal.completion, GoalCompletion::Open);
        assert!(before_program.objective_verdict.is_none());
        let prepared = services
        .goal_store()
        .prepare_program_conclusion(&before_program)
        .unwrap_or_else(|error| panic!(
            "original bounded Goal prepare rejected: {error}; program={} projection_cutoff={}; diagnostics={}",
            before_program.program_id,
            before_program.revision,
            r1_goal_proof_diagnostic(&services, fixture_program_id.as_str()),
        ));
        assert_eq!(
        prepared.goal.completion,
        GoalCompletion::Satisfied,
        "original prepared Goal gaps: {:?}; program={} projection_cutoff={}; diagnostics={}",
        prepared.gaps,
        before_program.program_id,
        before_program.revision,
        r1_goal_proof_diagnostic(&services, fixture_program_id.as_str()),
    );
        let writer_request = ScopeLockRequest {
            scope: ScopedResource::workspace_object(
                services
                    .path_identity_resolver()
                    .resolve_existing("bounded_source.py")
                    .unwrap(),
            ),
            mode: ScopeLockMode::Write,
        };
        let original =
            std::fs::read(services.workspace_root().join("bounded_source.py")).unwrap();
        let writer = services.scope_locks().try_acquire([writer_request.clone()]).unwrap()
        .expect("collector/prepare must release only its local frame before a later final operation");
        if proof_scenario == R1ProofScenario::BoundedProcessInputChanged {
            std::fs::write(
                services.workspace_root().join("bounded_source.py"),
                b"VALUE = 290\n",
            )
            .unwrap();
            drop(writer);
            let refused = action_service
                .commit_program_conclusion(&before_program, prepared)
                .unwrap_err()
                .to_string();
            assert!(
                refused.contains("process")
                    && (refused.contains("input") || refused.contains("witness")),
                "{refused}"
            );
            assert_eq!(
                services.goal_store().get(&before_goal.id).unwrap().unwrap(),
                before_goal
            );
            assert_eq!(
                serde_json::to_value(
                    action_service.project(fixture_program_id.as_str()).unwrap()
                )
                .unwrap(),
                serde_json::to_value(&before_program).unwrap()
            );
            drain_workers().await;
            return None;
        }
        let started = std::time::Instant::now();
        let busy = action_service
            .commit_program_conclusion(&before_program, prepared)
            .unwrap_err()
            .to_string();
        assert!(busy.contains("busy") || busy.contains("retry"), "{busy}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "sync busy must not await its own writer"
        );
        assert_eq!(
            services.goal_store().get(&before_goal.id).unwrap().unwrap(),
            before_goal
        );
        assert_eq!(
            serde_json::to_value(action_service.project(fixture_program_id.as_str()).unwrap())
                .unwrap(),
            serde_json::to_value(&before_program).unwrap()
        );
        assert_eq!(
            std::fs::read(services.workspace_root().join("bounded_source.py")).unwrap(),
            original
        );
        drop(writer);
        #[cfg(test)]
        {
            let prepared = services
                .goal_store()
                .prepare_program_conclusion(&before_program)
                .unwrap();
            let checked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let captured = checked.clone();
            let locks = Arc::clone(services.scope_locks());
            let source = services.workspace_root().join("bounded_source.py");
            with_native_conclusion_append_hook(move |transaction| {
            assert!(!transaction.expected_streams.is_empty());
            assert!(locks.try_acquire([writer_request]).unwrap().is_none(),
                "the actual final input frame must remain held at the original A transaction boundary");
            assert_eq!(std::fs::read(source).unwrap(), original);
            captured.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }, || action_service.commit_program_conclusion(&before_program, prepared)).unwrap();
            assert_eq!(checked.load(std::sync::atomic::Ordering::SeqCst), 1);
            let after = services
                .scope_locks()
                .try_acquire([ScopeLockRequest {
                    scope: ScopedResource::workspace_object(
                        services
                            .path_identity_resolver()
                            .resolve_existing("bounded_source.py")
                            .unwrap(),
                    ),
                    mode: ScopeLockMode::Write,
                }])
                .unwrap()
                .expect("final operation must release its own frame only after CAS returns");
            drop(after);
            true
        }
        #[cfg(not(test))]
        {
            false
        }
    } else {
        false
    };
    if !bounded_committed {
        let reconciled = crate::agentic::supervision::reconcile_completion_request(
            &action_service,
            &objective_supervisor,
            fixture_program_id.as_str(),
        )
        .expect("verified Objective");
        let goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        if goal.completion != GoalCompletion::Satisfied {
            eprintln!(
                "R1_FINAL_CONCLUSION_NOT_SATISFIED reconciled_status={:?}; diagnostics={}",
                reconciled.as_ref().map(|program| program.status),
                r1_goal_proof_diagnostic(&services, fixture_program_id.as_str()),
            );
        }
    }
    if let Some(optional) = optional_effect {
        let goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        let program = action_service.project(fixture_program_id.as_str()).unwrap();
        assert_eq!(
            goal.completion,
            GoalCompletion::Satisfied,
            "{program:?} {goal:?}"
        );
        assert!(program.objective_verdict.is_some());
        assert_eq!(
            program.tasks[&optional.task_ref].status,
            crate::AgenticTaskStatus::CancelRequested
        );
        assert!(program.tasks[&optional.task_ref]
            .pending_retirement
            .is_some());
        assert_ne!(
            program.tasks[&optional.task_ref].status,
            crate::AgenticTaskStatus::Accepted
        );
        optional.assert_pending(&services);
        println!(
            "R1_OPTIONAL_UNKNOWN_SATISFIED {}",
            serde_json::json!({
                "goal":goal.id,"verdict":program.objective_verdict,"optional_task":optional.task_ref,
                "execution":"unknown_draining","effect_index":optional.index,
            })
        );
        if probes_final_b {
            assert_r1_optional_final_b(
                &services,
                &effect_host,
                &optional,
                &content.selector,
                proof_scenario,
            )
            .await;
        }
        if proof_scenario == R1ProofScenario::PhysicalAfterA {
            optional.worker.await.unwrap().unwrap();
            wait_retired_process(&services, &optional.packet, &optional.task_ref).await;
        } else {
            optional.finish(&services).await;
        }
        drain_workers().await;
        assert_eq!(services.goal_store().get(&goal.id).unwrap().unwrap(), goal);
        return None;
    }
    Some(())
}
