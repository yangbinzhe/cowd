use harness_contract::agent_action::{
    AgentAction, AgentActionEnvelope, AgentActorBinding, AgentActorKind, AgentInviteInput,
    ArtifactCommitInput, MessagePublishInput, ObjectiveCompleteRequestInput, StateInspectInput,
    TaskAttemptFailInput, TaskClaimInput, TaskPublishInput, TaskReviewDecision, TaskReviewInput,
    TaskSubmitInput, TaskSupersedeInput, TeamCreateInput,
};
use harness_contract::goal::{AcceptanceCriterion, AcceptanceStatus, GoalCompletion, GoalContract};

use super::*;

/// T: a call that was admitted and then refused while it ran leaves one
/// durable fact, written under the call's own effect key.
///
/// The consumer reads the refusal by the admitted call's own key, so the
/// program event and its effect-stream twin are one transaction under that
/// key. A call that carries no admission intent gets no fact at all.
#[test]
fn t_refused_admitted_call_writes_one_consumable_rejection_fact() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    // The program this rejection is written under is the one the projection
    // names; an empty program stream is revision 0.
    let mut projection = AgenticProgramProjection::empty("t-program", "t-objective");
    projection.session_id = "t-session".into();
    let key = "t-refusal-effect";
    let call = RefusedCall {
        effect_key: key,
        tool_use_id: "t-refusal-call",
        tool_name: "read_ref",
        reason: "the delegated reader is an Agent action and this call carries no original action capability",
    };
    // A call without its own admission intent gets no fact.
    assert_eq!(
        service
            .record_refused_admitted_call(&projection, &call)
            .expect("refusal outcome"),
        RefusalOutcome::NotAdmitted
    );
    assert_eq!(
        store
            .stream_revision(&format!("execution-effect:{key}"))
            .expect("effect stream"),
        0
    );
    assert!(
        store
            .event_by_idempotency_key(
                &format!("agentic-program:{}", projection.program_id),
                &format!("completion-rejected:{key}"),
            )
            .expect("program event lookup")
            .is_none(),
        "an unadmitted call leaves no rejection fact behind"
    );
    // The admission intent the refusal is read against, written the way the
    // Read family admits it: one transaction event under its own key.
    store
        .append_transaction(crate::AppendTransactionRequest {
            transaction_id: format!("{key}:read-intent"),
            expected_streams: vec![crate::ExpectedStreamRevision {
                stream_id: format!("execution-effect:{key}"),
                expected_revision: 0,
            }],
            events: vec![crate::RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: format!("execution-effect:{key}"),
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "execution.read.intent".into(),
                    status: Some("inflight".into()),
                    actor: Some("governed_tool".into()),
                    // The admission names the call it admitted, the way the
                    // Read family admits it.
                    refs: vec![crate::RuntimeEventRef {
                        kind: "tool_invocation".into(),
                        id: call.tool_use_id.into(),
                    }],
                    payload: json!({
                        "idempotency_key": key,
                        "tool_use_id": call.tool_use_id,
                        "tool_name": call.tool_name,
                    }),
                },
                idempotency_key: Some(format!("{key}:read-intent")),
                schema_version: 1,
            }],
        })
        .expect("admission intent");
    assert!(
        store
            .event_by_idempotency_key(
                &format!("execution-effect:{key}"),
                &format!("{key}:read-intent"),
            )
            .expect("intent lookup")
            .is_some(),
        "the admission intent is on the call's own effect stream"
    );
    assert_eq!(
        service
            .record_refused_admitted_call(&projection, &call)
            .expect("refusal outcome"),
        RefusalOutcome::Recorded
    );
    // The program's rejection event.
    let program_stream = format!("agentic-program:{}", projection.program_id);
    let program_event = store
        .event_by_idempotency_key(&program_stream, &format!("completion-rejected:{key}"))
        .expect("program event lookup")
        .expect("program rejection event");
    assert_eq!(program_event.kind, "agentic.completion_rejected");
    assert_eq!(program_event.scope, RuntimeEventScope::Program);
    assert_eq!(program_event.status.as_deref(), Some("rejected"));
    assert_eq!(
        program_event.idempotency_key.as_deref(),
        Some(format!("completion-rejected:{key}").as_str())
    );
    assert_eq!(program_event.payload["request_action_id"], key);
    // Its effect-stream twin, on the refused call's own stream.
    let twin = store
        .event_by_idempotency_key(
            &format!("execution-effect:{key}"),
            &format!("agentic-single:{key}"),
        )
        .expect("twin lookup")
        .expect("effect stream twin");
    assert_eq!(twin.kind, "agentic.batch_member.index");
    assert_eq!(twin.scope, RuntimeEventScope::ExecutionNode);
    assert_eq!(twin.actor.as_deref(), Some("runtime.agentic_action"));
    assert_eq!(twin.status.as_deref(), Some("committed"));
    assert_eq!(twin.schema_version, 1);
    assert_eq!(twin.payload["outer_key"], key);
    assert_eq!(twin.payload["program_event_kind"], "agentic.completion_rejected");
    assert_eq!(twin.payload["program_stream"], program_stream);
    assert_eq!(
        twin.payload["program_event_key"],
        format!("completion-rejected:{key}")
    );
    // One transaction wrote both, and the receipt names both.
    assert_eq!(program_event.transaction_id, twin.transaction_id);
    assert_eq!(program_event.commit_cursor, twin.commit_cursor);
    let receipt = store
        .original_transaction_receipt(&program_event.transaction_id)
        .expect("transaction receipt")
        .expect("one transaction receipt");
    assert!(receipt.event_ids.contains(&program_event.event_id));
    assert!(receipt.event_ids.contains(&twin.event_id));
    // The same refusal again is not a second fact.
    assert_eq!(
        service
            .record_refused_admitted_call(&projection, &call)
            .expect("refusal outcome"),
        RefusalOutcome::Recorded
    );
    assert_eq!(
        store
            .list_stream(&format!("execution-effect:{key}"))
            .expect("effect stream")
            .len(),
        2,
        "the intent and its one refusal twin are the whole stream"
    );
}

#[test]
fn own832_retirement_execute_preserves_preclaim_and_claim_identity_fences() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(store);
    let (_, worker, _, task_ref) = team_with_reviewer_and_task(
        &service,
        "own832",
        "original evidence",
        Default::default(),
    );
    let mut task = service.project("program-1").unwrap().tasks[&task_ref].clone();
    task.status = AgenticTaskStatus::CancelRequested;
    task.claim_generation = 1;
    let attempt = super::super::program::AgenticTaskAttemptProjection {
        execution_id: "own832-original-execution".into(),
        agent_id: worker.clone(),
        membership_id: "own832-original-membership".into(),
        mode: harness_contract::agent_action::AgentAttemptMode::Execute,
        generation: 0,
    };
    for (execution, agent, expected) in [
        (None, None, true),
        (Some(attempt.execution_id.as_str()), Some(worker.as_str()), true),
        (Some(attempt.execution_id.as_str()), None, false),
        (None, Some(worker.as_str()), false),
        (Some("foreign-execution"), Some(worker.as_str()), false),
        (Some(attempt.execution_id.as_str()), Some("foreign-agent"), false),
    ] {
        task.claim_execution_id = execution.map(str::to_owned);
        task.claimant = agent.map(str::to_owned);
        assert_eq!(retirement_execute_claim_matches(&task, &attempt), expected);
        for generation in [1, 2] {
            let mut stale = attempt.clone();
            stale.generation = generation;
            assert!(!retirement_execute_claim_matches(&task, &stale));
        }
    }
}



fn native_unit_action(
    template: &AgentActionEnvelope,
    action_id: &str,
    action: AgentAction,
) -> AgentActionEnvelope {
    AgentActionEnvelope {
        action_id: action_id.into(),
        actor: template.actor.clone(),
        expected_revision: None,
        action,
    }
}

#[test]
fn r1_bounded_history_program_pages_past_one_tail_budget() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    service
        .apply(&root(
            "bounded-program-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Source team".into(),
                mission: "preserve complete current history".into(),
                objective: None,
            }),
        ))
        .unwrap();
    let before = service.project("program-1").unwrap();
    let stream = program_stream("program-1");
    let head = store.stream_revision(&stream).unwrap();
    let events = (0..400)
        .map(|index| crate::RuntimeTransactionEventInput {
            event: crate::RuntimeEventInput {
                stream_id: stream.clone(),
                scope: crate::RuntimeEventScope::Program,
                kind: "agentic.diagnostic".into(),
                status: None,
                actor: None,
                refs: Vec::new(),
                payload: json!({"ordinal":index,"padding":"x".repeat(32768)}),
            },
            idempotency_key: Some(format!("bounded-diagnostic:{index}")),
            schema_version: 1,
        })
        .collect();
    store
        .append_batch_if_revision(stream.clone(), head, "bounded-program-tail", events)
        .unwrap();
    let warm = service.project("program-1").unwrap();
    assert_eq!(warm.revision, head + 400);
    assert_eq!(warm.teams, before.teams);
    store
        .delete_projection_checkpoint(&read_model_projection_id("program-1"))
        .unwrap();
    drop(service);
    let restored = AgentActionService::new(Arc::clone(&store))
        .project("program-1")
        .unwrap();
    assert_eq!(restored, warm);
    let mut after = 0;
    let mut pages = 0;
    let frozen = store.stream_revision(&stream).unwrap();
    loop {
        let page = store
            .list_stream_after(&stream, after, frozen, 100, 1024 * 1024)
            .unwrap();
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= 100);
        assert!(
            page.iter()
                .map(|event| serde_json::to_vec(event).unwrap().len())
                .sum::<usize>()
                <= 1024 * 1024
        );
        for event in page {
            assert_eq!(event.sequence, after + 1);
            after = event.sequence;
        }
        pages += 1;
    }
    assert_eq!(after, frozen);
    assert!(pages > 2);
}

#[test]
fn r1_recovery_quarantine_blocks_original_program_mutation_and_preserves_healthy_program() {
    let backend = Arc::new(crate::runtime_event_store::EphemeralRuntimeEventStore::new());
    let store = Arc::new(RuntimeEventStore::from_backend(backend.clone()));
    let service = AgentActionService::new(Arc::clone(&store));
    let team = || {
        AgentAction::TeamCreate(TeamCreateInput {
            name: "Recovery owner".into(),
            mission: "retain original durable work".into(),
            objective: None,
        })
    };
    assert_eq!(
        service
            .apply(&root("recovery-open", team()))
            .unwrap()
            .status,
        AgentActionStatus::Applied
    );
    let before = service.project("program-1").unwrap();
    let head = store.stream_revision(&program_stream("program-1")).unwrap();
    store.replace_session_recovery_exclusions(std::collections::BTreeSet::from([
        "session-1".into()
    ]));
    assert!(service.program_recovery_is_excluded("program-1").unwrap());
    let error = service
        .apply(&root("recovery-blocked", team()))
        .unwrap_err();
    assert!(
        matches!(error, AgentActionServiceError::Store(RuntimeEventStoreError::InvalidTransaction(reason))
        if reason == "session_recovery_quarantined:session-1")
    );
    let mut foreign = root("recovery-foreign-session", team());
    foreign.actor.session_id = "healthy-session".into();
    let error = service.apply(&foreign).unwrap_err();
    assert!(
        matches!(error, AgentActionServiceError::Store(RuntimeEventStoreError::InvalidTransaction(reason))
        if reason == "session_recovery_quarantined:session-1")
    );
    assert_eq!(
        store.stream_revision(&program_stream("program-1")).unwrap(),
        head
    );
    assert_eq!(service.project("program-1").unwrap(), before);
    let mut healthy = root("recovery-healthy", team());
    healthy.actor.session_id = "healthy-session".into();
    healthy.actor.program_id = "healthy-program".into();
    healthy.actor.root_execution_id = Some("healthy-root".into());
    assert_eq!(
        service.apply(&healthy).unwrap().status,
        AgentActionStatus::Applied
    );
    assert!(!service
        .program_recovery_is_excluded("healthy-program")
        .unwrap());
    let restored = Arc::new(RuntimeEventStore::from_backend(backend));
    let replay = AgentActionService::new(restored);
    assert_eq!(replay.project("program-1").unwrap(), before);
}

fn root(action_id: &str, action: AgentAction) -> AgentActionEnvelope {
    root_in_turn("turn-1", action_id, action)
}

/// Same fixture, an isolated turn.
///
/// Every guard in this crate keys on the turn - the repetition ledger, the pattern
/// window, the inspection memory - and libtest runs tests on parallel threads, so a
/// shared turn lets one test's counters decide another test's outcome. The suite
/// passed test-by-test and failed as a suite for exactly this reason.
fn root_in_turn(turn_id: &str, action_id: &str, action: AgentAction) -> AgentActionEnvelope {
    AgentActionEnvelope {
        action_id: action_id.to_string(),
        actor: AgentActorBinding {
            original_execution: None,
            objective_id: "objective-1".to_string(),
            program_id: "program-1".to_string(),
            session_id: "session-1".to_string(),
            turn_id: turn_id.to_string(),
            root_execution_id: Some("root-execution-1".to_string()),
            required_team_count: 0,
            exact_team_count: None,
            objective_summary: "test objective".to_string(),
            model_lease: "test".to_string(),
            permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
            resource_scopes: Vec::new(),
            actor_id: "root-1".to_string(),
            kind: AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        },
        expected_revision: None,
        action,
    }
}

fn managed(
    action_id: &str,
    team_id: &str,
    agent_id: &str,
    action: AgentAction,
) -> AgentActionEnvelope {
    AgentActionEnvelope {
        action_id: action_id.to_string(),
        actor: AgentActorBinding {
            original_execution: None,
            objective_id: "objective-1".to_string(),
            program_id: "program-1".to_string(),
            session_id: "session-1".to_string(),
            turn_id: "turn-1".to_string(),
            root_execution_id: Some("root-execution-1".to_string()),
            required_team_count: 0,
            exact_team_count: None,
            objective_summary: "test objective".to_string(),
            model_lease: "test".to_string(),
            permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
            resource_scopes: Vec::new(),
            actor_id: agent_id.to_string(),
            kind: AgentActorKind::Agent,
            execution_id: Some(format!("execution:{agent_id}")),
            team_id: Some(team_id.to_string()),
            agent_id: Some(agent_id.to_string()),
        },
        expected_revision: None,
        action,
    }
}

fn supervisor(action_id: &str, execution_id: &str, action: AgentAction) -> AgentActionEnvelope {
    let mut envelope = root(action_id, action);
    envelope.actor.actor_id = "runtime.program-supervisor".to_string();
    envelope.actor.kind = AgentActorKind::Supervisor;
    envelope.actor.execution_id = Some(execution_id.to_string());
    envelope
}

#[test]
fn delegated_inspection_and_action_receipts_keep_root_closure_as_background() {
    use harness_contract::agent_action::AgentAttemptMode;

    let service = AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    let team = service
        .apply(&root(
            "owner-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Bound work".into(),
                mission: "complete assigned evidence".into(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let member = service
        .apply(&root(
            "owner-member",
            AgentAction::AgentInvite(
                serde_json::from_value(json!({
                    "team_ref": team,
                    "role": "Bound worker",
                    "mission": "work on one assignment",
                    "required_capabilities": ["read"],
                }))
                .expect("member input"),
            ),
        ))
        .expect("member")
        .changed_refs[0]
        .clone();
    let task_ref = service
        .apply(&root(
            "owner-task",
            AgentAction::TaskPublish(
                serde_json::from_value(json!({
                    "team_ref": team,
                    "title": "Assigned report",
                    "objective": "produce evidence",
                    "acceptance": "independent evidence review",
                }))
                .expect("task input"),
            ),
        ))
        .expect("task")
        .changed_refs[0]
        .clone();
    let mut program = service.project("program-1").expect("program");
    let root_request = root_in_turn(
        "turn-owner-root",
        "owner-root-inspect",
        AgentAction::StateInspect(StateInspectInput::default()),
    );
    for mode in [
        AgentAttemptMode::Execute,
        AgentAttemptMode::Review,
        AgentAttemptMode::Coordination,
    ] {
        let mut request = managed(
            "owner-leaf-inspect",
            &team,
            &member,
            AgentAction::StateInspect(StateInspectInput::default()),
        );
        let execution_id = format!("execution:owner-{mode:?}");
        request.actor.execution_id = Some(execution_id.clone());
        let task = program.tasks.get_mut(&task_ref).expect("bound Task");
        task.status = if mode == AgentAttemptMode::Review {
            AgenticTaskStatus::Submitted
        } else {
            AgenticTaskStatus::Claimed
        };
        task.active_attempts.clear();
        task.active_attempts.insert(
            execution_id.clone(),
            super::super::program::AgenticTaskAttemptProjection {
                execution_id,
                agent_id: member.clone(),
                membership_id: AgenticProgramProjection::membership_id(&member, &team),
                mode,
                generation: 0,
            },
        );
        let observation = inspected_observation(&service.store, &request, &program, None);
        assert_eq!(observation.status, AgentActionStatus::Observed);
        let page = observation.projection.expect("leaf page");
        assert!(page.get("next_rungs").is_none());
        assert_eq!(page["program_progress"]["owner"], "RootTurn");
        assert!(page["program_progress"]["role_note"]
            .as_str()
            .unwrap()
            .contains("read-only background"));
        assert_eq!(
            page["program_progress"]["next_rungs"],
            serde_json::Value::Array(
                crate::execution_core::goal::assessment::closure_rungs_for_program(&program)
            ),
        );
        let expected_action = match mode {
            AgentAttemptMode::Execute => "task_submit",
            AgentAttemptMode::Review => "task_review",
            AgentAttemptMode::Coordination => "bound coordination",
        };
        assert!(observation
            .actionable
            .iter()
            .any(|action| action.contains(expected_action)));
        assert_eq!(
            observation.actionable,
            actionable_for_actor(&program, &request.actor, false)
        );
        if mode == AgentAttemptMode::Execute {
            program.tasks.get_mut(&task_ref).unwrap().purpose =
                harness_contract::agent_action::TaskPurpose::Exploration;
            let exploration = inspected_observation(&service.store, &request, &program, None);
            assert!(exploration
                .actionable
                .iter()
                .any(|hint| hint
                    .contains(super::super::order_contract::EXPLORATION_CLOSEOUT_CONTRACT)));
            assert!(!exploration
                .actionable
                .iter()
                .any(|hint| hint.contains("Prefer one `task_submit`")));
            program.tasks.get_mut(&task_ref).unwrap().purpose =
                harness_contract::agent_action::TaskPurpose::Delivery;
        }
        assert!(!observation
            .actionable
            .iter()
            .any(|action| action.contains("claim an available Task")
                || action.contains("objective_complete_request")));
        let root_page = inspected_observation(&service.store, &root_request, &program, None)
            .projection
            .expect("root page");
        assert_eq!(
            root_page["next_rungs"],
            page["program_progress"]["next_rungs"]
        );
    }
}

#[test]
fn typed_decline_settles_only_its_opportunity_and_survives_cold_replay() {
    use harness_contract::agent_action::{AgentAttemptMode, TaskAttemptDispatchInput};
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(store.clone());
    let team = service
        .apply(&root(
            "decline-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Readers".into(),
                mission: "inspect evidence".into(),
                objective: None,
            }),
        ))
        .unwrap()
        .changed_refs[0]
        .clone();
    let mut agents = Vec::new();
    for index in 0..2 {
        agents.push(service.apply(&root(&format!("decline-agent-{index}"), AgentAction::AgentInvite(
            serde_json::from_value(json!({"team_ref":team,"role":"Reader","mission":"read evidence","required_capabilities":["read"]})).unwrap()
        ))).unwrap().changed_refs[0].clone());
    }
    let task = service.apply(&root("decline-task", AgentAction::TaskPublish(
        serde_json::from_value(json!({"team_ref":team,"title":"Evidence","objective":"read evidence","acceptance":"cited evidence","required_capabilities":["read"]})).unwrap()
    ))).unwrap().changed_refs[0].clone();
    let dispatch = |agent: &str, execution: &str, generation| {
        supervisor(
            &format!("dispatch:{execution}"),
            execution,
            AgentAction::TaskAttemptDispatch(TaskAttemptDispatchInput {
                task_ref: task.clone(),
                execution_id: execution.into(),
                agent_ref: agent.into(),
                membership_id: AgenticProgramProjection::membership_id(agent, &team),
                mode: AgentAttemptMode::Execute,
                generation,
            }),
        )
    };
    let execution = format!("execution:{}", agents[0]);
    assert_eq!(
        service
            .apply(&dispatch(&agents[0], &execution, 0))
            .unwrap()
            .status,
        AgentActionStatus::Applied
    );
    let message = AgentAction::MessagePublish(
        serde_json::from_value(json!({
            "topic_ref":format!("topic:program-1"), "summary":"This task needs different expertise",
            "intent":{"task_ref":task,"kind":"decline"}
        }))
        .unwrap(),
    );
    let decline = managed("decline", &team, &agents[0], message.clone());
    for invalid in [
        root("forged-decline", message.clone()),
        managed("other-decline", &team, &agents[1], message.clone()),
    ] {
        assert_eq!(
            service.apply(&invalid).unwrap().status,
            AgentActionStatus::Rejected
        );
    }
    // Prose is discussion only; it cannot consume the admitted opportunity.
    let prose = managed(
        "prose",
        &team,
        &agents[0],
        AgentAction::MessagePublish(
            serde_json::from_value(json!({
                "topic_ref":"topic:program-1", "summary":"DECLINE: narrative only", "refs":[task]
            }))
            .unwrap(),
        ),
    );
    assert_eq!(
        service.apply(&prose).unwrap().status,
        AgentActionStatus::Applied
    );
    assert_eq!(
        service.project("program-1").unwrap().tasks[&task]
            .active_attempts
            .len(),
        1
    );
    let applied = service.apply(&decline).unwrap();
    assert_eq!(applied.status, AgentActionStatus::Applied);
    let projection = service.project("program-1").unwrap();
    let work = &projection.tasks[&task];
    assert_eq!(work.status, AgenticTaskStatus::Published);
    assert_eq!(work.failed_attempts, 0);
    assert_eq!(work.claim_generation, 0);
    assert!(work.active_attempts.is_empty());
    let entry = projection.topics["topic:program-1"].last().unwrap();
    assert_eq!(
        entry.source_execution_id.as_deref(),
        Some(execution.as_str())
    );
    assert_eq!(entry.intent_generation, Some(0));
    let recovered = AgentActionService::new(store);
    assert_eq!(recovered.project("program-1").unwrap(), projection);
    assert!(recovered.apply(&decline).unwrap().duplicate);
    let claim = |id: &str, agent: &str| {
        managed(
            id,
            &team,
            agent,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task.clone(),
                reason: Some("I can perform this work".into()),
            }),
        )
    };
    assert_eq!(
        recovered
            .apply(&claim("late-claim", &agents[0]))
            .unwrap()
            .error
            .unwrap()
            .code,
        "task_opportunity_declined"
    );
    assert_eq!(
        recovered
            .apply(&dispatch(&agents[0], "fresh-execution-same-generation", 0))
            .unwrap()
            .error
            .unwrap()
            .code,
        "task_opportunity_declined"
    );
    assert_eq!(
        recovered
            .apply(&dispatch(&agents[1], "stale-generation", 7))
            .unwrap()
            .error
            .unwrap()
            .code,
        "attempt_generation_stale"
    );
    let second_execution = format!("execution:{}", agents[1]);
    assert_eq!(
        recovered
            .apply(&dispatch(&agents[1], &second_execution, 0))
            .unwrap()
            .status,
        AgentActionStatus::Applied
    );
    assert_eq!(
        recovered
            .apply(&claim("successor-claim", &agents[1]))
            .unwrap()
            .status,
        AgentActionStatus::Applied
    );
    let before_late = recovered.project("program-1").unwrap();
    let mut late = decline.clone();
    late.action_id = "late-decline".into();
    assert_eq!(
        recovered.apply(&late).unwrap().status,
        AgentActionStatus::Rejected
    );
    assert_eq!(
        recovered
            .apply(&managed("claimed-decline", &team, &agents[1], message))
            .unwrap()
            .status,
        AgentActionStatus::Rejected
    );
    assert_eq!(recovered.project("program-1").unwrap(), before_late);
    // Even after the task's claim generation advances, the old physical run
    // remains fenced; a new opportunity must be a different authenticated run.
    let mut stale_outbox = before_late.clone();
    let mut old_attempt = stale_outbox.tasks[&task].active_attempts[&second_execution].clone();
    old_attempt.execution_id = execution.clone();
    old_attempt.agent_id = agents[0].clone();
    stale_outbox
        .tasks
        .get_mut(&task)
        .unwrap()
        .active_attempts
        .insert(execution.clone(), old_attempt);
    let late_failure = supervisor(
        "late-failure",
        &execution,
        AgentAction::TaskAttemptFail(TaskAttemptFailInput {
            task_ref: task.clone(),
            execution_id: execution.clone(),
            mode: AgentAttemptMode::Execute,
            reason: "old physical failure".into(),
            retryable: true,
        }),
    );
    assert_eq!(
        validate_transition(&stale_outbox, &late_failure, now_ms(), None)
            .unwrap()
            .0,
        "task_claim_fence_mismatch"
    );
    let mut later = before_late;
    later.tasks.get_mut(&task).unwrap().status = AgenticTaskStatus::Rework;
    assert_eq!(
        validate_transition(&later, &claim("later-old-run", &agents[0]), now_ms(), None)
            .unwrap()
            .0,
        "task_opportunity_declined"
    );
}

#[test]
fn replay_authenticates_goal_derived_entity_in_original_transaction() {
    use harness_contract::agent_action::TeamUpdateInput;
    for case in 0..5 {
        let store = Arc::new(RuntimeEventStore::for_test());
        let service = AgentActionService::new(Arc::clone(&store));
        service.apply(&root("goal-fixture", AgentAction::TeamCreate(TeamCreateInput {
            name: "team".into(), mission: "replay".into(), objective: None,
        }))).unwrap();
        let action = root("goal-action", AgentAction::TeamUpdate(TeamUpdateInput {
            team_ref: "team:test".into(), mission_ref: None, request_retire: false, reason_ref: None,
        }));
        let stream = program_stream("program-1");
        let goal_stream = "goal:goal:root-execution-1".to_string();
        let entity = if case >= 2 { Some("goal:item") } else { None };
        let mut expected = vec![crate::ExpectedStreamRevision {
            stream_id: stream.clone(), expected_revision: store.stream_revision(&stream).unwrap(),
        }];
        let mut events = vec![crate::RuntimeTransactionEventInput {
            event: RuntimeEventInput { stream_id: stream.clone(), scope: RuntimeEventScope::Program,
                kind: ACTION_EVENT_KIND.into(), status: Some("applied".into()),
                actor: Some(action.actor.actor_id.clone()), refs: action_refs(&action, entity),
                payload: json!({"envelope":action,"entity_ref":entity}), },
            idempotency_key: Some(action_key(&action.action_id)), schema_version: 1,
        }];
        if case != 0 && case != 4 {
            expected.push(crate::ExpectedStreamRevision { stream_id: goal_stream.clone(), expected_revision: 0 });
            let goal_entity = if case == 3 { Some("goal:foreign") } else { entity };
            events.push(crate::RuntimeTransactionEventInput {
                event: RuntimeEventInput { stream_id: goal_stream, scope: RuntimeEventScope::Goal,
                    kind: "test.goal".into(), status: Some("applied".into()), actor: None,
                    refs: Vec::new(), payload: json!({"action_entity_ref":goal_entity}) },
                idempotency_key: Some("test-goal".into()), schema_version: 1,
            });
        }
        store.append_transaction(crate::AppendTransactionRequest {
            transaction_id: format!("goal-replay-{case}"), expected_streams: expected, events,
        }).unwrap();
        let result = service.replay_if_applied(&action);
        if case <= 2 { assert!(result.unwrap().unwrap().duplicate); }
        else { assert!(matches!(result, Err(AgentActionServiceError::MemberUnknown(_)))); }
    }
}

#[test]
fn replay_rejects_missing_or_mismatched_durable_entity() {
    for case in 0..5 {
        let store = Arc::new(RuntimeEventStore::for_test());
        let action = root("bad-replay", AgentAction::TeamCreate(TeamCreateInput {
            name: "team".into(), mission: "test replay provenance".into(), objective: None,
        }));
        let actual = entity_ref(&action).unwrap();
        let entity = match case { 0 => None, 1 => Some("team:foreign"), _ => Some(actual.as_str()) };
        let stream = program_stream("program-1");
        let mut payload = json!({"envelope": action});
        if let Some(value) = entity { payload["entity_ref"] = json!(value); }
        if case == 2 { payload["original_action_observation"] = json!(null); }
        if case == 3 { payload["original_action_observation"] = json!({"action_id":"foreign"}); }
        if case == 4 {
            payload["original_action_observation"] = json!({
                "receipt_id":"receipt:foreign", "action_id":"foreign", "action":action.action.kind(),
                "program_id":"program-1", "revision":1, "status":"applied", "duplicate":false,
                "changed_refs":[], "actionable":[]
            });
        }
        store.append_transaction(crate::AppendTransactionRequest {
            transaction_id: format!("bad-replay-{entity:?}"),
            expected_streams: vec![crate::ExpectedStreamRevision { stream_id: stream.clone(), expected_revision: 0 }],
            events: vec![crate::RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: stream.clone(), scope: RuntimeEventScope::Program,
                    kind: ACTION_EVENT_KIND.into(), status: Some("applied".into()),
                    actor: Some(action.actor.actor_id.clone()),
                    refs: action_refs(&action, entity), payload,
                },
                idempotency_key: Some(action_key(&action.action_id)), schema_version: 1,
            }],
        }).unwrap();
        let service = AgentActionService::new(Arc::clone(&store));
        assert!(matches!(service.apply(&action), Err(AgentActionServiceError::MemberUnknown(_))));
        assert_eq!(store.stream_revision(&stream).unwrap(), 1);
    }
}

#[test]
fn replay_uses_original_receipt_and_current_projection_without_another_write() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let first_action = root("replay-original", AgentAction::TeamCreate(TeamCreateInput {
        name: "first".into(), mission: "original work".into(), objective: None,
    }));
    let service = AgentActionService::new(Arc::clone(&store));
    let first = service.apply(&first_action).unwrap();
    service.apply(&root("replay-later", AgentAction::TeamCreate(TeamCreateInput {
        name: "later".into(), mission: "advance the head".into(), objective: None,
    }))).unwrap();
    let before = store.stream_revision(&program_stream("program-1")).unwrap();
    let recovered = AgentActionService::new(Arc::clone(&store));
    for _ in 0..2 {
        let replay = recovered.apply(&first_action).unwrap();
        assert!(replay.duplicate);
        assert_eq!(replay.changed_refs, first.changed_refs);
        assert_eq!(replay.revision, before);
        assert_eq!(store.stream_revision(&program_stream("program-1")).unwrap(), before);
    }
    let mut foreign = first_action.clone();
    foreign.actor.actor_id = "different-root".into();
    assert!(recovered.apply(&foreign).is_err());
    assert_eq!(store.stream_revision(&program_stream("program-1")).unwrap(), before);
}

#[test]
fn action_id_replay_requires_the_same_actor_and_payload_after_restart() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let action = root(
        "stable-action",
        AgentAction::TeamCreate(TeamCreateInput {
            name: "Research".into(),
            mission: "inspect original evidence".into(),
            objective: None,
        }),
    );
    let first = AgentActionService::new(Arc::clone(&store))
        .apply(&action)
        .unwrap();
    assert_eq!(first.status, AgentActionStatus::Applied);
    let recovered = AgentActionService::new(Arc::clone(&store));
    let stream = program_stream("program-1");
    let committed_revision = store.stream_revision(&stream).unwrap();
    let committed_cursor = store.durable_commit_cursor().unwrap();
    let replay = recovered.apply(&action).unwrap();
    assert!(replay.duplicate);
    assert_eq!(replay.changed_refs, first.changed_refs);
    assert_eq!(store.stream_revision(&stream).unwrap(), committed_revision);
    assert_eq!(store.durable_commit_cursor().unwrap(), committed_cursor);

    let mut changed_revision = action.clone();
    changed_revision.expected_revision = Some(first.revision);
    let mut changed_actor = action.clone();
    changed_actor.actor.actor_id = "different-root".into();
    let mut changed_mission = action.clone();
    if let AgentAction::TeamCreate(input) = &mut changed_mission.action {
        input.mission = "different work".into();
    } else {
        panic!("expected TeamCreate");
    }
    for (field, conflict) in [
        ("expected_revision", changed_revision),
        ("actor", changed_actor),
        ("mission", changed_mission),
    ] {
        // This is a service replay test: no invocation capability is constructed.
        // Prove each envelope is otherwise valid and reaches the original replay
        // identity comparison, not an unrelated earlier validation error.
        conflict.validate().unwrap();
        assert!(matches!(recovered.replay_if_applied(&conflict),
            Err(AgentActionServiceError::Corrupt(ref reason))
                if reason == "action_id conflicts with original action"), "{field}");
        assert!(matches!(recovered.apply(&conflict),
            Err(AgentActionServiceError::Corrupt(ref reason))
                if reason == "action_id conflicts with original action"), "{field}");
        assert_eq!(store.stream_revision(&stream).unwrap(), committed_revision, "{field}");
        assert_eq!(store.durable_commit_cursor().unwrap(), committed_cursor, "{field}");
    }
    let mut independent = action;
    independent.action_id = "independent-new-action".into();
    let second = recovered.apply(&independent).unwrap();
    assert_eq!(second.status, AgentActionStatus::Applied);
    assert!(!second.duplicate);
    assert_ne!(second.changed_refs, first.changed_refs);
    let mut actual_refs = recovered.project("program-1").unwrap().teams.keys().cloned().collect::<Vec<_>>();
    let mut expected_refs = vec![first.changed_refs[0].clone(), second.changed_refs[0].clone()];
    actual_refs.sort_unstable();
    expected_refs.sort_unstable();
    assert_eq!(actual_refs, expected_refs);
    assert!(!expected_refs.is_empty());
}

#[test]
fn program_cache_uses_shared_hot_state_budget_without_evicting_foreign_pins_or_journal() {
    use crate::execution_core::hot_state::{
        HotResidentClass, HotStateConfig, RuntimeHotStatePlane,
    };
    let mut config = HotStateConfig::default();
    config.memory.max_bytes = Some(8 * 1024);
    let hot = RuntimeHotStatePlane::new(config.clone());
    let residency = Arc::clone(hot.residency());
    residency.upsert(
        "foreign:pinned",
        HotResidentClass::DerivedProjection,
        "other-owner",
        2_000,
        Some(1),
    );
    assert!(residency.pin("foreign:pinned", "active-work"));
    let store = Arc::new(RuntimeEventStore::for_test());
    let cache =
        Arc::new(super::super::AgenticReadModel::new(100).with_residency(Arc::clone(&residency)));
    let service = AgentActionService::new(Arc::clone(&store)).with_read_model(Arc::clone(&cache));
    let mut committed = std::collections::BTreeSet::new();
    for index in 0..20 {
        let outcome = service
                .apply(&root(
                    &format!("budget-team-{index}"),
                    AgentAction::TeamCreate(TeamCreateInput {
                        name: format!("Budget team {index}"),
                        mission: "preserve canonical facts while releasing derived cache memory"
                            .into(),
                        objective: None,
                    })
                ))
                .unwrap();
        assert_eq!(outcome.status, AgentActionStatus::Applied);
        assert!(!outcome.changed_refs.is_empty());
        for reference in outcome.changed_refs {
            assert!(committed.insert(reference), "each action must create a distinct team");
        }
    }
    let expected = service.project("program-1").unwrap();
    let mut names = expected.teams.values().map(|team| team.name.clone()).collect::<Vec<_>>();
    let mut expected_names = (0..20).map(|index| format!("Budget team {index}")).collect::<Vec<_>>();
    names.sort_unstable();
    expected_names.sort_unstable();
    assert_eq!(names, expected_names);
    assert!(!committed.is_empty());
    assert_eq!(
        cache.len(),
        0,
        "an over-budget projection must not remain resident"
    );
    assert!(residency.snapshot("agentic-read-model:program-1").is_none());
    assert_eq!(residency.resident_bytes(), 2_000);
    assert_eq!(
        residency.snapshot("foreign:pinned").unwrap().pin_reasons,
        vec!["active-work"]
    );
    assert_eq!(
        store.stream_revision(&program_stream("program-1")).unwrap(),
        expected.revision
    );
    let cold = AgentActionService::new(Arc::clone(&store))
        .project("program-1")
        .unwrap();
    assert_eq!(
        cold, expected,
        "cache eviction cannot erase or change journal facts"
    );
    config.memory.max_bytes = Some(1024 * 1024);
    hot.reconfigure(&config).unwrap();
    assert_eq!(service.project("program-1").unwrap(), expected);
    assert!(cache.get("program-1").is_some());
    assert_eq!(cache.len(), usize::from(cache.get("program-1").is_some()));
    let resident = residency.snapshot("agentic-read-model:program-1").unwrap();
    assert!(resident.estimated_bytes > 8 * 1024);
    assert_eq!(resident.reconstruct_cursor, Some(expected.revision));
    drop(service);
    drop(cache);
    assert!(residency.snapshot("agentic-read-model:program-1").is_none());
    assert_eq!(residency.resident_bytes(), 2_000);
    assert!(residency.snapshot("foreign:pinned").is_some());
}

#[test]
fn read_model_large_histories_preserve_warm_identity_and_exact_one_and_ten_event_deltas() {
    // Reducer/consumer semantics, not a PostgreSQL performance baseline. Events
    // are seeded through the journal in one transaction to avoid benchmarking
    // repeated fixture setup instead of the read path under test.
    for history in [100u64, 1_000, 10_000] {
        let store = Arc::new(RuntimeEventStore::for_test());
        let service = AgentActionService::new(Arc::clone(&store));
        let team = service
            .apply(&root(
                "large-team",
                AgentAction::TeamCreate(TeamCreateInput {
                    name: "Large history".into(),
                    mission: "read only the relevant page".into(),
                    objective: None,
                }),
            ))
            .unwrap()
            .changed_refs[0]
            .clone();
        let agent = service
            .apply(&root(
                "large-agent",
                AgentAction::AgentInvite(AgentInviteInput {
                    team_ref: team.clone(),
                    role: "Reader".into(),
                    mission: "consume exact deltas".into(),
                    required_capabilities: vec![],
                    existing_agent_ref: None,
                    definition_ref: None,
                    model_profile_ref: None,
                    expertise_hints: vec![],
                    execution_requirements: vec![],
                }),
            ))
            .unwrap()
            .changed_refs[0]
            .clone();
        let append = |count: u64| {
            let stream = program_stream("program-1");
            let head = store.stream_revision(&stream).unwrap();
            store
                .append_transaction(AppendTransactionRequest {
                    transaction_id: format!("large:{head}:{count}"),
                    expected_streams: vec![ExpectedStreamRevision {
                        stream_id: stream.clone(),
                        expected_revision: head,
                    }],
                    events: (1..=count)
                        .map(|offset| {
                            let id = format!("message:{:06}", head + offset);
                            let envelope = root(
                                &id,
                                AgentAction::MessagePublish(MessagePublishInput {
                                    topic_ref: "topic:program-1".into(),
                                    summary: Some(format!("正文 {id}")),
                                    content_ref: None,
                                    refs: vec![],
                                    recipients: vec![],
                                    intent: None,
                                    issue_dispositions: vec![],
                                }),
                            );
                            RuntimeTransactionEventInput {
                                event: RuntimeEventInput {
                                    stream_id: stream.clone(),
                                    scope: RuntimeEventScope::Program,
                                    kind: ACTION_EVENT_KIND.into(),
                                    status: Some("applied".into()),
                                    actor: Some("root-1".into()),
                                    refs: vec![],
                                    payload: json!({"envelope":envelope,"entity_ref":id}),
                                },
                                idempotency_key: Some(format!("seed:{id}")),
                                schema_version: 1,
                            }
                        })
                        .collect(),
                })
                .unwrap();
        };
        let initial_head = store.stream_revision(&program_stream("program-1")).unwrap();
        append(history - initial_head);
        let initial = service.project_snapshot("program-1").unwrap();
        assert_eq!(initial.revision, history);
        let mut measurements = vec![];
        for repeat in 0..5 {
            let started = std::time::Instant::now();
            let warm = service.project_snapshot("program-1").unwrap();
            let warm_us = started.elapsed().as_micros();
            assert!(Arc::ptr_eq(
                &warm,
                &service.project_snapshot("program-1").unwrap()
            ));
            let mut deltas = vec![];
            for count in [1, 10] {
                let old = service.project_snapshot("program-1").unwrap();
                let old_entries = old.topics["topic:program-1"].len();
                append(count);
                let started = std::time::Instant::now();
                let current = service.project_snapshot("program-1").unwrap();
                let delta_us = started.elapsed().as_micros();
                assert_eq!(current.revision, old.revision + count);
                assert_eq!(
                    current.topics["topic:program-1"].len(),
                    old_entries + count as usize
                );
                assert_eq!(old.topics["topic:program-1"].len(), old_entries);
                service
                    .acknowledge_topic_observations(AgenticTopicObservationAck {
                        program_id: "program-1".into(),
                        execution_id: format!("large:{repeat}:{count}"),
                        through_revision: old.revision,
                        expected_cursor_revision: 0,
                        observation_kind: TopicObservationKind::WorkerTransport,
                    })
                    .unwrap();
                let page = service
                    .topic_observations(
                        "program-1",
                        &agent,
                        &team,
                        &format!("large:{repeat}:{count}"),
                        32,
                        48 * 1024,
                    )
                    .unwrap()
                    .unwrap();
                assert_eq!(page.entries.len(), count as usize);
                assert_eq!(
                    page.entries.first().unwrap().entry.revision,
                    old.revision + 1
                );
                assert_eq!(page.to_revision, current.revision);
                deltas.push(json!({"delta":count,"elapsed_us":delta_us}));
            }
            measurements.push(json!({"repeat":repeat,"warm_us":warm_us,"deltas":deltas}));
        }
        println!(
            "PROGRAM_READ_SEMANTICS {}",
            json!({"initial_events":history,"backend":"ephemeral_fixture_not_pg_performance","samples":measurements})
        );
    }
}

#[test]
fn read_snapshot_corruption_rebuilds_from_journal_and_repairs_the_checkpoint() {
    use crate::RuntimeEventStoreBackend;
    let backend = Arc::new(crate::runtime_event_store::EphemeralRuntimeEventStore::new());
    let store = Arc::new(RuntimeEventStore::from_backend(backend.clone()));
    let service = AgentActionService::new(Arc::clone(&store));
    service
        .apply(&root(
            "snapshot-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Original team".into(),
                mission: "original source obligation".into(),
                objective: None,
            }),
        ))
        .unwrap();
    let expected = service.project_snapshot("program-1").unwrap();
    let checkpoint_id = read_model_projection_id("program-1");
    let original = store
        .projection_checkpoint(&checkpoint_id)
        .unwrap()
        .unwrap();
    for case in ["body", "schema", "digest", "legacy", "ahead", "misbound"] {
        let mut payload = original.payload.clone();
        let mut source_cursor = original.source_cursor;
        match case {
            "body" => {
                payload["body"]["body"]["value"]["objective_summary"] = json!("forged valid JSON")
            }
            "schema" => payload["schema_version"] = json!(999),
            "digest" => payload["body"]["sha256"] = json!("sha256:wrong"),
            "legacy" => payload = payload["body"]["body"]["value"].clone(),
            "ahead" => {
                source_cursor += 10;
                payload["body"]["body"]["value"]["revision"] = json!(source_cursor);
                payload["body"]["sha256"] = json!(format!(
                    "sha256:{:x}",
                    Sha256::digest(payload["body"]["body"]["value"].to_string().as_bytes())
                ));
                payload["body"]["bytes"] =
                    json!(payload["body"]["body"]["value"].to_string().len());
            }
            "misbound" => {
                payload["body"]["body"]["value"]["program_id"] = json!("other-program");
                payload["body"]["sha256"] = json!(format!(
                    "sha256:{:x}",
                    Sha256::digest(payload["body"]["body"]["value"].to_string().as_bytes())
                ));
                payload["body"]["bytes"] =
                    json!(payload["body"]["body"]["value"].to_string().len());
            }
            _ => unreachable!(),
        }
        // Fault injection targets storage below the trusted content writer.
        // The production port correctly refuses malformed envelopes before
        // they can become a cache-recovery input.
        backend
            .put_projection_checkpoint(&checkpoint_id, source_cursor, &payload, now_ms())
            .unwrap();
        let injected = backend
            .projection_checkpoint(&checkpoint_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            injected.payload, payload,
            "{case}: corruption must actually reach storage"
        );
        assert_eq!(injected.source_cursor, source_cursor, "{case}");
        let restarted = AgentActionService::new(Arc::clone(&store));
        let rebuilt = restarted.project_snapshot("program-1").unwrap();
        assert_eq!(
            serde_json::to_value(rebuilt.as_ref()).unwrap(),
            serde_json::to_value(expected.as_ref()).unwrap(),
            "{case}"
        );
        let repaired = store
            .projection_checkpoint(&checkpoint_id)
            .unwrap()
            .unwrap();
        assert_eq!(repaired.source_cursor, expected.revision, "{case}");
        assert!(
            decode_read_snapshot(&repaired, "program-1", &store).is_some(),
            "{case}"
        );
        assert!(matches!(
            store.compare_and_repair_projection_checkpoint(
                &checkpoint_id,
                repaired.source_cursor,
                repaired.revision - 1,
                &repaired.payload,
                now_ms(),
            ),
            Err(RuntimeEventStoreError::StaleRevision { .. })
        ));
        assert_eq!(
            store
                .projection_checkpoint(&checkpoint_id)
                .unwrap()
                .unwrap(),
            repaired
        );
        assert_eq!(
            store.stream_revision(&program_stream("program-1")).unwrap(),
            expected.revision
        );
    }
}

#[test]
fn shared_read_model_tracks_durable_head_across_service_handles() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let read_model = Arc::new(super::super::AgenticReadModel::new(4));
    let first =
        AgentActionService::new(Arc::clone(&store)).with_read_model(Arc::clone(&read_model));
    let first_mutation = first
        .apply(&root(
            "cache-team-a",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Research".to_string(),
                mission: "establish cached state".to_string(),
                objective: None,
            }),
        ))
        .expect("first mutation");
    let before = first
        .project_snapshot("program-1")
        .expect("first projection");
    let warm = first
        .project_snapshot("program-1")
        .expect("shared warm projection");
    assert!(
        Arc::ptr_eq(&before, &warm),
        "a warm read must not copy the Program"
    );
    assert!(read_model.get("program-1").is_some());
    assert_eq!(read_model.len(), usize::from(read_model.get("program-1").is_some()));

    let second = AgentActionService::new(store).with_read_model(Arc::clone(&read_model));
    let second_mutation = second
        .apply(&root(
            "cache-team-b",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Review".to_string(),
                mission: "advance durable head".to_string(),
                objective: None,
            }),
        ))
        .expect("delta mutation");
    let after = second
        .project_snapshot("program-1")
        .expect("delta projection");
    assert!(after.revision > before.revision);
    let mut after_names = after.teams.values().map(|team| team.name.as_str()).collect::<Vec<_>>();
    after_names.sort_unstable();
    assert_eq!(after_names, ["Research", "Review"]);
    assert_eq!(before.teams.values().map(|team| team.name.as_str()).collect::<Vec<_>>(), ["Research"], "a published snapshot is immutable");
    assert!(!first_mutation.changed_refs.is_empty());
    assert!(!Arc::ptr_eq(&before, &after));
    assert!(Arc::ptr_eq(
        &after,
        &first.project_snapshot("program-1").unwrap()
    ));
    assert!(read_model.get("program-1").is_some());
    assert_eq!(read_model.len(), usize::from(read_model.get("program-1").is_some()));
    read_model.put(Arc::clone(&before));
    assert!(
        Arc::ptr_eq(&after, &read_model.get("program-1").unwrap()),
        "a delayed reader cannot regress the shared cache"
    );
}

#[test]
fn topic_observations_cross_teams_and_resume_from_durable_execution_cursor() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    let team_a = service
        .apply(&root(
            "topic-team-a",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Research".to_string(),
                mission: "publish evidence".to_string(),
                objective: None,
            }),
        ))
        .expect("team a")
        .changed_refs[0]
        .clone();
    let team_b = service
        .apply(&root(
            "topic-team-b",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Synthesis".to_string(),
                mission: "consume cross-Team evidence".to_string(),
                objective: None,
            }),
        ))
        .expect("team b")
        .changed_refs[0]
        .clone();
    let invite = |action_id: &str, team_ref: &str, role: &str| {
        service
            .apply(&root(
                action_id,
                AgentAction::AgentInvite(AgentInviteInput {
                    team_ref: team_ref.to_string(),
                    role: role.to_string(),
                    mission: "observe committed topic deltas".to_string(),
                    required_capabilities: vec!["read".to_string()],
                    existing_agent_ref: None,
                    definition_ref: None,
                    model_profile_ref: None,
                    expertise_hints: Vec::new(),
                    execution_requirements: Vec::new(),
                }),
            ))
            .expect("invite")
            .changed_refs[0]
            .clone()
    };
    let author = invite("topic-author", &team_a, "Author");
    let peer = invite("topic-peer", &team_a, "Peer");
    let cross_team_peer = invite("topic-cross-peer", &team_b, "Synthesizer");
    service
        .apply(&managed(
            "program-broadcast",
            &team_a,
            &author,
            AgentAction::MessagePublish(MessagePublishInput {
                topic_ref: "topic:program-1".to_string(),
                summary: Some("public cross-Team finding".to_string()),
                content_ref: None,
                refs: vec!["artifact://finding".to_string()],
                recipients: Vec::new(),
                intent: None,
                issue_dispositions: Vec::new(),
            }),
        ))
        .expect("Program broadcast");
    service
        .apply(&managed(
            "team-a-message",
            &team_a,
            &author,
            AgentAction::MessagePublish(MessagePublishInput {
                topic_ref: format!("topic:{team_a}"),
                summary: Some("Team-only implementation note".to_string()),
                content_ref: None,
                refs: Vec::new(),
                recipients: Vec::new(),
                intent: None,
                issue_dispositions: Vec::new(),
            }),
        ))
        .expect("Team message");

    let cross_page = service
        .topic_observations(
            "program-1",
            &cross_team_peer,
            &team_b,
            "execution:cross-team-peer",
            16,
            48 * 1024,
        )
        .expect("cross-Team observations")
        .expect("Program message");
    assert!(matches!(cross_page.entries.as_slice(), [item]
        if item.entry.summary.as_deref() == Some("public cross-Team finding")));
    service
        .acknowledge_topic_observations(AgenticTopicObservationAck {
            program_id: "program-1".to_string(),
            execution_id: "execution:cross-team-peer".to_string(),
            through_revision: cross_page.to_revision,
            expected_cursor_revision: cross_page.cursor_revision,
            observation_kind: super::TopicObservationKind::WorkerTransport,
        })
        .expect("acknowledge cross-Team page");

    let restarted = AgentActionService::new(store);
    assert!(restarted
        .topic_observations(
            "program-1",
            &cross_team_peer,
            &team_b,
            "execution:cross-team-peer",
            16,
            48 * 1024,
        )
        .expect("restart query")
        .is_none());
    let own_team_page = restarted
        .topic_observations(
            "program-1",
            &peer,
            &team_a,
            "execution:own-team-peer",
            16,
            48 * 1024,
        )
        .expect("own-Team observations")
        .expect("Program and own-Team messages");
    let mut summaries = own_team_page.entries.iter().map(|item| item.entry.summary.as_deref().unwrap()).collect::<Vec<_>>();
    summaries.sort_unstable();
    assert_eq!(summaries, ["Team-only implementation note", "public cross-Team finding"]);
    assert!(own_team_page
        .entries
        .iter()
        .any(|entry| entry.entry.summary.as_deref() == Some("Team-only implementation note")));

    let own_topic = format!("topic:{team_a}");
    for index in 0..37 {
        service
            .apply(&root(
                &format!("old-topic-{index}"),
                AgentAction::MessagePublish(MessagePublishInput {
                    topic_ref: own_topic.clone(),
                    summary: Some(format!("archived discussion {index}")),
                    content_ref: None,
                    refs: Vec::new(),
                    recipients: Vec::new(),
                    intent: None,
                    issue_dispositions: Vec::new(),
                }),
            ))
            .unwrap();
    }
    let detail = service
        .apply(&root(
            "topic-detail",
            AgentAction::StateInspect(StateInspectInput {
                entry_ref: Some(own_topic.clone()),
                ..StateInspectInput::default()
            }),
        ))
        .unwrap()
        .projection
        .unwrap();
    assert_eq!(detail["coverage"]["total"], 38);
    assert_eq!(detail["coverage"]["complete"], false);
    let mut request: StateInspectInput =
        serde_json::from_value(detail["directory_request"].clone()).unwrap();
    let mut seen = std::collections::BTreeSet::new();
    let mut first_cursor = None;
    loop {
        let page = service
            .apply(&root("topic-directory", AgentAction::StateInspect(request)))
            .unwrap()
            .projection
            .unwrap();
        for item in page["entries"].as_array().unwrap() {
            if item["kind"] != "topic_entry" {
                continue;
            }
            assert!(seen.insert(item["entry_ref"].as_str().unwrap().to_string()));
            let input: StateInspectInput =
                serde_json::from_value(item["inspect_request"].clone()).unwrap();
            let exact = service
                .apply(&root("topic-exact", AgentAction::StateInspect(input)))
                .unwrap()
                .projection
                .unwrap();
            assert_eq!(exact["entry"]["entry_id"], item["entry_ref"]);
        }
        if page["next_request"].is_null() {
            break;
        }
        first_cursor = page["next_page_cursor"].as_str().map(str::to_owned);
        request = serde_json::from_value(page["next_request"].clone()).unwrap();
    }
    assert_eq!(seen.len() as u64, detail["coverage"]["total"].as_u64().unwrap(), "every original topic entry must be returned once");
    let foreign = service
        .apply(&managed(
            "foreign-topic",
            &team_b,
            &cross_team_peer,
            AgentAction::StateInspect(StateInspectInput {
                entry_ref: Some(own_topic.clone()),
                ..StateInspectInput::default()
            }),
        ))
        .unwrap()
        .projection
        .unwrap();
    assert_eq!(foreign["not_found"], true);
    assert!(!foreign.to_string().contains("Team-only"));
    let replay = service
        .apply(&managed(
            "foreign-cursor",
            &team_a,
            &peer,
            AgentAction::StateInspect(StateInspectInput {
                query: Some(own_topic.clone()),
                page_cursor: first_cursor,
                ..StateInspectInput::default()
            }),
        ))
        .unwrap();
    assert_eq!(replay.status, AgentActionStatus::Rejected);
    let private = service
        .apply(&root(
            "direct-message",
            AgentAction::MessagePublish(MessagePublishInput {
                topic_ref: "topic:program-1".into(),
                summary: Some("recipient-only detail".into()),
                content_ref: None,
                refs: Vec::new(),
                recipients: vec![author.clone()],
                intent: None,
                issue_dispositions: Vec::new(),
            }),
        ))
        .unwrap()
        .changed_refs[0]
        .clone();
    let denied = service
        .apply(&managed(
            "private-inspect",
            &team_a,
            &peer,
            AgentAction::StateInspect(StateInspectInput {
                entry_ref: Some(private.clone()),
                ..StateInspectInput::default()
            }),
        ))
        .unwrap()
        .projection
        .unwrap();
    assert_eq!(denied["not_found"], true);
    let mut lead = managed(
        "lead-private-inspect",
        &team_a,
        &peer,
        AgentAction::StateInspect(StateInspectInput {
            entry_ref: Some(private),
            ..Default::default()
        }),
    );
    lead.actor.kind = AgentActorKind::TeamLead;
    let hidden = service.apply(&lead).unwrap().projection.unwrap();
    assert_eq!(hidden["not_found"], true);
    assert!(!hidden.to_string().contains("recipient-only detail"));
    service
        .apply(&root(
            "join-other-team",
            AgentAction::MembershipUpdate(harness_contract::agent_action::MembershipUpdateInput {
                team_ref: team_a.clone(),
                agent_ref: cross_team_peer.clone(),
                operation: harness_contract::agent_action::MembershipOperation::Join,
                reason_ref: None,
            }),
        ))
        .unwrap();
    let unchanged = service
        .apply(&managed(
            "old-run-after-join",
            &team_b,
            &cross_team_peer,
            AgentAction::StateInspect(StateInspectInput {
                entry_ref: Some(own_topic.clone()),
                ..Default::default()
            }),
        ))
        .unwrap()
        .projection
        .unwrap();
    assert_eq!(
        unchanged["not_found"], true,
        "new membership cannot broaden the old run's Team binding"
    );
    let new_scope = service
        .apply(&managed(
            "new-run-team-scope",
            &team_a,
            &cross_team_peer,
            AgentAction::StateInspect(StateInspectInput {
                entry_ref: Some(own_topic.clone()),
                ..Default::default()
            }),
        ))
        .unwrap()
        .projection
        .unwrap();
    assert_ne!(new_scope["not_found"], true);
    assert!(service
        .topic_observations(
            "program-1",
            &cross_team_peer,
            &team_b,
            "execution:cross-team-peer",
            16,
            48 * 1024
        )
        .unwrap()
        .is_none());
    assert!(service
        .topic_observations(
            "program-1",
            &cross_team_peer,
            &team_a,
            "execution:new-team-run",
            16,
            48 * 1024
        )
        .unwrap()
        .unwrap()
        .entries
        .iter()
        .any(|item| item.topic_ref == own_topic));
    let directory: StateInspectInput =
        serde_json::from_value(new_scope["directory_request"].clone()).unwrap();
    let first_page = service
        .apply(&managed(
            "scoped-page",
            &team_a,
            &cross_team_peer,
            AgentAction::StateInspect(directory),
        ))
        .unwrap()
        .projection
        .unwrap();
    let next: StateInspectInput =
        serde_json::from_value(first_page["next_request"].clone()).unwrap();
    let valid_next = service
        .apply(&managed(
            "same-scope-page",
            &team_a,
            &cross_team_peer,
            AgentAction::StateInspect(next.clone()),
        ))
        .unwrap();
    assert_eq!(valid_next.status, AgentActionStatus::Observed);
    let changed_scope = service
        .apply(&managed(
            "changed-scope-page",
            &team_b,
            &cross_team_peer,
            AgentAction::StateInspect(next.clone()),
        ))
        .unwrap();
    assert_eq!(changed_scope.status, AgentActionStatus::Rejected);
    let mut changed_run = managed(
        "changed-run-page",
        &team_a,
        &cross_team_peer,
        AgentAction::StateInspect(next),
    );
    changed_run.actor.execution_id = Some("another-physical-run".into());
    assert_eq!(
        service.apply(&changed_run).unwrap().status,
        AgentActionStatus::Rejected
    );

    let membership = AgenticProgramProjection::membership_id(&peer, &team_a);
    let member = service
        .apply(&root(
            "membership-exact",
            AgentAction::StateInspect(StateInspectInput {
                entry_ref: Some(membership.clone()),
                ..StateInspectInput::default()
            }),
        ))
        .unwrap()
        .projection
        .unwrap();
    assert_eq!(member["membership"]["membership_id"], membership);
}

/// A program-level read names the outstanding rungs, and names them from the
/// same function the barrier checkpoint renders them from.
///
/// The model-step invariant already forces a tool call whenever a rung is
/// owed, but it never said which one. A root that was forced to call something
/// and given nothing to aim at issued the same generic `{"query": "task"}` five
/// times while two of its Tasks sat Submitted and unreviewed, and its next rung
/// was `independent_review` the whole time. Reading the rungs on the polling
/// path is what turns "something is owed" into "this is what closes it".
#[test]
fn a_program_level_read_names_the_outstanding_rungs() {
    let turn = "turn-rungs";
    let service = AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    service
        .apply(&root_in_turn(
            turn,
            "rung-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Rung Team".to_string(),
                mission: "hold a task that still owes its review".to_string(),
                objective: None,
            }),
        ))
        .expect("team mutation");
    let page = service
        .apply(&root_in_turn(
            turn,
            "rung-inspect",
            AgentAction::StateInspect(StateInspectInput {
                query: None,
                wait_for_workers: false,
                scope_ref: None,
                after_revision: None,
                page_cursor: None,
                entry_ref: None,
            }),
        ))
        .expect("program page")
        .projection
        .expect("bounded index page");
    let rungs = page
        .get("next_rungs")
        .and_then(serde_json::Value::as_array)
        .expect("a program-level read must name the outstanding rungs");
    let steps = rungs
        .iter()
        .filter_map(|rung| rung.get("step").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>();
    assert!(
        steps.contains(&"publish_task"),
        "no Task is published yet, so that rung must be named: {steps:?}"
    );
    // The rungs are the barrier view's own, not a second derivation of them.
    let program = service
        .project_if_exists("program-1")
        .ok()
        .flatten()
        .expect("program projection");
    assert_eq!(
        serde_json::Value::Array(
            crate::execution_core::goal::assessment::closure_rungs_for_program(&program)
        ),
        page["next_rungs"].clone(),
        "both paths must render the same rungs"
    );
}

/// The second identical read at an unchanged revision is answered with the cheap
/// receipt, not with the whole page again.
///
/// A notice alone was tried and measured: it reached the model twenty-two times in
/// one live run and the share of calls that repeated an actor's own exact input did
/// not move. At an unchanged revision the honest answer to "what is the state" is
/// "the state you already have", which is what `after_revision` would have
/// returned. Runtime now serves that, and names the ways to get more.
#[test]
fn a_repeated_read_at_an_unchanged_revision_is_answered_cheaply() {
    let turn = "turn-cheap";
    let service = AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    service
        .apply(&root_in_turn(
            turn,
            "cheap-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Cheap Team".to_string(),
                mission: "hold state that does not move".to_string(),
                objective: None,
            }),
        ))
        .expect("team mutation");
    let request = || {
        AgentAction::StateInspect(StateInspectInput {
            query: None,
            wait_for_workers: false,
            scope_ref: None,
            after_revision: None,
            page_cursor: None,
            entry_ref: None,
        })
    };
    let first = service
        .apply(&root_in_turn(turn, "cheap-first", request()))
        .expect("first read")
        .projection
        .expect("program page");
    assert!(
        first.get("entries").is_some() || first.get("next_rungs").is_some(),
        "the first read is a real page: {first}"
    );
    let second = service
        .apply(&root_in_turn(turn, "cheap-second", request()))
        .expect("second read")
        .projection
        .expect("program page");
    assert_eq!(
        second["unchanged"],
        serde_json::json!(true),
        "an unchanged revision must be answered with the cheap receipt: {second}"
    );
    assert!(
        second["notice"]
            .as_str()
            .is_some_and(|notice| notice.contains("after_revision")),
        "and it must name how to wait cheaply or ask for something specific: {second}"
    );
    // The full page is still reachable the moment anything moves.
    service
        .apply(&root_in_turn(
            turn,
            "cheap-move",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Cheap Team Two".to_string(),
                mission: "advance the revision".to_string(),
                objective: None,
            }),
        ))
        .expect("team mutation");
    let third = service
        .apply(&root_in_turn(turn, "cheap-third", request()))
        .expect("third read")
        .projection
        .expect("program page");
    assert_ne!(
        third["unchanged"],
        serde_json::json!(true),
        "a moved revision must re-send the page: {third}"
    );
}

/// The stall signal is the entity that was asked about, not the program revision.
///
/// Keying on the revision missed the commonest loop: a model polling
/// `state_inspect {"query": "task:X"}` while the program advances elsewhere. The
/// revision moves, the check never fires, and the same query is re-issued against
/// a Task that has not moved at all. One live run was stopped for spinning with
/// exactly that shape in the evidence, after a revision-based check had already
/// shipped and been measured.
#[test]
fn an_unchanged_entity_is_a_stall_even_while_the_program_advances() {
    let turn = "turn-entity-stall";
    let service = AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    let team_ref = service
        .apply(&root_in_turn(
            turn,
            "entity-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Entity Team".to_string(),
                mission: "hold a task that does not move".to_string(),
                objective: None,
            }),
        ))
        .expect("team mutation")
        .changed_refs
        .first()
        .cloned()
        .expect("the created team ref");
    let task_ref = service
        .apply(&root_in_turn(
            turn,
            "entity-task",
            AgentAction::TaskPublish(harness_contract::agent_action::TaskPublishInput {
                title: "A task that does not move".to_string(),
                objective: "stay put".to_string(),
                acceptance: "nothing changes".to_string(),
                team_ref: team_ref.clone(),
                depends_on: Vec::new(),
                expertise_hints: Vec::new(),
                required_capabilities: Vec::new(),
                execution_requirements: Vec::new(),
                acceptance_checks: Vec::new(),
                criterion_source: Default::default(),
                obligation_refs: Vec::new(),
                purpose: Default::default(),
            }),
        ))
        .expect("task mutation")
        .changed_refs
        .first()
        .cloned()
        .expect("the published task ref");
    let request = || {
        AgentAction::StateInspect(StateInspectInput {
            query: Some(task_ref.clone()),
            wait_for_workers: false,
            scope_ref: None,
            after_revision: None,
            page_cursor: None,
            entry_ref: None,
        })
    };
    let first = service
        .apply(&root_in_turn(turn, "entity-first", request()))
        .expect("first read")
        .projection
        .expect("program page");
    assert_ne!(
        first["unchanged"],
        serde_json::json!(true),
        "the first read is served in full: {first}"
    );
    // Advance the program without touching the Task the query asked about.
    service
        .apply(&root_in_turn(
            turn,
            "entity-elsewhere",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Unrelated Team".to_string(),
                mission: "advance the revision elsewhere".to_string(),
                objective: None,
            }),
        ))
        .expect("team mutation");
    let second = service
        .apply(&root_in_turn(turn, "entity-second", request()))
        .expect("second read")
        .projection
        .expect("program page");
    assert_eq!(
        second["unchanged"],
        serde_json::json!(true),
        "the Task the query asked about did not move, so this is a stall even \
             though the program revision did: {second}"
    );
}

/// An exact-entity read and the `unchanged` receipt stay narrow.
///
/// The `unchanged` receipt exists to be a few bytes; attaching rungs to it
/// would defeat the cheap-wait path. An exact read is about one entity, and a
/// reader that asked for one Task does not need the Program's rung list.
#[test]
fn exact_and_unchanged_reads_stay_narrow() {
    let service = AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    service
        .apply(&root(
            "narrow-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Narrow Team".to_string(),
                mission: "hold one task".to_string(),
                objective: None,
            }),
        ))
        .expect("team mutation");
    let exact = service
        .apply(&root(
            "narrow-exact",
            AgentAction::StateInspect(StateInspectInput {
                query: None,
                wait_for_workers: false,
                scope_ref: Some("program-1".to_string()),
                after_revision: None,
                page_cursor: None,
                entry_ref: None,
            }),
        ))
        .expect("exact read")
        .projection
        .expect("exact page");
    assert!(
        exact.get("next_rungs").is_none(),
        "an exact-entity read must stay narrow: {exact}"
    );
}

fn state_inspect_pages_indexes_and_never_falls_back_to_full_program_dump() {
    let service = AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    for index in 0..33 {
        let result = service
            .apply(&root(
                &format!("inspect-team-{index}"),
                AgentAction::TeamCreate(TeamCreateInput {
                    name: format!("Team {index:02}"),
                    mission: "provide a bounded inspect fixture".to_string(),
                    objective: None,
                }),
            ))
            .expect("team mutation");
        assert_eq!(result.status, AgentActionStatus::Applied);
    }
    let first = service
        .apply(&root(
            "inspect-first-page",
            AgentAction::StateInspect(StateInspectInput {
                query: None,
                wait_for_workers: false,
                scope_ref: None,
                after_revision: None,
                page_cursor: None,
                entry_ref: None,
            }),
        ))
        .expect("first page");
    let first = first.projection.expect("bounded index page");
    assert_eq!(first["entries"].as_array().map(Vec::len), Some(32));
    let cursor = first["next_page_cursor"].as_str().unwrap().to_string();
    assert!(cursor.contains("revision"));
    assert!(
        first.get("tasks").is_none(),
        "index must not expose a full Program"
    );

    let second = service
        .apply(&root(
            "inspect-second-page",
            AgentAction::StateInspect(StateInspectInput {
                query: None,
                wait_for_workers: false,
                scope_ref: None,
                after_revision: None,
                page_cursor: Some(cursor.clone()),
                entry_ref: None,
            }),
        ))
        .expect("second page")
        .projection
        .expect("bounded second page");
    assert_eq!(second["entries"].as_array().map(Vec::len), Some(1));
    assert!(second["next_page_cursor"].is_null());

    let malformed = service
        .apply(&root(
            "inspect-malformed-cursor",
            AgentAction::StateInspect(StateInspectInput {
                query: None,
                wait_for_workers: false,
                scope_ref: None,
                after_revision: None,
                page_cursor: Some("state:not-a-number".to_string()),
                entry_ref: None,
            }),
        ))
        .expect("invalid inspect is an observation");
    assert_eq!(malformed.status, AgentActionStatus::Rejected);
    assert_eq!(malformed.error.expect("error").code, "invalid_state_cursor");
    let changed_query = service
        .apply(&root(
            "inspect-query-change",
            AgentAction::StateInspect(StateInspectInput {
                query: Some("Team".into()),
                page_cursor: Some(cursor.clone()),
                ..StateInspectInput::default()
            }),
        ))
        .unwrap();
    assert_eq!(changed_query.status, AgentActionStatus::Rejected);
    let focused = service
        .apply(&root(
            "inspect-query",
            AgentAction::StateInspect(StateInspectInput {
                query: Some("Team 32".into()),
                ..StateInspectInput::default()
            }),
        ))
        .unwrap()
        .projection
        .unwrap();
    assert!(matches!(focused["entries"].as_array().unwrap().as_slice(), [entry] if entry["kind"] == "team" && entry["label"] == "Team 32"));
    service
        .apply(&root(
            "inspect-update",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "new team".into(),
                mission: "changed directory".into(),
                objective: None,
            }),
        ))
        .unwrap();
    let stale = service
        .apply(&root(
            "inspect-stale",
            AgentAction::StateInspect(StateInspectInput {
                page_cursor: Some(cursor),
                ..StateInspectInput::default()
            }),
        ))
        .unwrap();
    assert_eq!(stale.status, AgentActionStatus::Rejected);
    assert!(stale.error.unwrap().message.contains("revision"));
}



#[test]
fn completion_cannot_bypass_real_artifact_and_task_review() {
    let service = AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    service
        .apply(&root(
            "create-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Team".to_string(),
                mission: "Work".to_string(),
                objective: None,
            }),
        ))
        .expect("team");
    let rejected = service
        .apply(&root(
            "complete",
            AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                blocked: false,
                blocked_reason: None,
                human_review_required: false,
                recommended_next_actions: Vec::new(),
                result_refs: vec!["artifact:missing".to_string()],
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
            }),
        ))
        .expect("rejection");
    assert_eq!(rejected.status, AgentActionStatus::Rejected);
    assert_eq!(
        rejected.error.expect("error").code,
        "objective_not_verifiable"
    );
}

    #[test]
    fn r_e_1_the_named_failure_reaches_the_durable_projection() {
        let store = Arc::new(RuntimeEventStore::for_test());
        // The physical binding the root action carries is checked against the
        // graph store, so the service is wired to one and the graph it names
        // is registered with a RUNNING node.
        use crate::execution_core::graph::{
            ExecutionCommitService, ExecutionGraphStateStore, ExecutionNodeBinding,
            NodeExecutionTicket,
        };
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
            ExecutionNodeStatus,
        };
        let graphs = ExecutionGraphStateStore::new(Arc::clone(&store));
        let service = AgentActionService::new(Arc::clone(&store)).with_graph_store(graphs);
        let mut graph = ExecutionGraph::new("Entries named failure")
            .with_lineage(ExecutionGraphLineage {
                session_id: "session-1".into(),
                turn_id: "turn-1".into(),
                root_task_id: "task:entries".into(),
                task_id: "task:entries".into(),
                generation: 1,
            });
        graph.nodes.push(ExecutionNodeSpec {
            id: "entries-node".into(),
            kind: ExecutionNodeKind::InlineModel,
            payload_ref: "payload:entries".into(),
            executor_kind: "entries-test".into(),
            idempotency_key: "entries:attempt".into(),
            lease_ref: None,
            acceptance: Default::default(),
            retry_policy: Default::default(),
            resource_scopes: Vec::new(),
            work: None,
        });
        let commits = ExecutionCommitService::new(Arc::clone(&store));
        let graph = commits.register_graph(graph).unwrap().graph;
        // Planned -> Ready -> Running is the official sequence: a node that is
        // bound and started without being admitted first is an illegal
        // transition.
        let graph = commits
            .transition_node(
                &graph,
                "entries-node",
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let binding = ExecutionNodeBinding {
            executor_kind: "entries-test".to_string(),
            ticket_idempotency_key: "entries:attempt".to_string(),
            attempt: 1,
            resource_lease_refs: Vec::new(),
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        let graph = commits
            .bind_and_start_nodes(
                &graph,
                std::collections::BTreeMap::from([(
                    "entries-node".to_string(),
                    binding.clone(),
                )]),
                vec![NodeExecutionTicket {
                    graph_id: graph.id.clone(),
                    node_id: "entries-node".into(),
                    executor_kind: "entries-test".into(),
                    service_class: graph.service_class,
                    attempt: 1,
                    idempotency_key: "entries:attempt".to_string(),
                    payload_ref: "payload:entries".into(),
                }],
            )
            .unwrap()
            .graph;
        ExecutionGraphStateStore::new(Arc::clone(&store))
            .binding_start_ticket(&graph, "entries-node", &binding)
            .unwrap();
        let mut team_action = root(
            "entries-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Entries Team".to_string(),
                mission: "carry a named failure".to_string(),
                objective: None,
            }),
        );
        team_action.actor.original_execution = Some(
            harness_contract::agent_action::OriginalAgentExecution {
                graph_id: graph.id.clone(),
                node_id: "entries-node".to_string(),
                attempt: 1,
            },
        );
        // A Root actor's physical execution is the graph it names.
        team_action.actor.root_execution_id = Some(graph.id.clone());
        // The Root actor's identity is derived from the graph's own lineage:
        // objective, program and actor id all follow from the session and turn
        // the graph was registered under.
        let derived_objective =
            harness_contract::agent_action::root_objective_id("session-1", "turn-1");
        let derived_program =
            harness_contract::agent_action::program_id_for_objective(&derived_objective);
        team_action.actor.actor_id = "root:session-1".to_string();
        team_action.actor.objective_id = derived_objective.clone();
        team_action.actor.program_id = derived_program.clone();
        let team = service
            .apply(&team_action)
            .expect("team")
            .changed_refs[0]
            .clone();
        let named_failure = vec![serde_json::json!({
            "name": "required_write_terminally_failed:f24-failed-effect-key",
            "passed": false,
            "evidence": {
                "grade": "required_write_terminally_failed",
                "effect_key": "f24-failed-effect-key",
                "tool_use_id": "f24-failed-call",
                "receipt_cursor": 4242,
            },
        })];
        let mut invite = root(
            "named-failure-invite",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "worker".to_string(),
                mission: "named failure".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        );
        invite.actor.original_execution = Some(
            harness_contract::agent_action::OriginalAgentExecution {
                graph_id: graph.id.clone(),
                node_id: "entries-node".to_string(),
                attempt: 1,
            },
        );
        // A Root actor's physical execution is the graph it names.
        invite.actor.root_execution_id = Some(graph.id.clone());
        invite.actor.actor_id = "root:session-1".to_string();
        invite.actor.objective_id = derived_objective.clone();
        invite.actor.program_id = derived_program.clone();
        service
            .apply_locked(
                &invite,
                &super::program_stream(&derived_program),
                None,
                None,
                &named_failure,
                &[],
                false,
                false,
                None,
                None,
            )
            .expect("the named failure");
        let projection = service.project(&derived_program).expect("projection");
        let encoded = serde_json::to_value(&projection).expect("the projection");
        let entry = &encoded["required_write_failures"][0];
        assert!(
            entry["name"]
                .as_str()
                .unwrap_or_default()
                .contains("required_write_terminally_failed:"),
            "the entry is named by its prefix"
        );
        assert_eq!(entry["passed"], false);
        assert_eq!(entry["evidence"]["grade"], "required_write_terminally_failed");
        assert_eq!(entry["evidence"]["effect_key"], "f24-failed-effect-key");
        assert_eq!(entry["evidence"]["tool_use_id"], "f24-failed-call");
        assert_eq!(entry["evidence"]["receipt_cursor"], 4242);
        // Negative control: an apply that carries no named failure adds none.
        let mut plain = root(
            "no-named-failure-invite",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "worker".to_string(),
                mission: "no named failure".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        );
        plain.actor.original_execution = Some(
            harness_contract::agent_action::OriginalAgentExecution {
                graph_id: graph.id.clone(),
                node_id: "entries-node".to_string(),
                attempt: 1,
            },
        );
        // A Root actor's physical execution is the graph it names.
        plain.actor.root_execution_id = Some(graph.id.clone());
        plain.actor.actor_id = "root:session-1".to_string();
        plain.actor.objective_id = derived_objective.clone();
        plain.actor.program_id = derived_program.clone();
        service
            .apply_locked(
                &plain,
                &super::program_stream(&derived_program),
                None,
                None,
                &[],
                &[],
                false,
                false,
                None,
                None,
            )
            .expect("the plain invite");
        let projection = service.project(&derived_program).expect("projection");
        let encoded = serde_json::to_value(&projection).expect("the projection");
        assert_eq!(
            encoded["required_write_failures"]
                .as_array()
                .map(|entries| entries.len()),
            Some(1),
            "an apply without a named failure adds none"
        );
    }

#[test]
fn task_claim_requires_a_roster_agent_and_expired_lease_is_reclaimable() {
    let service = AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    let team = service
        .apply(&root(
            "lease-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Lease Team".to_string(),
                mission: "recover abandoned work".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let agent = service
        .apply(&root(
            "lease-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "worker".to_string(),
                mission: "own recoverable work".to_string(),
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
    let task = service
        .apply(&root(
            "lease-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Recover".to_string(),
                objective: "complete even after a worker crash".to_string(),
                acceptance: "durable result".to_string(),
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
    let root_rejection = service
        .apply(&root(
            "root-cannot-claim",
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task.clone(),
                reason: None,
            }),
        ))
        .expect("root rejection");
    assert_eq!(
        root_rejection.error.expect("error").code,
        "actor_cannot_execute_task"
    );

    let claimant = managed(
        "expired-reclaim",
        &team,
        &agent,
        AgentAction::TaskClaim(TaskClaimInput {
            task_ref: task.clone(),
            reason: Some("previous claimant lease expired".to_string()),
        }),
    );
    let mut projection = service.project("program-1").expect("projection");
    let task_projection = projection.tasks.get_mut(&task).expect("task projection");
    task_projection.status = AgenticTaskStatus::Claimed;
    task_projection.claimant = Some("agent:abandoned".to_string());
    task_projection.claim_execution_id = Some("execution:abandoned".to_string());
    task_projection.lease_expires_at_ms = Some(10);
    assert!(validate_transition(&projection, &claimant, 11, None).is_none());

    let expired_submit = managed(
        "expired-submit",
        &team,
        &agent,
        AgentAction::TaskSubmit(TaskSubmitInput {
            deliverable: None,
            task_ref: task.clone(),
            artifact_refs: vec!["artifact:any".to_string()],
            evidence_refs: vec!["artifact://any".to_string()],
            unresolved: Vec::new(),
        }),
    );
    let task_projection = projection.tasks.get_mut(&task).expect("task projection");
    task_projection.claimant = Some(agent);
    task_projection.claim_execution_id = expired_submit.actor.execution_id.clone();
    assert_eq!(
        validate_transition(&projection, &expired_submit, 11, None),
        Some(("task_claim_expired", task))
    );
}

#[test]
fn one_physical_agent_execution_cannot_claim_two_active_tasks() {
    let service = AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    let team = service
        .apply(&root(
            "single-execution-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Execution Fence Team".to_string(),
                mission: "keep physical work identity unambiguous".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let agent = service
        .apply(&root(
            "single-execution-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "worker".to_string(),
                mission: "execute exactly one bound task".to_string(),
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
    let first = service
        .apply(&root(
            "single-execution-task-a",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Task A".to_string(),
                objective: "Task A".to_string(),
                acceptance: "reviewed artifact".to_string(),
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
        .expect("task A")
        .changed_refs[0]
        .clone();
    let second = service
        .apply(&root(
            "single-execution-task-b",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Task B".to_string(),
                objective: "Task B".to_string(),
                acceptance: "reviewed artifact".to_string(),
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
        .expect("task B")
        .changed_refs[0]
        .clone();
    service
        .apply(&managed(
            "single-execution-claim-a",
            &team,
            &agent,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: first,
                reason: None,
            }),
        ))
        .expect("first claim");
    let rejected = service
        .apply(&managed(
            "single-execution-claim-b",
            &team,
            &agent,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: second,
                reason: None,
            }),
        ))
        .expect("second claim rejection");
    assert_eq!(rejected.status, AgentActionStatus::Rejected);
    assert_eq!(
        rejected.error.expect("error").code,
        "execution_already_claims_task"
    );
}

#[test]
fn repeated_identical_physical_failure_blocks_for_explicit_replan() {
    let service = AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    let team = service
        .apply(&root(
            "failure-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Failure recovery".to_string(),
                mission: "bound retries".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let agent = service
        .apply(&root(
            "failure-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "worker".to_string(),
                mission: "attempt work".to_string(),
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
    let task = service
        .apply(&root(
            "failure-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Bounded attempt".to_string(),
                objective: "never loop forever".to_string(),
                acceptance: "durable completion or blocker".to_string(),
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
    let execution_id = format!("execution:{agent}");
    for attempt in 1..=2 {
        assert_eq!(
            service
                .apply(&managed(
                    &format!("failure-claim-{attempt}"),
                    &team,
                    &agent,
                    AgentAction::TaskClaim(TaskClaimInput {
                        task_ref: task.clone(),
                        reason: None,
                    }),
                ))
                .expect("claim")
                .status,
            AgentActionStatus::Applied
        );
        assert_eq!(
            service
                .apply(&supervisor(
                    &format!("failure-settle-{attempt}"),
                    &execution_id,
                    AgentAction::TaskAttemptFail(TaskAttemptFailInput {
                        task_ref: task.clone(),
                        execution_id: execution_id.clone(),
                        mode: harness_contract::agent_action::AgentAttemptMode::Execute,
                        reason: "provider failure: unavailable capability".to_string(),
                        retryable: true,
                    }),
                ))
                .expect("settle")
                .status,
            AgentActionStatus::Applied
        );
    }
    let projection = service.project("program-1").expect("projection");
    let task = projection.tasks.get(&task).expect("task");
    assert_eq!(task.failed_attempts, 2);
    assert_eq!(task.status, AgenticTaskStatus::Blocked);
    assert_eq!(
        task.last_failure.as_deref(),
        Some("provider failure: unavailable capability")
    );
    assert_eq!(projection.status, crate::AgenticProgramStatus::Open);
    assert!(projection.unresolved.is_empty());
}

#[test]
fn repeated_identical_review_failure_blocks_for_explicit_replan() {
    let service = AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    let team = service
        .apply(&root(
            "review-failure-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Independent review".to_string(),
                mission: "review submitted work".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let task = service
        .apply(&root(
            "review-failure-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team,
                title: "Review retry".to_string(),
                objective: "prove review recovery".to_string(),
                acceptance: "bounded independent review".to_string(),
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
    // The execution worker reports review failure only after a real
    // submission. Set that already-durable precondition directly so this
    // reducer test stays focused on the review attempt state machine.
    let mut projection = service.project("program-1").expect("projection");
    projection.tasks.get_mut(&task).expect("task").status = AgenticTaskStatus::Submitted;

    for attempt in 1..=2 {
        let input = TaskAttemptFailInput {
            task_ref: task.clone(),
            execution_id: format!("review-execution-{attempt}"),
            mode: harness_contract::agent_action::AgentAttemptMode::Review,
            reason: "review provider failure: unavailable capability".to_string(),
            retryable: true,
        };
        assert_eq!(
            validate_transition(
                &projection,
                &supervisor(
                    &format!("review-failure-{attempt}"),
                    &input.execution_id,
                    AgentAction::TaskAttemptFail(input.clone()),
                ),
                now_ms(),
                None
            ),
            None
        );
        crate::agentic::work_market::apply_task_attempt_fail(&mut projection, &input);
    }
    let task = projection.tasks.get(&task).expect("task");
    assert_eq!(task.review_generation, 2);
    assert_eq!(task.failed_review_attempts, 2);
    assert_eq!(task.status, AgenticTaskStatus::Blocked);
    assert_eq!(
        task.last_failure.as_deref(),
        Some("review provider failure: unavailable capability")
    );
}

#[test]
fn roster_agent_can_expand_own_team_but_not_mutate_another_team() {
    let service = AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    let first_team = service
        .apply(&root(
            "delegation-team-a",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Team A".to_string(),
                mission: "own scope".to_string(),
                objective: None,
            }),
        ))
        .expect("first team")
        .changed_refs[0]
        .clone();
    let second_team = service
        .apply(&root(
            "delegation-team-b",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Team B".to_string(),
                mission: "separate scope".to_string(),
                objective: None,
            }),
        ))
        .expect("second team")
        .changed_refs[0]
        .clone();
    let agent = service
        .apply(&root(
            "delegation-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: first_team.clone(),
                role: "lead".to_string(),
                mission: "organize local work".to_string(),
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
    let local = service
        .apply(&managed(
            "delegation-local-invite",
            &first_team,
            &agent,
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: first_team.clone(),
                role: "peer".to_string(),
                mission: "help locally".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("local invite");
    assert_eq!(local.status, AgentActionStatus::Applied);
    let cross_team = service
        .apply(&managed(
            "delegation-cross-invite",
            &first_team,
            &agent,
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: second_team,
                role: "outsider".to_string(),
                mission: "mutate other team".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("cross-team observation");
    assert_eq!(cross_team.status, AgentActionStatus::Rejected);
    assert_eq!(
        cross_team.error.expect("error").code,
        "cross_team_roster_mutation_not_delegated"
    );
}

#[test]
fn concurrent_actions_serialize_without_stale_revision_loss() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(store);
    let workers = (0..24)
        .map(|index| {
            let service = service.clone();
            std::thread::spawn(move || {
                service
                    .apply(&root(
                        &format!("team-{index}"),
                        AgentAction::TeamCreate(TeamCreateInput {
                            name: format!("Team {index}"),
                            mission: "independent workstream".to_string(),
                            objective: None,
                        }),
                    ))
                    .expect("concurrent action")
            })
        })
        .collect::<Vec<_>>();
    let mut committed = std::collections::BTreeSet::new();
    for worker in workers {
        let outcome = worker.join().expect("worker");
        assert_eq!(outcome.status, AgentActionStatus::Applied);
        assert!(!outcome.changed_refs.is_empty());
        for reference in outcome.changed_refs {
            assert!(committed.insert(reference), "each action must create a distinct team");
        }
    }
    let projection = service.project("program-1").expect("projection");
    let mut names = projection.teams.values().map(|team| team.name.clone()).collect::<Vec<_>>();
    let mut expected = (0..24).map(|index| format!("Team {index}")).collect::<Vec<_>>();
    names.sort_unstable();
    expected.sort_unstable();
    assert_eq!(names, expected);
    assert_eq!(projection.revision, 25);
    assert!(!committed.is_empty());
    assert_eq!(projection.revision, 1 + committed.len() as u64);
}

#[test]
fn program_projection_recovers_exactly_after_service_reconstruction() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let team_id;
    let member_id;
    {
        let service = AgentActionService::new(Arc::clone(&store));
        team_id = service
            .apply(&root(
                "recover-team",
                AgentAction::TeamCreate(TeamCreateInput {
                    name: "Recovery Team".to_string(),
                    mission: "survive a Runtime restart".to_string(),
                    objective: None,
                }),
            ))
            .expect("commit team")
            .changed_refs[0]
            .clone();
        member_id = service
            .apply(&root(
                "recover-agent",
                AgentAction::AgentInvite(AgentInviteInput {
                    team_ref: team_id.clone(),
                    role: "recovery owner".to_string(),
                    mission: "resume durable work".to_string(),
                    required_capabilities: vec!["read".to_string()],
                    existing_agent_ref: None,
                    definition_ref: None,
                    model_profile_ref: None,
                    expertise_hints: Vec::new(),
                    execution_requirements: Vec::new(),
                }),
            ))
            .expect("commit member").changed_refs[0].clone();
        assert_eq!(service.project("program-1").unwrap().teams[&team_id].member_ids, vec![member_id.clone()]);
    }

    let reopened = AgentActionService::new(store);
    let projection = reopened.project("program-1").expect("replay program");
    assert_eq!(projection.teams[&team_id].name, "Recovery Team");
    assert_eq!(projection.teams[&team_id].member_ids, vec![member_id]);
    assert_eq!(projection.revision, 3);

    let duplicate = reopened
        .apply(&root(
            "recover-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Recovery Team".to_string(),
                mission: "survive a Runtime restart".to_string(),
                objective: None,
            }),
        ))
        .expect("idempotent replay after reopen");
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.revision, projection.revision);
}

#[test]
fn published_task_supersedes_across_teams_without_failure_and_rejects_invalid_replacements() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    let create_team = |id: &str| {
        service
            .apply(&root(
                id,
                AgentAction::TeamCreate(TeamCreateInput {
                    name: id.into(),
                    mission: "preserve the original obligation".into(),
                    objective: None,
                }),
            ))
            .unwrap()
            .changed_refs[0]
            .clone()
    };
    let first_team = create_team("source-team");
    let second_team = create_team("replacement-team");
    let publish = |id: &str, team: &str, dependencies: Vec<String>| {
        service
            .apply(&root(
                id,
                AgentAction::TaskPublish(TaskPublishInput {
                    team_ref: team.into(),
                    title: id.into(),
                    objective: "produce verifiable evidence".into(),
                    acceptance: "independently checked result".into(),
                    criterion_source: Default::default(),
                    acceptance_checks: Vec::new(),
                    required_capabilities: vec![],
                    depends_on: dependencies,
                    obligation_refs: vec![],
                    purpose: Default::default(),
                    execution_requirements: vec![],
                    expertise_hints: vec![],
                }),
            ))
            .unwrap()
            .changed_refs[0]
            .clone()
    };
    let source = publish("source-task", &first_team, vec![]);
    let successor = publish("replacement-task", &second_team, vec![]);
    let cyclic = publish("cyclic-task", &second_team, vec![source.clone()]);
    let action = |replacements: Vec<String>| {
        AgentAction::TaskSupersede(TaskSupersedeInput {
            task_ref: source.clone(),
            replacement_task_refs: replacements,
            reason: "new evidence favors work by the other team before execution".into(),
            evidence_refs: vec!["artifact://replanning-evidence".into()],
        })
    };
    let before = service.project("program-1").unwrap();
    assert_eq!(before.tasks[&source].status, AgenticTaskStatus::Published);
    assert_eq!(before.tasks[&source].failed_attempts, 0);
    for (id, replacements) in [
        ("empty-replacement", vec![]),
        ("self-replacement", vec![source.clone()]),
        ("cyclic-replacement", vec![cyclic]),
    ] {
        let rejected = service.apply(&root(id, action(replacements))).unwrap();
        assert_eq!(rejected.status, AgentActionStatus::Rejected, "{id}");
        assert_eq!(
            service.project("program-1").unwrap().revision,
            before.revision
        );
    }
    let mut foreign_lead = root("foreign-lead", action(vec![successor.clone()]));
    foreign_lead.actor.kind = AgentActorKind::TeamLead;
    foreign_lead.actor.team_id = Some(second_team.clone());
    foreign_lead.actor.agent_id = Some("foreign-lead-agent".into());
    foreign_lead.actor.execution_id = Some("foreign-lead-execution".into());
    let rejected = service.apply(&foreign_lead).unwrap();
    assert_eq!(rejected.status, AgentActionStatus::Rejected);
    assert_eq!(
        rejected.error.unwrap().code,
        "cross_team_work_mutation_not_delegated"
    );
    assert_eq!(
        service.project("program-1").unwrap().revision,
        before.revision
    );

    let commit = root(
        "root-cross-team-replacement",
        action(vec![successor.clone()]),
    );
    let applied = service.apply(&commit).unwrap();
    assert_eq!(applied.status, AgentActionStatus::Applied);
    drop(service);
    let recovered = AgentActionService::new(store);
    let projection = recovered.project("program-1").unwrap();
    let retired = &projection.tasks[&source];
    assert_eq!(retired.status, AgenticTaskStatus::Superseded);
    assert_eq!(retired.failed_attempts, 0);
    assert_eq!(retired.objective, before.tasks[&source].objective);
    assert_eq!(retired.acceptance, before.tasks[&source].acceptance);
    assert_eq!(
        retired.obligation_refs,
        before.tasks[&source].obligation_refs
    );
    assert_eq!(retired.replacement_task_refs, vec![successor.clone()]);
    assert_eq!(projection.tasks[&successor].team_id, second_team);
    assert_eq!(
        projection.tasks[&successor].status,
        AgenticTaskStatus::Published
    );
    assert!(!crate::agentic::work_market::task_dependency_satisfied(
        &projection,
        &source
    ));
    let replay = recovered.apply(&commit).unwrap();
    assert!(replay.duplicate);
    assert_eq!(replay.revision, applied.revision);
}

#[test]
fn failed_task_supersede_is_cas_idempotent_recoverable_and_not_fake_completion() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    let team = service
        .apply(&root(
            "supersede-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Recovery team".to_string(),
                mission: "replace a disproved approach without losing its audit trail".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let worker = service
        .apply(&root(
            "supersede-worker",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "investigator".to_string(),
                mission: "run falsifiable work".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("worker")
        .changed_refs[0]
        .clone();
    let source = service
        .apply(&root(
            "supersede-source",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Disproved approach".to_string(),
                objective: "test the original hypothesis".to_string(),
                acceptance: "reproducible evidence".to_string(),
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
        .expect("source task")
        .changed_refs[0]
        .clone();
    let publish_successor = |action_id: &str, title: &str, depends_on: Vec<String>| {
        service
            .apply(&root(
                action_id,
                AgentAction::TaskPublish(TaskPublishInput {
                    team_ref: team.clone(),
                    title: title.to_string(),
                    objective: "replace the failed hypothesis with bounded evidence".to_string(),
                    acceptance: "independently reviewable artifact".to_string(),
                    criterion_source: Default::default(),
                    acceptance_checks: Vec::new(),
                    required_capabilities: vec!["read".to_string()],
                    depends_on,

                    obligation_refs: Vec::new(),
                    purpose: Default::default(),
                    execution_requirements: Vec::new(),
                    expertise_hints: Vec::new(),
                }),
            ))
            .expect("successor task")
            .changed_refs[0]
            .clone()
    };
    let part_a = publish_successor("supersede-part-a", "Replacement A", Vec::new());
    let part_b = publish_successor("supersede-part-b", "Replacement B", Vec::new());
    let cyclic = publish_successor(
        "supersede-cyclic",
        "Invalid replacement",
        vec![source.clone()],
    );
    let supersede_action = |replacement_task_refs: Vec<String>| {
        AgentAction::TaskSupersede(TaskSupersedeInput {
            task_ref: source.clone(),
            replacement_task_refs,
            reason: "the provider attempt disproved the original approach; split the work"
                .to_string(),
            evidence_refs: vec!["tool://failure-receipt".to_string()],
        })
    };

    let invalid_replacement = service
        .apply(&root(
            "supersede-cycle-rejection",
            supersede_action(vec![cyclic.clone()]),
        ))
        .expect("cyclic replacement rejection");
    assert_eq!(invalid_replacement.status, AgentActionStatus::Rejected);
    assert_eq!(
        invalid_replacement.error.expect("error").code,
        "replacement_depends_on_superseded_task"
    );

    service
        .apply(&managed(
            "supersede-claim",
            &team,
            &worker,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: source.clone(),
                reason: None,
            }),
        ))
        .expect("claim");
    service
        .apply(&supervisor(
            "supersede-attempt-failed",
            &format!("execution:{worker}"),
            AgentAction::TaskAttemptFail(TaskAttemptFailInput {
                task_ref: source.clone(),
                execution_id: format!("execution:{worker}"),
                mode: harness_contract::agent_action::AgentAttemptMode::Execute,
                reason: "provider returned a reproducible counterexample".to_string(),
                retryable: true,
            }),
        ))
        .expect("attempt failure");

    let cycle_rejected = service
        .apply(&root(
            "supersede-cycle",
            supersede_action(vec![cyclic.clone()]),
        ))
        .expect("cycle rejection");
    assert_eq!(cycle_rejected.status, AgentActionStatus::Rejected);
    assert_eq!(
        cycle_rejected.error.expect("error").code,
        "replacement_depends_on_superseded_task"
    );
    let agent_rejected = service
        .apply(&managed(
            "supersede-by-worker",
            &team,
            &worker,
            supersede_action(vec![part_a.clone(), part_b.clone()]),
        ))
        .expect("actor rejection");
    assert_eq!(agent_rejected.status, AgentActionStatus::Rejected);
    assert_eq!(
        agent_rejected.error.expect("error").code,
        "task_supersede_not_delegated"
    );

    let current_revision = service.project("program-1").expect("projection").revision;
    let mut stale = root(
        "supersede-stale",
        supersede_action(vec![part_a.clone(), part_b.clone()]),
    );
    stale.expected_revision = Some(current_revision.saturating_sub(1));
    let stale = service.apply(&stale).expect("stale rejection");
    assert_eq!(stale.status, AgentActionStatus::Rejected);
    assert_eq!(stale.error.expect("error").code, "stale_revision");

    let mut supersede = root(
        "supersede-commit",
        supersede_action(vec![part_a.clone(), part_b.clone()]),
    );
    supersede.expected_revision = Some(current_revision);
    let applied = service.apply(&supersede).expect("supersede");
    assert_eq!(applied.status, AgentActionStatus::Applied);
    assert_eq!(applied.changed_refs, vec![source.clone()]);
    let duplicate = service.apply(&supersede).expect("idempotent replay");
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.revision, applied.revision);
    let audit_event = store
        .list_stream("agentic-program:program-1")
        .expect("audit stream")
        .into_iter()
        .find(|event| {
            event
                .payload
                .pointer("/envelope/action_id")
                .and_then(serde_json::Value::as_str)
                == Some("supersede-commit")
        })
        .expect("supersede audit event");
    assert!(audit_event
        .refs
        .iter()
        .any(|reference| reference.kind == "replacement_task" && reference.id == part_a));
    assert!(audit_event.refs.iter().any(|reference| {
        reference.kind == "evidence" && reference.id == "tool://failure-receipt"
    }));

    let restarted = AgentActionService::new(store);
    let recovered = restarted
        .project("program-1")
        .expect("recovered projection");
    let retired = &recovered.tasks[&source];
    assert_eq!(retired.status, AgenticTaskStatus::Superseded);
    assert_eq!(
        retired.replacement_task_refs,
        vec![part_a.clone(), part_b.clone()]
    );
    assert_eq!(retired.failed_attempts, 1);
    assert_eq!(
        retired.supersede_evidence_refs,
        vec!["tool://failure-receipt"]
    );
    assert_eq!(retired.superseded_by.as_deref(), Some("root-1"));

    let reclaim = restarted
        .apply(&managed(
            "supersede-reclaim",
            &team,
            &worker,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: source.clone(),
                reason: None,
            }),
        ))
        .expect("retired source rejection");
    assert_eq!(reclaim.status, AgentActionStatus::Rejected);
    assert_eq!(reclaim.error.expect("error").code, "task_not_claimable");

    let final_artifact_ref = "artifact:successor-final".to_string();
    let mut incomplete_projection = recovered.clone();
    incomplete_projection.artifacts.insert(
        final_artifact_ref.clone(),
        super::super::program::AgenticArtifactProjection {
            artifact_ref: final_artifact_ref.clone(),
            content_ref: "artifact://accepted-evidence".to_string(),
            kind: "final".to_string(),
            title: "Premature replacement synthesis".to_string(),
            relates_to: vec![part_a.clone(), part_b.clone()],
            committed_by: "root-1".to_string(),
            claim_execution_id: None,
            claim_generation: None,
        },
    );
    assert!(
        super::super::supervision::completion_gap(
            &incomplete_projection,
            &ObjectiveCompleteRequestInput {
                blocked: false,
                blocked_reason: None,
                human_review_required: false,
                recommended_next_actions: Vec::new(),
                result_refs: vec![final_artifact_ref.clone()],
                evidence_refs: vec!["tool://failure-receipt".to_string()],
                unresolved: Vec::new(),
            },
        )
        .is_some_and(|gap| gap.starts_with("tasks_not_accepted:")),
        "retiring the failed source must not waive unfinished successor Tasks"
    );

    let mut supervisor_projection = recovered;
    for successor in [&part_a, &part_b, &cyclic] {
        let task = supervisor_projection
            .tasks
            .get_mut(successor)
            .expect("successor");
        task.status = AgenticTaskStatus::Accepted;
        task.claimant = Some("agent:author".to_string());
        task.reviewed_by = Some("agent:reviewer".to_string());
        task.artifact_refs = vec![final_artifact_ref.clone()];
        task.evidence_refs = vec!["tool://accepted-evidence".to_string()];
    }
    supervisor_projection.artifacts.insert(
        final_artifact_ref.clone(),
        super::super::program::AgenticArtifactProjection {
            artifact_ref: final_artifact_ref.clone(),
            content_ref: "artifact://accepted-evidence".to_string(),
            kind: "final".to_string(),
            title: "Replacement synthesis".to_string(),
            relates_to: vec![part_a, part_b, cyclic],
            committed_by: "agent:author".to_string(),
            claim_execution_id: None,
            claim_generation: None,
        },
    );
    assert_eq!(
        super::super::supervision::completion_gap(
            &supervisor_projection,
            &ObjectiveCompleteRequestInput {
                blocked: false,
                blocked_reason: None,
                human_review_required: false,
                recommended_next_actions: Vec::new(),
                result_refs: vec![final_artifact_ref],
                evidence_refs: vec!["tool://accepted-evidence".to_string()],
                unresolved: Vec::new(),
            },
        ),
        None,
        "ObjectiveSupervisor must require accepted active successors while retaining, not accepting, the superseded source"
    );
}

#[test]
fn new_run_membership_authorizes_incremental_work_and_leave_revokes_it() {
    use harness_contract::agent_action::{MembershipOperation, MembershipUpdateInput};
    let service = AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    let mut teams = Vec::new();
    for name in ["home", "joined"] {
        teams.push(
            service
                .apply(&root(
                    name,
                    AgentAction::TeamCreate(TeamCreateInput {
                        name: name.into(),
                        mission: "collaborate".into(),
                        objective: None,
                    }),
                ))
                .unwrap()
                .changed_refs[0]
                .clone(),
        );
    }
    let invite = |team: &str| {
        AgentAction::AgentInvite(serde_json::from_value(json!({
        "team_ref":team,"role":"ordinary contributor","mission":"question and extend evidence"
    })).unwrap())
    };
    let agent_id = service
        .apply(&root("member", invite(&teams[0])))
        .unwrap()
        .changed_refs[0]
        .clone();
    let publish = || {
        AgentAction::TaskPublish(serde_json::from_value(json!({
        "team_ref":teams[1],"title":"new contribution","objective":"resolve one cited gap","acceptance":"source-backed response"
    })).unwrap())
    };
    for (label, action) in [
        ("before-publish", publish()),
        ("before-invite", invite(&teams[1])),
    ] {
        let before_rejected_write = service.project("program-1").unwrap();
        assert_eq!(
            service
                .apply(&managed(label, &teams[0], &agent_id, action))
                .unwrap()
                .status,
            harness_contract::agent_action::AgentActionStatus::Rejected
        );
        assert_eq!(
            service.project("program-1").unwrap(),
            before_rejected_write,
            "a delegated Agent outside the Team must not write Program state"
        );
    }
    for (operation, prefix, expected) in [
        (
            MembershipOperation::Join,
            "joined",
            harness_contract::agent_action::AgentActionStatus::Applied,
        ),
        (
            MembershipOperation::Leave,
            "left",
            harness_contract::agent_action::AgentActionStatus::Rejected,
        ),
    ] {
        let membership = managed(
            &format!("membership-{prefix}"),
            &teams[0],
            &agent_id,
            AgentAction::MembershipUpdate(MembershipUpdateInput {
                agent_ref: agent_id.clone(),
                team_ref: teams[1].clone(),
                operation,
                reason_ref: None,
            }),
        );
        assert_eq!(
            service.apply(&membership).unwrap().status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        for (label, action) in [("publish", publish()), ("invite", invite(&teams[1]))] {
            let before_old_run_write = service.project("program-1").unwrap();
            assert_eq!(
                service
                    .apply(&managed(
                        &format!("old-run-{prefix}-{label}"),
                        &teams[0],
                        &agent_id,
                        action.clone()
                    ))
                    .unwrap()
                    .status,
                harness_contract::agent_action::AgentActionStatus::Rejected
            );
            assert_eq!(
                service.project("program-1").unwrap(),
                before_old_run_write,
                "a revoked delegated run must not write Program state"
            );
            let result = service
                .apply(&managed(
                    &format!("{prefix}-{label}"),
                    &teams[1],
                    &agent_id,
                    action,
                ))
                .unwrap();
            assert_eq!(result.status, expected, "{prefix}-{label}: {result:?}");
        }
    }
    let projection = service.project("program-1").unwrap();
    assert!(matches!(projection.tasks.values().collect::<Vec<_>>().as_slice(), [task] if task.title == "new contribution" && task.team_id == teams[1]));
    assert!(projection
        .tasks
        .values()
        .all(|task| task.claimant.is_none()));
}

/// PLAN B2 (corrected): the operator's NAMED Team count is binding - a floor AND
/// a ceiling. `supervision.rs` already enforced the floor; nothing enforced the
/// ceiling, so a program frozen at "the user asked for 3 Teams" could open a 4th.
fn root_with_team_budget(
    action_id: &str,
    required: u8,
    exact: Option<u8>,
    action: AgentAction,
) -> AgentActionEnvelope {
    let mut envelope = root(action_id, action);
    envelope.actor.required_team_count = required;
    envelope.actor.exact_team_count = exact;
    envelope
}

fn team_create(action_id: &str, required: u8, exact: Option<u8>) -> AgentActionEnvelope {
    root_with_team_budget(
        action_id,
        required,
        exact,
        AgentAction::TeamCreate(TeamCreateInput {
            name: "Evidence".to_string(),
            mission: "collect independent evidence".to_string(),
            objective: None,
        }),
    )
}

fn seed_teams(projection: &mut crate::agentic::program::AgenticProgramProjection, count: usize) {
    for index in 0..count {
        let team_id = format!("team-{index}");
        projection.teams.insert(
            team_id.clone(),
            crate::agentic::program::AgenticTeamProjection {
                team_id,
                name: "t".to_string(),
                mission: "m".to_string(),
                objective: None,
                topic_ref: "topic".to_string(),
                created_by: "root-1".to_string(),
                member_ids: Vec::new(),
                task_ids: Vec::new(),
                lifecycle: crate::agentic::program::AgenticTeamLifecycle::Active,
            },
        );
    }
}

#[test]
fn an_explicit_exact_team_count_is_a_ceiling() {
    let mut projection =
        crate::agentic::program::AgenticProgramProjection::empty("program-1", "objective-1");
    projection.required_team_count = 3;
    projection.exact_team_count = Some(3);
    seed_teams(&mut projection, 2);

    assert!(
        validate_transition(&projection, &team_create("t3", 3, Some(3)), 0, None).is_none(),
        "the third Team is still within the operator's count"
    );
    seed_teams(&mut projection, 3);
    let rejection = validate_transition(&projection, &team_create("t4", 3, Some(3)), 0, None)
        .expect("a fourth Team must be refused when the user asked for three");
    assert_eq!(rejection.0, "team_ceiling_reached");
    assert!(
        rejection.1.contains('3'),
        "the refusal must name the operator's count: {}",
        rejection.1
    );
}

/// An explicit minimum permits alternative authorized topology above its floor.
#[test]
fn an_explicit_team_minimum_is_a_floor_and_not_a_ceiling() {
    let mut projection =
        crate::agentic::program::AgenticProgramProjection::empty("program-1", "objective-1");
    projection.required_team_count = 3; // a real floor ...
    projection.exact_team_count = None; // ... without an exact upper bound
    seed_teams(&mut projection, 5);

    assert!(
        validate_transition(&projection, &team_create("t6", 3, None), 0, None).is_none(),
        "an explicit minimum must not cap the model's topology choice"
    );

    // A truly autonomous objective has neither floor nor ceiling.
    let mut autonomous =
        crate::agentic::program::AgenticProgramProjection::empty("program-1", "objective-1");
    seed_teams(&mut autonomous, 9);
    assert!(
        validate_transition(&autonomous, &team_create("t10", 0, None), 0, None).is_none(),
        "an autonomous objective must never be capped"
    );
}


#[tokio::test]
async fn blocked_exit_requires_a_reason_human_review_and_next_actions() {
    let service = crate::AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    let team = service
        .apply(&root(
            "blocked-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Blocked".to_string(),
                mission: "prove the exception exit is guarded".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let missing_reason = service
        .apply(&root(
            "blocked-no-reason",
            AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                result_refs: vec![team.clone()],
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
                blocked: true,
                blocked_reason: Some(String::new()),
                human_review_required: true,
                recommended_next_actions: vec!["retry".to_string()],
            }),
        ))
        .expect("blocked exit");
    assert_eq!(missing_reason.status, AgentActionStatus::Rejected);
    assert_eq!(
        missing_reason.error.expect("error").code,
        "blocked_exit_incomplete"
    );

    let no_next_steps = service
        .apply(&root(
            "blocked-no-next",
            AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                result_refs: vec![team.clone()],
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
                blocked: true,
                blocked_reason: Some("the external source is unreachable".to_string()),
                human_review_required: true,
                recommended_next_actions: Vec::new(),
            }),
        ))
        .expect("blocked exit");
    assert_eq!(no_next_steps.status, AgentActionStatus::Rejected);
    assert_eq!(
        no_next_steps.error.expect("error").code,
        "blocked_exit_incomplete"
    );
}

#[tokio::test]
async fn blocked_exit_is_rejected_while_work_is_still_in_flight() {
    let service = crate::AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    let team = service
        .apply(&root(
            "inflight-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "InFlight".to_string(),
                mission: "prove a blocked exit is not an excuse to stop working".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let task = service
        .apply(&root(
            "inflight-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Open work".to_string(),
                objective: "still doable".to_string(),
                acceptance: "durable evidence".to_string(),
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
    let premature = service
        .apply(&root(
            "blocked-premature",
            AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                result_refs: vec![team.clone()],
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
                blocked: true,
                blocked_reason: Some("I decided this is unreachable".to_string()),
                human_review_required: true,
                recommended_next_actions: vec!["retry later".to_string()],
            }),
        ))
        .expect("premature blocked exit");
    assert_eq!(premature.status, AgentActionStatus::Rejected);
    assert_eq!(
        premature.error.expect("error").code,
        "blocked_exit_unearned",
        "work is still in flight, so the Program is not quiescent"
    );
    assert!(!task.is_empty());
}

#[tokio::test]
async fn blocked_exit_is_accepted_once_every_task_is_terminal() {
    let service = crate::AgentActionService::new(Arc::new(RuntimeEventStore::for_test()));
    let team = service
        .apply(&root(
            "quiesced-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Quiesced".to_string(),
                mission: "prove a well-formed blocked exit is accepted for human review"
                    .to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let task = service
        .apply(&root(
            "quiesced-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Attempted work".to_string(),
                objective: "unreachable in practice".to_string(),
                acceptance: "durable evidence".to_string(),
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
    service
        .apply(&root(
            "quiesced-withdraw",
            AgentAction::TaskWithdraw(harness_contract::agent_action::TaskWithdrawInput {
                task_ref: task.clone(),
                reason_ref: "objectively unreachable".to_string(),
                evidence_refs: Vec::new(),
            }),
        ))
        .expect("withdraw");
    let blocked = service
        .apply(&root(
            "quiesced-blocked",
            AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                result_refs: vec![team.clone()],
                evidence_refs: Vec::new(),
                unresolved: vec!["the source could not be read".to_string()],
                blocked: true,
                blocked_reason: Some("every read-only path was exhausted".to_string()),
                human_review_required: true,
                recommended_next_actions: vec!["obtain the source externally".to_string()],
            }),
        ))
        .expect("blocked exit");
    assert_eq!(
        blocked.error.expect("error").code,
        "objective_blocked_for_human_review",
        "a quiescent Program with a well-formed blocked exit becomes a durably recorded human-review Partial"
    );
    assert!(
        blocked
            .actionable
            .iter()
            .any(|hint| hint.contains("human review"))
            || blocked
                .actionable
                .iter()
                .any(|hint| hint.contains("inspect")),
        "the model must be told what to do with the recorded partial: {:?}",
        blocked.actionable
    );
}


#[test]
fn a_rework_round_must_be_reviewable_again_in_its_own_graph() {
    let mut projection = AgenticProgramProjection::empty("program-1", "objective-1");
    let task = "task:round-reducer";
    let publication = root(
        "round-task",
        AgentAction::TaskPublish(TaskPublishInput {
            team_ref: "team:round-reducer".into(),
            title: "One fact".into(),
            objective: "extract one verifiable fact".into(),
            acceptance: "artifact plus evidence".into(),
            criterion_source: Default::default(),
            acceptance_checks: vec![],
            required_capabilities: vec!["read".into()],
            depends_on: vec![],
            obligation_refs: vec![],
            purpose: Default::default(),
            execution_requirements: vec![],
            expertise_hints: vec![],
        }),
    );
    projection.apply(&publication, Some(task), 1, 1);
    for round in 1..=crate::agentic::work_market::MAX_TASK_REVIEW_ROUNDS {
        let mut executor = managed(
            &format!("round-{round}"),
            "team:round-reducer",
            "agent:executor",
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task.into(),
                reason: None,
            }),
        );
        executor.actor.execution_id = Some(format!("execute-graph-{round}"));
        projection.apply(&executor, Some(task), u64::from(round) * 3, 1);
        let claim = &projection.tasks[task];
        assert_eq!(claim.claim_generation, u64::from(round));
        assert_eq!(
            claim.claim_execution_id.as_deref(),
            executor.actor.execution_id.as_deref()
        );
        let submit = native_unit_action(
            &executor,
            &format!("round-{round}-submit"),
            AgentAction::TaskSubmit(TaskSubmitInput {
                task_ref: task.into(),
                artifact_refs: vec![format!("artifact:round-{round}")],
                evidence_refs: vec!["tool://observation".into()],
                unresolved: vec![],
                deliverable: None,
            }),
        );
        projection.apply(&submit, Some(task), u64::from(round) * 3 + 1, 1);
        assert_eq!(projection.tasks[task].status, AgenticTaskStatus::Submitted);
        let mut review = managed(
            &format!("round-{round}-review"),
            "team:round-reducer",
            "agent:reviewer",
            AgentAction::TaskReview(TaskReviewInput {
                gaps: Vec::new(),
                directed_actions: Vec::new(),
                task_ref: task.into(),
                decision: TaskReviewDecision::Rework,
                operator_requirement: Some("test objective".into()),
                reason: "does not state one verifiable fact".into(),
                evidence_refs: vec!["tool://independent-inspection".into()],
            }),
        );
        review.actor.execution_id = Some(format!("review-graph-{round}"));
        projection.apply(&review, Some(task), u64::from(round) * 3 + 2, 1);
        let reviewed = &projection.tasks[task];
        assert_eq!(reviewed.review_generation, u64::from(round));
        assert_eq!(
            reviewed.status,
            if round == crate::agentic::work_market::MAX_TASK_REVIEW_ROUNDS {
                AgenticTaskStatus::Blocked
            } else {
                AgenticTaskStatus::Rework
            }
        );
        assert!(reviewed.claimant.is_none());
        assert!(reviewed.claim_execution_id.is_none());
        assert!(reviewed.active_attempts.is_empty());
    }
}

#[test]
fn reopened_program_receipts_never_advertise_the_exit_action() {
    // A reopened Program records completion gaps. Its actionable receipt must
    // guide the model to close them: one live run received "resolve the
    // recorded gaps" and "request Objective completion" in the same receipt,
    // re-requested completion, and looped until the observation window expired.
    let mut program = crate::AgenticProgramProjection::empty("program:test", "objective:test");
    program.required_team_count = 1;
    program.artifacts.insert(
        "artifact:final".to_string(),
        crate::AgenticArtifactProjection {
            artifact_ref: "artifact:final".to_string(),
            content_ref: "artifact://art_final".to_string(),
            kind: "report".to_string(),
            title: "final synthesis".to_string(),
            relates_to: Vec::new(),
            committed_by: "agent:worker".to_string(),
            claim_execution_id: None,
            claim_generation: None,
        },
    );
    let task = crate::AgenticTaskProjection {
        obligations: Vec::new(),
        rework_directives: Vec::new(),
        unsettled_released_attempts: Vec::new(),
        terminal_note: None,
        task_id: "task:1".to_string(),
        team_id: "team:1".to_string(),
        title: "delivered".to_string(),
        objective: "objective".to_string(),
        acceptance: "accepted evidence".to_string(),
        acceptance_checks: Vec::new(),
        criterion_source: Default::default(),
        required_capabilities: Vec::new(),
        depends_on: Vec::new(),
        status: crate::AgenticTaskStatus::Accepted,
        claimant: Some("agent:worker".to_string()),
        claim_generation: 1,
        claim_execution_id: None,
        claimed_at_ms: None,
        lease_expires_at_ms: None,
        claim_renewal_streak: 0,
        active_attempts: Default::default(),
        artifact_refs: vec!["artifact:final".to_string()],
        evidence_refs: vec!["tool://receipt".to_string()],
        unresolved: Vec::new(),
        review_reason: None,
        reviewed_by: Some("agent:reviewer".to_string()),
        failed_attempts: 0,
        review_generation: 1,
        failed_review_attempts: 0,
        structural_execution_stalls: 0,
        structural_review_stalls: 0,
        reviews_applied: 0,
        review_obligation: None,
        reviews_accepted: 0,
        last_failure: None,
        replacement_task_refs: Vec::new(),
        supersede_evidence_refs: Vec::new(),
        superseded_reason: None,
        superseded_by: None,
        obligation_refs: Vec::new(),
        purpose: Default::default(),
        execution_requirements: Vec::new(),
        expertise_hints: Vec::new(),
        cancel_requested_by: None,
        cancel_reason_ref: None,
        cancel_evidence_refs: Vec::new(),
        pending_retirement: None,
    };
    program.tasks.insert("task:1".to_string(), task);
    program.unresolved = vec!["original_intent_review_required:user_intent".to_string()];

    let actions = super::actionable(
        &program,
        false,
        program.unresolved.first().map(String::as_str),
    );
    assert!(
        actions
            .iter()
            .any(|action| action.contains("Resolve the recorded gaps")),
        "{actions:?}"
    );
    assert!(
        actions
            .iter()
            .any(|action| action.contains("objective_review")),
        "{actions:?}"
    );
    assert!(
        !actions
            .iter()
            .any(|action| action.contains("request Objective completion")),
        "the exit action must stay silent while gaps remain: {actions:?}"
    );

    // Once the gaps clear, the exit action is advertised again.
    program.unresolved.clear();
    let actions = super::actionable(
        &program,
        false,
        program.unresolved.first().map(String::as_str),
    );
    assert!(
        actions
            .iter()
            .any(|action| action.contains("request Objective completion")),
        "{actions:?}"
    );
}

/// The v14 deadlock, replayed as a regression test.
///
/// Step 3.7 satisfied the `user_intent` criterion and recorded an
/// `objective_review` with decision `satisfied`. The Goal said so: criteria
/// `satisfied`, reviews `[satisfied]`, blockers `[]`. The Program projection
/// nevertheless kept repeating the reopen event's text -
/// `original_intent_review_required:user_intent; ... criterion_unsatisfied:
/// user_intent` - and withheld the exit action, so the model stopped asking to
/// finish and the run froze at 303 steps.
///
/// The gaps must therefore be derived from Goal truth, not frozen from the
/// event that recorded them.
#[test]
fn a_satisfied_review_retires_the_gap_it_was_sent_to_close() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    let goals = Arc::new(crate::execution_core::goal::GoalStore::new(Arc::clone(
        &store,
    )));

    // The Goal exactly as v14 left it: everything satisfied, but still carrying
    // the `completion_gap` the last reopen wrote.
    goals
        .create(GoalContract {
            input_contract: None,
            id: "goal:root-execution-1".to_string(),
            session_id: "session-1".to_string(),
            objective: "test objective".to_string(),
            criteria: vec![AcceptanceCriterion {
                id: "user_intent".to_string(),
                statement: "satisfy the operator".to_string(),
                statement_ref: None,
                source_refs: Vec::new(),
                required_evidence: Vec::new(),
                status: AcceptanceStatus::Satisfied,
                waiver: None,
            }],
            constraints: Vec::new(),
            phase: "waiting".to_string(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            scope: harness_contract::goal::GoalScope::UserObjective,
            user_intent_criterion_id: Some("user_intent".to_string()),
            source_intent_ref: Some("session_message:test".to_string()),
            execution_binding: Some(harness_contract::goal::GoalExecutionBinding {
                objective_id: "objective-1".to_string(),
                session_id: "session-1".to_string(),
                turn_id: "turn-1".to_string(),
                root_execution_id: "root-execution-1".to_string(),
                agentic_program_id: "program-1".to_string(),
            }),
            spec_revision: 1,
            spec_digest: "spec-digest-1".to_string(),
            review_refs: vec!["objective_review:a26".to_string()],
            reviews: vec![harness_contract::goal::ObjectiveReviewRecord {
                review_id: "objective_review:a26".to_string(),
                criterion_ref: "user_intent".to_string(),
                spec_revision: 1,
                input_manifest_digest: "spec-digest-1".to_string(),
                result_refs: Vec::new(),
                evidence_refs: Vec::new(),
                reviewer_actor: "agent:234a74107029a33c834cfbd7".to_string(),
                reviewer_execution_id: None,
                producer_refs: vec!["agent:1267d62300e69e4c4e79c725".to_string()],
                decision: "satisfied".to_string(),
                reason_ref: String::new(),
                verification: None,
            }],
            waiting: Some(harness_contract::goal::WaitDescriptor {
                reason_code: "completion_gap".to_string(),
                reason:
                    "original_intent_review_required:user_intent; criterion_unsatisfied:user_intent"
                        .to_string(),
                required_refs: vec![
                    "original_intent_review_required:user_intent".to_string(),
                    "criterion_unsatisfied:user_intent".to_string(),
                ],
                generation: 66,
            }),
            participation_requirement: None,
            obligations: Vec::new(),
            recovery: None,
            completion: GoalCompletion::Open,
            terminal: None,
            revision: 1,
            user_sequence: 1,
        })
        .unwrap();

    service
        .apply(&root(
            "open-program",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "team".into(),
                mission: "deliver".into(),
                objective: None,
            }),
        ))
        .unwrap();

    let projection = service.project("program-1").unwrap();
    assert!(
        projection.unresolved.is_empty(),
        "a satisfied review must retire the gap it closed, got: {:?}",
        projection.unresolved
    );

    let assessment =
        crate::execution_core::goal::assessment::read_bound_goal_assessment(&store, &projection)
            .unwrap();
    assert_eq!(
        assessment.gap(),
        None,
        "current Goal review clears the historical waiting gap"
    );
    let projection_again = service.project("program-1").unwrap();
    assert_eq!(
        serde_json::to_value(&projection_again).unwrap(),
        serde_json::to_value(&projection).unwrap(),
        "reading Goal assessment must not rewrite Program contents at the same revision"
    );
    let actions = super::actionable(&projection, false, None);
    assert!(
        !actions
            .iter()
            .any(|action| action.contains("criterion_unsatisfied")),
        "a satisfied criterion must not be reported as unsatisfied: {actions:?}"
    );
}

/// A reopened program that has since closed every gap must be told the exit is
/// reachable, and must be shown the reopen record as history rather than as the
/// current state. v14 failed exactly here: the framework kept naming
/// `criterion_unsatisfied:user_intent` after the criterion was satisfied, and
/// the model stopped asking to finish.
#[test]
fn a_resolved_reopen_advertises_the_exit_and_labels_the_record_as_history() {
    let mut program = crate::AgenticProgramProjection::empty("program:test", "objective:test");
    program.artifacts.insert(
        "artifact:final".to_string(),
        crate::AgenticArtifactProjection {
            artifact_ref: "artifact:final".to_string(),
            content_ref: "artifact://art_final".to_string(),
            kind: "report".to_string(),
            title: "final synthesis".to_string(),
            relates_to: Vec::new(),
            committed_by: "agent:worker".to_string(),
            claim_execution_id: None,
            claim_generation: None,
        },
    );
    let mut task = crate::AgenticTaskProjection {
        obligations: Vec::new(),
        rework_directives: Vec::new(),
        unsettled_released_attempts: Vec::new(),
        terminal_note: None,
        task_id: "task:1".to_string(),
        team_id: "team:1".to_string(),
        title: "delivered".to_string(),
        objective: "objective".to_string(),
        acceptance: "accepted evidence".to_string(),
        acceptance_checks: Vec::new(),
        criterion_source: Default::default(),
        required_capabilities: Vec::new(),
        depends_on: Vec::new(),
        status: crate::AgenticTaskStatus::Accepted,
        claimant: Some("agent:worker".to_string()),
        claim_generation: 1,
        claim_execution_id: None,
        claimed_at_ms: None,
        lease_expires_at_ms: None,
        claim_renewal_streak: 0,
        active_attempts: Default::default(),
        artifact_refs: vec!["artifact:final".to_string()],
        evidence_refs: vec!["tool://receipt".to_string()],
        unresolved: Vec::new(),
        review_reason: None,
        reviewed_by: Some("agent:reviewer".to_string()),
        failed_attempts: 0,
        review_generation: 1,
        failed_review_attempts: 0,
        structural_execution_stalls: 0,
        structural_review_stalls: 0,
        reviews_applied: 0,
        review_obligation: None,
        reviews_accepted: 0,
        last_failure: None,
        replacement_task_refs: Vec::new(),
        supersede_evidence_refs: Vec::new(),
        superseded_reason: None,
        superseded_by: None,
        obligation_refs: Vec::new(),
        purpose: Default::default(),
        execution_requirements: Vec::new(),
        expertise_hints: Vec::new(),
        cancel_requested_by: None,
        cancel_reason_ref: None,
        cancel_evidence_refs: Vec::new(),
        pending_retirement: None,
    };
    task.status = crate::AgenticTaskStatus::Accepted;
    program.tasks.insert("task:1".to_string(), task);

    // Every gap is closed, but the reopen that recorded them is still on file.
    program.unresolved = Vec::new();
    program.last_reopen_gaps = vec![
        "original_intent_review_required:user_intent; criterion_unsatisfied:user_intent"
            .to_string(),
    ];

    let actions = super::actionable(
        &program,
        false,
        program.unresolved.first().map(String::as_str),
    );
    assert!(
        actions
            .iter()
            .any(|action| action.contains("request Objective completion")),
        "the exit must be reachable once the gaps are gone: {actions:?}"
    );
    assert!(
        !actions
            .iter()
            .any(|action| action
                .starts_with("Every gap recorded by the last reopen is now resolved")),
        "Program history alone must not certify current Goal gaps as resolved: {actions:?}"
    );
    assert!(
        !actions
            .iter()
            .any(|action| action.contains("Resolve the recorded gaps")),
        "a resolved gap must not be presented as unresolved: {actions:?}"
    );

    // When a *different* gap is still open, the receipt names the current one
    // and keeps the reopen record as reference. Presenting the record as the
    // current state is what once told a model to keep resolving a criterion it
    // had already satisfied.
    program.unresolved = vec!["criterion_unsatisfied:second_criterion".to_string()];
    let actions = super::actionable(
        &program,
        false,
        program.unresolved.first().map(String::as_str),
    );
    let current = actions
        .iter()
        .find(|action| action.contains("Resolve the recorded gaps"))
        .expect("the current gap must be named");
    assert!(
        current.contains("second_criterion"),
        "the current gap must be the live one: {current}"
    );
    assert!(
        !current.contains("user_intent"),
        "history must not be merged into the current gap: {current}"
    );
    assert!(
        actions
            .iter()
            .any(|action| action.starts_with("For reference, the last reopen recorded")),
        "the reopen record stays visible as history: {actions:?}"
    );
    assert!(
        !actions
            .iter()
            .any(|action| action.contains("request Objective completion")),
        "the exit action must stay silent while a gap remains: {actions:?}"
    );
}

/// A Team with a worker and a distinct reviewer, plus a submitted Task. Returns
/// `(team_id, worker_id, reviewer_id, task_id)`.
fn team_with_reviewer_and_task(
    service: &AgentActionService,
    tag: &str,
    acceptance: &str,
    criterion_source: harness_contract::agent_action::CriterionSource,
) -> (String, String, String, String) {
    let team = service
        .apply(&root(
            &format!("{tag}-team"),
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Delivery".to_string(),
                mission: "produce and independently verify evidence".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let worker = service
        .apply(&root(
            &format!("{tag}-worker"),
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "implementer".to_string(),
                mission: "produce the report".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("worker")
        .changed_refs[0]
        .clone();
    let reviewer = service
        .apply(&root(
            &format!("{tag}-reviewer"),
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "reviewer".to_string(),
                mission: "verify evidence independently".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("reviewer")
        .changed_refs[0]
        .clone();
    let task = service
        .apply(&root(
            &format!("{tag}-task"),
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Research".to_string(),
                objective: "Read source".to_string(),
                acceptance: acceptance.to_string(),
                acceptance_checks: Vec::new(),
                criterion_source,
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
    let claim = service
        .apply(&managed(
            &format!("{tag}-claim"),
            &team,
            &worker,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task.clone(),
                reason: None,
            }),
        ))
        .expect("claim submitted review fixture");
    assert_eq!(claim.status, AgentActionStatus::Applied);
    let committed = service
        .apply(&managed(
            &format!("{tag}-artifact"),
            &team,
            &worker,
            AgentAction::ArtifactCommit(ArtifactCommitInput {
                content_ref: "artifact://review-fixture-body".to_string(),
                kind: "report".to_string(),
                title: "Review fixture".to_string(),
                relates_to: vec![task.clone()],
            }),
        ))
        .expect("commit review fixture");
    assert_eq!(committed.status, AgentActionStatus::Applied);
    let submitted = service
        .apply(&managed(
            &format!("{tag}-submit"),
            &team,
            &worker,
            AgentAction::TaskSubmit(TaskSubmitInput {
                task_ref: task.clone(),
                deliverable: None,
                artifact_refs: vec![committed.changed_refs[0].clone()],
                evidence_refs: vec!["tool://review-fixture".to_string()],
                unresolved: Vec::new(),
            }),
        ))
        .expect("submit review fixture");
    assert_eq!(submitted.status, AgentActionStatus::Applied);
    (team, worker, reviewer, task)
}

/// A payload that says nothing about provenance is the operator's own
/// requirement, not a silently demoted one. Without this default every legacy
/// Task would look model-invented and no rework could ever be justified.
#[test]
fn criterion_source_defaults_to_operator_derived_for_legacy_payloads() {
    let json = serde_json::json!({
        "team_ref": "team:1",
        "title": "Research",
        "objective": "Read source",
        "acceptance": "artifact and evidence"
    });
    let input: TaskPublishInput = serde_json::from_value(json).expect("legacy payload");
    assert_eq!(
        input.criterion_source,
        harness_contract::agent_action::CriterionSource::OperatorDerived
    );
}

#[test]
fn task_publish_records_criterion_source_and_projection_exposes_it() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    let team = service
        .apply(&root(
            "create-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Gamma".into(),
                mission: "coerced lane".into(),
                objective: None,
            }),
        ))
        .expect("team");
    let team_id = team.changed_refs[0].clone();
    let receipt = service
        .apply(&root(
            "publish-invented",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team_id.clone(),
                title: "Research".to_string(),
                objective: "Read source".to_string(),
                acceptance: "must also list every TODO/FIXME/unwrap hazard".to_string(),
                acceptance_checks: Vec::new(),
                criterion_source: harness_contract::agent_action::CriterionSource::AgentInvented,
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("task");
    let task_id = receipt.changed_refs[0].clone();
    let program = service.project("program-1").expect("program");
    let task = program.tasks.get(&task_id).expect("durable Task");
    assert_eq!(
        task.criterion_source,
        harness_contract::agent_action::CriterionSource::AgentInvented,
        "the projection must expose the recorded provenance"
    );
}

/// The notice is written where the standard was authored, so the author sees it
/// named as its own on every subsequent receipt. The cost is paid at authoring
/// time, so the correction loop belongs there too.
#[test]
fn invented_criterion_is_durably_labeled_for_author_and_reviewer() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    let team = service
        .apply(&root(
            "create-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Gamma".into(),
                mission: "coerced lane".into(),
                objective: None,
            }),
        ))
        .expect("team");
    let team_id = team.changed_refs[0].clone();
    service
        .apply(&root(
            "publish-invented",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team_id.clone(),
                title: "Research".to_string(),
                objective: "Read source".to_string(),
                acceptance: "must also survey the public API".to_string(),
                acceptance_checks: Vec::new(),
                criterion_source: harness_contract::agent_action::CriterionSource::AgentInvented,
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("task");
    let program = service.project("program-1").expect("program");
    assert!(
        program
            .program_warnings
            .iter()
            .any(|warning| warning.contains("operator never asked for")
                && warning.contains("must not rework")),
        "the invented standard must be labelled where it was written: {:?}",
        program.program_warnings
    );
    let actions = super::actionable(
        &program,
        false,
        program.unresolved.first().map(String::as_str),
    );
    assert!(
        actions
            .iter()
            .any(|action| action.contains("operator never asked for")),
        "the author must see the label on its own receipt: {actions:?}"
    );
}

fn create_bound_review_goal(store: Arc<RuntimeEventStore>, objective: &str, program_id: &str) {
    let goal: GoalContract = serde_json::from_value(json!({
        "id": "goal:root-execution-1", "session_id": "session-1", "objective": objective,
        "criteria": [{"id": "user_intent", "statement": objective, "status": "open"}],
        "phase": "executing", "scope": "user_objective", "completion": "open",
        "user_intent_criterion_id": "user_intent", "source_intent_ref": "session_message:original",
        "execution_binding": {"objective_id": "objective-1", "session_id": "session-1",
            "turn_id": "turn-1", "root_execution_id": "root-execution-1",
            "agentic_program_id": program_id},
        "spec_revision": 1, "spec_digest": "original-intent-digest", "revision": 1,
        "user_sequence": 1
    }))
    .expect("bound review goal");
    crate::execution_core::GoalStore::new(store)
        .create(goal)
        .expect("persist review goal");
}

#[tokio::test]
async fn program_evidence_alias_preflight_preserves_scope_and_content_guards() {
    let services = crate::RuntimeServices::in_memory().unwrap();
    let service = services.agent_action_service();
    let content = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/plain".into(),
                visibility_scope: "session:session-1".into(),
                expected_bytes: None,
                original_name: None,
            },
            b"retained evidence",
        )
        .await
        .unwrap();
    service
        .apply(&root(
            "alias-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Evidence".into(),
                mission: "Verify scoped evidence".into(),
                objective: None,
            }),
        ))
        .unwrap();
    let committed = service
        .apply(&root(
            "alias-content",
            AgentAction::ArtifactCommit(ArtifactCommitInput {
                content_ref: content.selector.clone(),
                kind: "report".into(),
                title: "Evidence".into(),
                relates_to: Vec::new(),
            }),
        ))
        .unwrap();
    assert_eq!(committed.status, AgentActionStatus::Applied);
    let logical = &committed.changed_refs[0];
    let alias = format!("artifact://{}", logical.strip_prefix("artifact:").unwrap());
    let actor = root(
        "scope",
        AgentAction::StateInspect(StateInspectInput::default()),
    )
    .actor;
    let review = |reference: String| {
        AgentAction::TaskReview(TaskReviewInput {
            gaps: Vec::new(),
            directed_actions: Vec::new(),
            task_ref: "task:preflight-only".into(),
            decision: TaskReviewDecision::Accept,
            reason: "retained content".into(),
            evidence_refs: vec![reference],
            operator_requirement: None,
        })
    };
    for reference in [content.selector.clone(), alias.clone()] {
        services
            .validate_agent_action_evidence(&actor, &review(reference))
            .await
            .unwrap();
    }
    let unknown = services
        .validate_agent_action_evidence(&actor, &review("artifact://unknown-logical-key".into()))
        .await
        .unwrap_err();
    assert!(
        unknown.contains("points to missing durable content"),
        "{unknown}"
    );
    let mut foreign_program = actor.clone();
    foreign_program.program_id = "another-program".into();
    let foreign = services
        .validate_agent_action_evidence(&foreign_program, &review(alias.clone()))
        .await
        .unwrap_err();
    assert!(
        foreign.contains("points to missing durable content"),
        "{foreign}"
    );
    let mut foreign_session = actor.clone();
    foreign_session.session_id = "another-session".into();
    let unreadable = services
        .validate_agent_action_evidence(&foreign_session, &review(alias.clone()))
        .await
        .unwrap_err();
    assert!(
        unreadable.contains("not readable in Session"),
        "{unreadable}"
    );
    services
        .artifact_store()
        .delete(&content, "session:session-1")
        .unwrap();
    let missing = services
        .validate_agent_action_evidence(&actor, &review(alias))
        .await
        .unwrap_err();
    assert!(
        missing.contains("points to missing durable content"),
        "{missing}"
    );
}



#[test]
fn review_rejects_a_goal_from_another_program_even_with_matching_wording() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    let (team, _, reviewer, task) = team_with_reviewer_and_task(
        &service,
        "wrong-goal",
        "test objective",
        harness_contract::agent_action::CriterionSource::OperatorDerived,
    );
    create_bound_review_goal(Arc::clone(&store), "test objective", "program-other");
    let before = service.project("program-1").unwrap();
    let cursor = store.durable_commit_cursor().unwrap();
    let receipt = service
        .apply(&managed(
            "wrong-goal-review",
            &team,
            &reviewer,
            AgentAction::TaskReview(TaskReviewInput {
                gaps: Vec::new(),
                directed_actions: Vec::new(),
                task_ref: task.clone(),
                decision: TaskReviewDecision::Rework,
                reason: "missing evidence".into(),
                operator_requirement: Some("test objective".into()),
                evidence_refs: vec!["tool://independent-review-fixture".to_string()],
            }),
        ))
        .unwrap();
    assert_eq!(receipt.status, AgentActionStatus::Rejected);
    assert_eq!(receipt.error.unwrap().code, "review_goal_binding_mismatch");
    assert_eq!(
        service.project("program-1").unwrap().tasks[&task].status,
        AgenticTaskStatus::Submitted
    );
    assert_eq!(service.project("program-1").unwrap(), before);
    assert_eq!(store.durable_commit_cursor().unwrap(), cursor);
}


#[test]
fn rework_without_operator_wording_is_refused() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    let (team, _worker, reviewer, task) = team_with_reviewer_and_task(
        &service,
        "invented",
        "must identify every TODO/FIXME/unwrap",
        harness_contract::agent_action::CriterionSource::AgentInvented,
    );
    let before = service.project("program-1").unwrap();
    let cursor = store.durable_commit_cursor().unwrap();
    let refused = service
        .apply(&managed(
            "rework-invented",
            &team,
            &reviewer,
            AgentAction::TaskReview(TaskReviewInput {
                gaps: Vec::new(),
                directed_actions: Vec::new(),
                operator_requirement: None,
                task_ref: task.clone(),
                decision: TaskReviewDecision::Rework,
                reason: "did not list every TODO/FIXME/unwrap".to_string(),
                evidence_refs: vec!["tool://independent-review-fixture".to_string()],
            }),
        ))
        .expect("review receipt");
    assert_eq!(refused.status, AgentActionStatus::Rejected);
    assert_eq!(
        refused.error.as_ref().map(|error| error.code.as_str()),
        Some("review_requirement_missing"),
        "{refused:?}"
    );
    assert_eq!(service.project("program-1").unwrap(), before);
    assert_eq!(store.durable_commit_cursor().unwrap(), cursor);
}


/// The gap a reopen produces is assembled with an `objective_completion_gap:`
/// prefix. Matching the bare `original_intent_review_required:` prefix never
/// fired, so the one sentence naming the tool the model must call was dropped
/// and a live run cycled seven completion requests without ever reviewing.
#[test]
fn the_review_guidance_fires_on_the_prefixed_gap_the_runtime_actually_emits() {
    let mut program = crate::AgenticProgramProjection::empty("program:test", "objective:test");
    program.status = crate::AgenticProgramStatus::Open;
    // Exactly the shape `objective_completion_waiting_gap` builds.
    program.unresolved = vec![
        "objective_completion_gap:original_intent_review_required:user_intent; uncovered_result_refs: artifact:a, artifact:b; criterion_unsatisfied:user_intent".to_string(),
    ];
    let actions = super::actionable(
        &program,
        false,
        program.unresolved.first().map(String::as_str),
    );
    assert!(
        actions
            .iter()
            .any(|action| action.contains("Use objective_review for the original criterion")),
        "the review how-to must fire on the prefixed gap: {actions:?}"
    );
    assert!(
        actions
            .iter()
            .any(|action| action.contains("artifact:a") && action.contains("artifact:b")),
        "the uncovered refs must be named so the model need not parse them out: {actions:?}"
    );
    assert!(
        !actions
            .iter()
            .any(|action| action.contains("request Objective completion")),
        "the exit action must stay silent while a gap remains: {actions:?}"
    );
}


/// A decline nobody can read is a decline nobody can diagnose.
///
/// The dispatch path declines for nine reasons and, until now, left no durable
/// trace of any of them. A Task stuck `Submitted` with no reviewer was
/// indistinguishable from one that was never asked, and two root causes were
/// guessed wrong from exactly that silence.
#[test]
fn a_dispatch_decline_is_recorded_durably_with_its_reason() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    service.record_dispatch_decline(AgenticDispatchDecline {
        program_id: "program-1",
        task_ref: "task-1",
        mode: "review",
        reason: AgenticDispatchDeclineReason::ModeStateMismatch,
        task_status: "claimed",
        eligible_members: 2,
        busy_members: 1,
        consecutive_deferrals: 3,
        review_generation: 0,
    });

    let recorded = store
        .all_events(64)
        .expect("read the durable event log")
        .into_iter()
        .filter(|event| event.kind == "agentic.dispatch.declined")
        .collect::<Vec<_>>();
    let [decline] = recorded.as_slice() else { panic!("expected one recorded decline: {recorded:?}") };
    let payload = &decline.payload;
    assert_eq!(payload["reason"], "mode_state_mismatch");
    assert_eq!(payload["task_ref"], "task-1");
    assert_eq!(payload["mode"], "review");
    assert_eq!(payload["task_status"], "claimed");
    assert_eq!(payload["eligible_members"], 2);
    assert_eq!(payload["busy_members"], 1);
    assert_eq!(payload["consecutive_deferrals"], 3);
    // A diagnostic record must not be able to re-arm the Program lane.
    assert_eq!(decline.scope, RuntimeEventScope::Relation);
}

/// `dispatch_ready_agentic_work` runs on every worker exit, so a Program that
/// stays undispatchable would emit the same decline forever and bury the one
/// that mattered.
#[test]
fn an_unchanged_decline_is_recorded_once_and_a_changed_one_again() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    fn decline(
        reason: AgenticDispatchDeclineReason,
        task_status: &str,
    ) -> AgenticDispatchDecline<'_> {
        AgenticDispatchDecline {
            program_id: "program-1",
            task_ref: "task-1",
            mode: "review",
            reason,
            task_status,
            eligible_members: 1,
            busy_members: 0,
            consecutive_deferrals: 1,
            review_generation: 0,
        }
    }
    for _ in 0..5 {
        service.record_dispatch_decline(decline(
            AgenticDispatchDeclineReason::ModeStateMismatch,
            "claimed",
        ));
    }
    let count = |kind: &str| {
        store
            .all_events(64)
            .expect("read the durable event log")
            .into_iter()
            .filter(|event| event.kind == kind)
            .count()
    };
    assert_eq!(
        count("agentic.dispatch.declined"),
        1,
        "the same decline repeated is not news"
    );
    // The Task advanced, so the same reason now describes a different fact.
    service.record_dispatch_decline(decline(
        AgenticDispatchDeclineReason::ModeStateMismatch,
        "submitted",
    ));
    assert_eq!(count("agentic.dispatch.declined"), 2);
    // A different reason is a different failure.
    service.record_dispatch_decline(decline(
        AgenticDispatchDeclineReason::NoEligibleMember,
        "submitted",
    ));
    assert_eq!(count("agentic.dispatch.declined"), 3);
}

/// Every reason must serialize to a distinct, stable name: the reader is a query
/// against `runtime_events`, and matching on prose is how the previous two root
/// causes were guessed wrong.
#[test]
fn every_decline_reason_has_a_distinct_stable_name() {
    let reasons = [
        AgenticDispatchDeclineReason::ProgramNotOpen,
        AgenticDispatchDeclineReason::StarvationBackoff,
        AgenticDispatchDeclineReason::RootGraphTerminal,
        AgenticDispatchDeclineReason::CoordinationWakeAbsent,
        AgenticDispatchDeclineReason::CoordinationIntentMismatch,
        AgenticDispatchDeclineReason::CoordinationNotCurrent,
        AgenticDispatchDeclineReason::CoordinationAlreadySettled,
        AgenticDispatchDeclineReason::ModeStateMismatch,
        AgenticDispatchDeclineReason::NoEligibleMember,
        AgenticDispatchDeclineReason::NoFreeEligibleMember,
        AgenticDispatchDeclineReason::FlightBusy,
    ];
    let names = reasons
        .iter()
        .map(|reason| reason.as_str())
        .collect::<Vec<_>>();
    let unique = names.iter().collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        unique.len(),
        names.len(),
        "duplicate reason name in {names:?}"
    );
    for name in &names {
        assert!(
            !name.is_empty() && name.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
            "{name}"
        );
    }
}

#[test]
fn pure_program_cache_rejects_legacy_goal_derived_snapshot() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    service
        .apply(&root(
            "pure-cache",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "team".into(),
                mission: "deliver".into(),
                objective: None,
            }),
        ))
        .unwrap();
    let checkpoint = store
        .projection_checkpoint(&read_model_projection_id("program-1"))
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.payload["schema_version"], 3);
    assert!(super::decode_read_snapshot(&checkpoint, "program-1", &store).is_some());
    let mut legacy = checkpoint;
    legacy.payload["schema_version"] = serde_json::json!(2);
    assert!(
        super::decode_read_snapshot(&legacy, "program-1", &store).is_none(),
        "Goal-derived cache requires reconstruction from original Program events"
    );
}

#[test]
fn bound_goal_assessment_is_lazy_and_root_only() {
    use crate::execution_core::goal::assessment::AssessmentReadError;
    let program = crate::AgenticProgramProjection::empty("program-1", "objective-1");
    let initial = AgentActionObservation {
        receipt_id: "observation:local".into(),
        action_id: "inspect".into(),
        action: "state_inspect".into(),
        program_id: "program-1".into(),
        revision: 1,
        status: AgentActionStatus::Observed,
        duplicate: false,
        changed_refs: vec![],
        actionable: vec!["bound work only".into()],
        projection: Some(
            json!({"program_progress":{"next_rungs":[{"step":"independent_review"}]}}),
        ),
        error: None,
    };
    for kind in [AgentActorKind::Agent, AgentActorKind::TeamLead] {
        let mut actor = root(
            "inspect",
            AgentAction::StateInspect(StateInspectInput::default()),
        )
        .actor;
        actor.kind = kind;
        let mut observation = initial.clone();
        attach_completion_assessment(&mut observation, &actor, &program, || {
            panic!("a leaf must not evaluate root-private Goal sources")
        });
        assert_eq!(
            serde_json::to_value(observation).unwrap(),
            serde_json::to_value(&initial).unwrap()
        );
    }
    let actor = root(
        "inspect",
        AgentAction::StateInspect(StateInspectInput::default()),
    )
    .actor;
    let mut observation = initial;
    attach_completion_assessment(&mut observation, &actor, &program, || {
        Err(AssessmentReadError::Source(
            "root source unavailable".into(),
        ))
    });
    assert_eq!(
        observation.projection.as_ref().unwrap()["completion_assessment"]["status"],
        "unknown"
    );
    assert!(observation
        .actionable
        .iter()
        .all(|action| !action.contains("request Objective completion")));
}

#[test]
fn team_quantity_admission_uses_original_journal_and_cold_projection() {
    for (minimum, exact, admitted) in [(3, None, 4), (3, Some(3), 3), (0, None, 4), (0, Some(0), 0)]
    {
        let store = Arc::new(RuntimeEventStore::for_test());
        let service = AgentActionService::new(Arc::clone(&store));
        for index in 0..4 {
            let action = team_create(&format!("quantity-{index}"), minimum, exact);
            let receipt = service.apply(&action).unwrap();
            if index < admitted {
                assert_eq!(
                    receipt.status,
                    AgentActionStatus::Applied,
                    "{minimum}/{exact:?}: {receipt:?}"
                );
            } else {
                assert_eq!(receipt.status, AgentActionStatus::Rejected, "{receipt:?}");
                assert_eq!(
                    receipt.error.as_ref().unwrap().code,
                    "team_ceiling_reached",
                    "{receipt:?}"
                );
            }
        }
        if admitted > 0 {
            let hot = service.project("program-1").unwrap();
            assert_eq!(
                (
                    hot.required_team_count,
                    hot.exact_team_count,
                    hot.team_count_contract_version
                ),
                (minimum, exact, 1)
            );
            assert_eq!(hot.teams.len(), admitted);
            let cold = AgentActionService::new(store).project("program-1").unwrap();
            assert_eq!(cold, hot);
        } else {
            assert!(
                service.project("program-1").is_err(),
                "rejected first Team must not bootstrap a Program"
            );
        }
    }
}

#[test]
fn unknown_team_quantity_and_forbidden_team_are_distinct_from_zero_minimum() {
    let mut projection = crate::AgenticProgramProjection::empty("program-1", "objective-1");
    projection.exact_team_count = Some(0);
    assert_eq!(
        validate_transition(&projection, &team_create("forbidden", 0, Some(0)), 0, None)
            .unwrap()
            .0,
        "team_ceiling_reached"
    );
    projection.exact_team_count = None;
    assert!(validate_transition(
        &projection,
        &team_create("optional-reviewer", 0, None),
        0,
        None
    )
    .is_none());
    projection.team_count_contract_version = 0;
    assert_eq!(
        validate_transition(&projection, &team_create("unknown", 0, None), 0, None)
            .unwrap()
            .0,
        "team_quantity_unknown"
    );
    projection.team_count_contract_version = 2;
    projection.exact_team_count = Some(0);
    assert!(projection.validate_team_quantity_contract().is_err());
}

#[test]
fn opened_quantity_decoder_rejects_malformed_present_authority() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    service
        .apply(&team_create("valid-open", 3, Some(3)))
        .unwrap();
    let opened = store
        .list_stream("agentic-program:program-1")
        .unwrap()
        .into_iter()
        .find(|event| event.kind == PROGRAM_OPENED_EVENT_KIND)
        .unwrap();
    for (field, value) in [
        ("exact_team_count", json!(999)),
        ("exact_team_count", json!("3")),
        ("team_count_contract_version", json!("1")),
        ("required_team_count", json!(-1)),
    ] {
        let mut corrupted = opened.clone();
        corrupted.payload[field] = value;
        assert!(
            projection_from_opened("program-1", &corrupted).is_err(),
            "{field}"
        );
    }
    let mut legacy = opened;
    legacy
        .payload
        .as_object_mut()
        .unwrap()
        .remove("team_count_contract_version");
    let legacy = projection_from_opened("program-1", &legacy).unwrap();
    assert_eq!(legacy.team_count_contract_version, 0);
    assert_eq!(legacy.exact_team_count, Some(3));
    assert!(legacy.validate_team_quantity_contract().is_ok());
}

#[test]
fn original_member_error_categories_never_include_error_details() {
    let error = AgentActionServiceError::Corrupt("private action detail".into());
    assert_eq!(error.fixed_kind(), "corrupt");
    assert_eq!(error.fixed_store_kind(), None);
    let error = AgentActionServiceError::Store(RuntimeEventStoreError::UnknownScope("private scope".into()));
    assert_eq!(error.fixed_kind(), "store");
    assert_eq!(error.fixed_store_kind(), Some("unknown_scope"));
    let error = AgentActionServiceError::MemberCancelled;
    assert_eq!(error.fixed_kind(), "member_cancelled");
    assert_eq!(error.fixed_store_kind(), None);
}

#[test]
fn l1_existing_agent_ref_gate_rejects_definition_ref_with_durable_candidates() {
    use crate::execution_core::graph::{
        ExecutionCommitService, ExecutionGraphStateStore, ExecutionNodeBinding,
        NodeExecutionTicket,
    };
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
        ExecutionNodeStatus,
    };
    let store = Arc::new(RuntimeEventStore::for_test());
    let graphs = ExecutionGraphStateStore::new(Arc::clone(&store));
    let service = AgentActionService::new(Arc::clone(&store)).with_graph_store(graphs);
    let mut graph = ExecutionGraph::new("L1 ref gate").with_lineage(ExecutionGraphLineage {
        session_id: "session-1".into(),
        turn_id: "turn-l1-ref".into(),
        root_task_id: "task:l1".into(),
        task_id: "task:l1".into(),
        generation: 1,
    });
    graph.nodes.push(ExecutionNodeSpec {
        id: "l1-node".into(),
        kind: ExecutionNodeKind::InlineModel,
        payload_ref: "payload:l1".into(),
        executor_kind: "l1-test".into(),
        idempotency_key: "l1:attempt".into(),
        lease_ref: None,
        acceptance: Default::default(),
        retry_policy: Default::default(),
        resource_scopes: Vec::new(),
        work: None,
    });
    let commits = ExecutionCommitService::new(Arc::clone(&store));
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(&graph, "l1-node", ExecutionNodeStatus::Ready, None, Vec::new())
        .unwrap()
        .graph;
    let binding = ExecutionNodeBinding {
        executor_kind: "l1-test".into(),
        ticket_idempotency_key: "l1:attempt".into(),
        attempt: 1,
        resource_lease_refs: Vec::new(),
        scope_lease_ref: None,
        worktree_lease_ref: None,
    };
    let graph = commits
        .bind_and_start_nodes(
            &graph,
            std::collections::BTreeMap::from([("l1-node".into(), binding.clone())]),
            vec![NodeExecutionTicket {
                graph_id: graph.id.clone(),
                node_id: "l1-node".into(),
                executor_kind: "l1-test".into(),
                service_class: graph.service_class,
                attempt: 1,
                idempotency_key: "l1:attempt".into(),
                payload_ref: "payload:l1".into(),
            }],
        )
        .unwrap()
        .graph;
    ExecutionGraphStateStore::new(Arc::clone(&store))
        .binding_start_ticket(&graph, "l1-node", &binding)
        .unwrap();
    let objective = harness_contract::agent_action::root_objective_id("session-1", "turn-l1-ref");
    let program = harness_contract::agent_action::program_id_for_objective(&objective);
    let l1 = |action_id: &str, action| {
        let mut envelope = root_in_turn("turn-l1-ref", action_id, action);
        envelope.actor.actor_id = "root:session-1".into();
        envelope.actor.objective_id = objective.clone();
        envelope.actor.program_id = program.clone();
        envelope.actor.root_execution_id = Some(graph.id.clone());
        envelope.actor.original_execution = Some(
            harness_contract::agent_action::OriginalAgentExecution {
                graph_id: graph.id.clone(),
                node_id: "l1-node".into(),
                attempt: 1,
            },
        );
        envelope
    };
    let team = service
        .apply(&l1(
            "l1-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Builders".into(),
                mission: "build".into(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let mut durable = Vec::new();
    for index in 0..2 {
        let input: AgentInviteInput = serde_json::from_value(json!({
            "team_ref": team, "role": "Builder", "mission": "build",
            "required_capabilities": ["read"]
        }))
        .unwrap();
        durable.push(
            service
                .apply(&l1(
                    &format!("l1-agent-{index}"),
                    AgentAction::AgentInvite(input),
                ))
                .expect("invite")
                .changed_refs[0]
                .clone(),
        );
    }
    let before = service.project_snapshot(&program).unwrap().revision;
    let placeholder: AgentInviteInput = serde_json::from_value(json!({
        "team_ref": team, "role": "Reuse", "mission": "build",
        "required_capabilities": ["read"],
        "existing_agent_ref": "builtin/cowd/autonomous"
    }))
    .unwrap();
    let denied = service
        .apply(&l1("l1-placeholder", AgentAction::AgentInvite(placeholder)))
        .expect("placeholder invite")
        .clone();
    assert_eq!(denied.status, AgentActionStatus::Rejected);
    let error = denied.error.expect("the refusal is named");
    assert_eq!(error.code, "unknown_existing_agent_ref");
    assert!(
        error.message.contains(&durable[0]) && error.message.contains(&durable[1]),
        "{error:?}"
    );
    assert!(
        error.message.contains("Definition ids, not invite references"),
        "{error:?}"
    );
    assert_eq!(service.project_snapshot(&program).unwrap().revision, before);
    // L1b (e2 ②): a blank-but-present ref is refused by name too, not silently
    // converted into a create-new invite.
    let blank: AgentInviteInput = serde_json::from_value(json!({
        "team_ref": team, "role": "Reuse", "mission": "build",
        "required_capabilities": ["read"],
        "existing_agent_ref": "   "
    }))
    .unwrap();
    let blank_denied = service
        .apply(&l1("l1-blank", AgentAction::AgentInvite(blank)))
        .expect("blank invite");
    assert_eq!(blank_denied.status, AgentActionStatus::Rejected);
    assert_eq!(
        blank_denied.error.expect("named").code,
        "unknown_existing_agent_ref"
    );
    assert_eq!(service.project_snapshot(&program).unwrap().revision, before);
    let reuse: AgentInviteInput = serde_json::from_value(json!({
        "team_ref": team, "role": "Reuse", "mission": "build",
        "required_capabilities": ["read"],
        "existing_agent_ref": durable[0]
    }))
    .unwrap();
    let accepted = service
        .apply(&l1("l1-reuse", AgentAction::AgentInvite(reuse)))
        .expect("reuse invite");
    assert_eq!(accepted.status, AgentActionStatus::Applied);
    // The refusal removed the poison: no `builtin/…` key can enter the
    // projection, so a later publish can never block on it as an effect.
    let projection = service.project_snapshot(&program).unwrap();
    assert!(!projection.agents.keys().any(|key| key.starts_with("builtin/")));
}

/// 033 F2 kernel control: which gate does a review-role action die at?
///
/// The B-family common death is claimed to be the durable path's
/// physical-execution binding gate ("new root action requires original
/// physical execution", action_service.rs:3596 / agentic/ingress.rs:723).
/// This walks the three actor shapes one review action can have and names
/// the gate each shape dies at.
///
/// (a) Root without the original physical execution dies exactly at the
///     physical-execution gate.
/// (b) Agent without it dies at a *different* gate (the agent-identity
///     gate, action_service.rs:3600). The physical-execution gate never
///     fires for a delegated actor.
/// (c) Agent *with* its own physical binding - the shape a review turn
///     really has, because the delegated invocation is issued with
///     `original_execution: Some(..)` (agentic/action_invocation.rs:687)
///     - passes both gates and reaches the physical graph binding stage.
///
/// So the physical-execution gate cannot be the common death cause of a
/// review path whose final batch is bound to its own child execution.
#[test]
fn f2_the_physical_binding_gate_fires_only_for_root_without_physical() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(store);
    let review = AgentAction::TaskReview(TaskReviewInput {
        task_ref: "task".into(),
        decision: TaskReviewDecision::Rework,
        reason: "the answer does not cite the declared criterion".into(),
        operator_requirement: None,
        evidence_refs: Vec::new(),
        gaps: Vec::new(),
        directed_actions: Vec::new(),
    });

    // (a) The Root shape without the physical binding, performing the
    // physical effect the gate fences. 033 S3 narrowed that gate to the
    // actions that cause a physical effect (a Root action that only writes
    // program state names the Program instead), so this arm uses the
    // physical effect to reach the same gate.
    let root_effect = root(
        "f2-root-effect",
        AgentAction::ArtifactCommit(ArtifactCommitInput {
            content_ref: "artifact://art-f2".into(),
            kind: "report".into(),
            title: "report".into(),
            relates_to: Vec::new(),
        }),
    );
    let error = service.apply(&root_effect).unwrap_err().to_string();
    assert!(
        error.contains("new root action requires original physical execution"),
        "the Root shape without the physical binding dies at the gate: {error}"
    );

    // (b) The delegated shape without the physical binding dies at the
    // agent-identity gate, never at the physical-execution gate.
    let child_review = managed(
        "f2-child-review",
        "team-f2",
        "agent:worker",
        review.clone(),
    );
    let error = service.apply(&child_review).unwrap_err().to_string();
    assert!(
        !error.contains("new root action requires original physical execution"),
        "the delegated shape never dies at the physical-execution gate: {error}"
    );
    assert!(
        error.contains("agent actor original execution required for new action"),
        "the delegated shape without the physical binding dies at the agent-identity gate: {error}"
    );

    // (c) The delegated shape *with* its own physical binding: the review
    // path's real shape. It clears the physical-execution gate and the
    // agent-identity gate and reaches the physical graph binding stage.
    let mut bound_review = child_review.clone();
    bound_review.actor.original_execution =
        Some(harness_contract::agent_action::OriginalAgentExecution {
            graph_id: "graph-f2".into(),
            node_id: "node-f2".into(),
            attempt: 1,
        });
    let error = service.apply(&bound_review).unwrap_err().to_string();
    assert!(
        !error.contains("new root action requires original physical execution"),
        "the child-bound review shape clears the physical-execution gate: {error}"
    );
    assert!(
        !error.contains("agent actor original execution required for new action"),
        "the child-bound review shape clears the agent-identity gate: {error}"
    );
    assert!(
        error.contains("agent actor graph store unavailable"),
        "the child-bound review shape reaches the physical graph binding stage: {error}"
    );
}

// Synthetic journal fold control; the original action authorization is tested separately.
#[test]
fn gb3_p2_journal_fold_restores_missing_rounds_without_reclassifying_legacy_events() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let stream = program_stream("program-1");
    let append = |expected: u64, kind: &str, payload: serde_json::Value| {
        store.append_transaction(crate::AppendTransactionRequest {
            transaction_id: format!("gb3-replay-{expected}"),
            expected_streams: vec![crate::ExpectedStreamRevision { stream_id: stream.clone(), expected_revision: expected }],
            events: vec![crate::RuntimeTransactionEventInput {
                event: RuntimeEventInput { stream_id: stream.clone(), scope: RuntimeEventScope::Program,
                    kind: kind.into(), status: Some("applied".into()), actor: Some("runtime-test".into()), refs: Vec::new(), payload },
                idempotency_key: Some(format!("gb3-replay-{expected}")), schema_version: 1,
            }],
        }).unwrap();
    };
    append(0, PROGRAM_OPENED_EVENT_KIND, json!({"program_id":"program-1", "objective_id":"objective-1", "session_id":"session-1", "turn_id":"turn-1"}));
    let publish = root("gb3-publish", AgentAction::TaskPublish(TaskPublishInput {
        team_ref:"team".into(), title:"task".into(), objective:"read".into(), acceptance:"cite".into(),
        criterion_source:Default::default(), acceptance_checks:Vec::new(), required_capabilities:Vec::new(),
        depends_on:Vec::new(), obligation_refs:Vec::new(), purpose:Default::default(), execution_requirements:Vec::new(), expertise_hints:Vec::new(),
    }));
    append(1, ACTION_EVENT_KIND, json!({"envelope":publish, "entity_ref":"task"}));
    let service = AgentActionService::new(Arc::clone(&store));
    let projection = service.project_snapshot("program-1").unwrap();
    assert_eq!(projection.applied_rework_rounds.as_ref().unwrap()["task"], 0);
    let mut legacy = (*projection).clone();
    legacy.applied_rework_rounds = None;
    for n in 0..2 {
        let fail = root(&format!("gb3-fail-{n}"), AgentAction::TaskAttemptFail(TaskAttemptFailInput {
            task_ref:"task".into(), execution_id:format!("graph-{n}"), mode:harness_contract::agent_action::AgentAttemptMode::Review,
            reason:format!("proof_actual_input_extent_unsupported:{n}"), retryable:true,
        }));
        append(2+n, ACTION_EVENT_KIND, json!({"envelope":fail}));
    }
    let review = root("gb3-review", AgentAction::TaskReview(TaskReviewInput {
        task_ref:"task".into(), decision:TaskReviewDecision::Rework, reason:"missing fact".into(),
        operator_requirement:None, evidence_refs:Vec::new(), gaps:Vec::new(), directed_actions:Vec::new(),
    }));
    append(4, ACTION_EVENT_KIND, json!({"envelope":review, "review_round_rule":"verdict_v1"}));
    let events = store.list_stream_after(&stream, 2, 5, 100, 1024*1024).unwrap();
    apply_projection_events(&store, &mut legacy, events).unwrap();
    assert_eq!(legacy.tasks["task"].review_generation, 3);
    assert_eq!(legacy.tasks["task"].status, AgenticTaskStatus::Rework);
    assert_eq!(legacy.applied_rework_rounds.as_ref().unwrap()["task"], 1);
    let cold = AgentActionService::new(Arc::clone(&store)).project("program-1").unwrap();
    assert_eq!(cold.tasks["task"].review_generation, legacy.tasks["task"].review_generation);
    assert_eq!(cold.tasks["task"].status, legacy.tasks["task"].status);
    assert_eq!(cold.applied_rework_rounds, legacy.applied_rework_rounds);
}

// Synthetic event-fold control; a real action/legacy checkpoint test is still required.
#[test]
fn gb3_p2_accept_does_not_require_missing_legacy_rework_history() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let mut projection = AgenticProgramProjection::empty("program-1", "objective-1");
    let publish = TaskPublishInput { team_ref:"team".into(), title:"task".into(), objective:"read".into(), acceptance:"cite".into(),
        criterion_source:Default::default(), acceptance_checks:Vec::new(), required_capabilities:Vec::new(), depends_on:Vec::new(),
        obligation_refs:Vec::new(), purpose:Default::default(), execution_requirements:Vec::new(), expertise_hints:Vec::new() };
    let env = root("gb3-accept-publish", AgentAction::TaskPublish(publish.clone()));
    crate::agentic::work_market::apply_task_publish(&mut projection, &env, &publish, Some("task"));
    projection.applied_rework_rounds = None;
    let review = root("gb3-accept", AgentAction::TaskReview(TaskReviewInput { task_ref:"task".into(),
        decision:TaskReviewDecision::Accept, reason:"done".into(), operator_requirement:None,
        evidence_refs:Vec::new(), gaps:Vec::new(), directed_actions:Vec::new() }));
    let stream = program_stream("program-1");
    store.append_transaction(crate::AppendTransactionRequest { transaction_id:"gb3-accept-test".into(),
        expected_streams:vec![crate::ExpectedStreamRevision{stream_id:stream.clone(), expected_revision:0}],
        events:vec![crate::RuntimeTransactionEventInput { event:RuntimeEventInput { stream_id:stream.clone(),
            scope:RuntimeEventScope::Program, kind:ACTION_EVENT_KIND.into(), status:Some("applied".into()),
            actor:Some("runtime-test".into()), refs:Vec::new(), payload:json!({"envelope":review}) },
            idempotency_key:Some("gb3-accept-test".into()), schema_version:1 }] }).unwrap();
    let events = store.list_stream_after(&stream, 0, 1, 100, 1024*1024).unwrap();
    apply_projection_events(&store, &mut projection, events).unwrap();
    assert_eq!(projection.tasks["task"].status, AgenticTaskStatus::Accepted);
    assert!(projection.applied_rework_rounds.is_none());
    assert_eq!(store.stream_revision(&stream).unwrap(), 1);
}

// Storage-level continuation replay control; not a substitute for the original action path.
#[test]
fn gb3_p2_missing_continuation_source_is_unknown_and_recovers_without_rewriting_seed() {
    use crate::agentic::program::{AgenticProgramContinuation, AgenticProgramStatus};
    let store = RuntimeEventStore::for_test();
    let mut seed = AgenticProgramProjection::empty("child", "objective");
    seed.applied_rework_rounds = None;
    seed.continuation = Some(AgenticProgramContinuation {
        source_program_id: "parent".into(), source_objective_id: "objective".into(),
        source_root_id: "root".into(), source_revision: 3, source_status: AgenticProgramStatus::Open,
        authorization_generation: 1, authorization_revision: 1, binding_digest: "binding".into(),
    });
    let publish: TaskPublishInput = serde_json::from_value(json!({
        "team_ref":"team", "title":"task", "objective":"read", "acceptance":"cite", "required_capabilities":[]
    })).unwrap();
    let published = root("gb3-parent-publish", AgentAction::TaskPublish(publish.clone()));
    crate::agentic::work_market::apply_task_publish(&mut seed, &published, &publish, Some("task"));
    let append_open = |program: &str, seed: Option<serde_json::Value>| {
        let stream = program_stream(program);
        let mut payload = json!({"program_id":program, "objective_id":"objective", "session_id":"session", "turn_id":"turn"});
        if let Some(seed) = seed { payload["continuation_seed"] = seed; }
        store.append_transaction(crate::AppendTransactionRequest {
            transaction_id: format!("gb3-open-{program}"),
            expected_streams: vec![crate::ExpectedStreamRevision { stream_id: stream.clone(), expected_revision: 0 }],
            events: vec![crate::RuntimeTransactionEventInput {
                event: RuntimeEventInput { stream_id: stream, scope: RuntimeEventScope::Program,
                    kind: PROGRAM_OPENED_EVENT_KIND.into(), status: Some("open".into()),
                    actor: Some("test".into()), refs: Vec::new(), payload },
                idempotency_key: Some(format!("gb3-open-{program}")), schema_version: 1,
            }],
        }).unwrap();
    };
    append_open("child", Some(serde_json::to_value(&seed).unwrap()));
    let before = store.list_stream(&program_stream("child")).unwrap();
    assert!(matches!(rebuild_rework_rounds(&store, "child", 1, &mut Vec::new()),
        Err(AgentActionServiceError::MemberUnknown(_))));
    assert_eq!(store.stream_revision(&program_stream("child")).unwrap(), 1);
    append_open("parent", None);
    let review = root("gb3-parent-review", AgentAction::TaskReview(TaskReviewInput {
        task_ref: "task".into(), decision: TaskReviewDecision::Rework, reason: "missing fact".into(),
        operator_requirement: None, evidence_refs: Vec::new(), gaps: Vec::new(), directed_actions: Vec::new(),
    }));
    for (expected, envelope, entity) in [(1, published, Some("task")), (2, review, None)] {
        let stream = program_stream("parent");
        store.append_transaction(crate::AppendTransactionRequest {
            transaction_id: format!("gb3-parent-{expected}"),
            expected_streams: vec![crate::ExpectedStreamRevision { stream_id: stream.clone(), expected_revision: expected }],
            events: vec![crate::RuntimeTransactionEventInput {
                event: RuntimeEventInput { stream_id: stream, scope: RuntimeEventScope::Program,
                    kind: ACTION_EVENT_KIND.into(), status: Some("applied".into()), actor: Some("test".into()),
                    refs: Vec::new(), payload: json!({"envelope":envelope, "entity_ref":entity}) },
                idempotency_key: Some(format!("gb3-parent-{expected}")), schema_version: 1,
            }],
        }).unwrap();
    }
    assert_eq!(rebuild_rework_rounds(&store, "child", 1, &mut Vec::new()).unwrap()["task"], 1);
    let after = store.list_stream(&program_stream("child")).unwrap();
    assert_eq!(serde_json::to_value(before).unwrap(), serde_json::to_value(after).unwrap());
}
