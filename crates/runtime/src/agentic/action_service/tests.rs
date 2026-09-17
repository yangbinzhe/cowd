use harness_contract::agent_action::{
    AgentAction, AgentActionEnvelope, AgentActorBinding, AgentActorKind, AgentInviteInput,
    ArtifactCommitInput, MessagePublishInput, ObjectiveCompleteRequestInput, StateInspectInput,
    TaskAttemptFailInput, TaskClaimInput, TaskPublishInput, TaskReviewDecision, TaskReviewInput,
    TaskSubmitInput, TaskSupersedeInput, TeamCreateInput,
};
use harness_contract::goal::{
    AcceptanceCriterion, AcceptanceStatus, GoalCompletion, GoalContract, ObjectiveTerminal,
    ObjectiveTerminalKind,
};

use super::*;

fn root(action_id: &str, action: AgentAction) -> AgentActionEnvelope {
    AgentActionEnvelope {
        action_id: action_id.to_string(),
        actor: AgentActorBinding {
            objective_id: "objective-1".to_string(),
            program_id: "program-1".to_string(),
            session_id: "session-1".to_string(),
            turn_id: "turn-1".to_string(),
            root_execution_id: Some("root-execution-1".to_string()),
            required_team_count: 1,
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
            objective_id: "objective-1".to_string(),
            program_id: "program-1".to_string(),
            session_id: "session-1".to_string(),
            turn_id: "turn-1".to_string(),
            root_execution_id: Some("root-execution-1".to_string()),
            required_team_count: 1,
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
    let recovered = AgentActionService::new(store);
    let mut retry = action.clone();
    retry.expected_revision = Some(first.revision);
    let replay = recovered.apply(&retry).unwrap();
    assert!(replay.duplicate);
    assert_eq!(replay.changed_refs, first.changed_refs);
    for changed_actor in [false, true] {
        let mut conflict = action.clone();
        if changed_actor {
            conflict.actor.actor_id = "different-root".into();
        } else if let AgentAction::TeamCreate(input) = &mut conflict.action {
            input.mission = "different work".into();
        }
        let denied = recovered.apply(&conflict).unwrap();
        assert_eq!(denied.status, AgentActionStatus::Rejected);
        assert_eq!(denied.error.unwrap().code, "action_id_conflict");
        assert_eq!(
            recovered.project("program-1").unwrap().revision,
            first.revision
        );
    }
    let mut independent = action;
    independent.action_id = "independent-new-action".into();
    let second = recovered.apply(&independent).unwrap();
    assert_eq!(second.status, AgentActionStatus::Applied);
    assert!(!second.duplicate);
    assert_ne!(second.changed_refs, first.changed_refs);
    assert_eq!(recovered.project("program-1").unwrap().teams.len(), 2);
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
    for index in 0..20 {
        assert_eq!(
            service
                .apply(&root(
                    &format!("budget-team-{index}"),
                    AgentAction::TeamCreate(TeamCreateInput {
                        name: format!("Budget team {index}"),
                        mission: "preserve canonical facts while releasing derived cache memory"
                            .into(),
                        objective: None,
                    })
                ))
                .unwrap()
                .status,
            AgentActionStatus::Applied
        );
    }
    let expected = service.project("program-1").unwrap();
    assert_eq!(expected.teams.len(), 20);
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
    assert_eq!(cache.len(), 1);
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
    let store = Arc::new(RuntimeEventStore::for_test());
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
            "body" => payload["projection"]["objective_summary"] = json!("forged valid JSON"),
            "schema" => payload["schema_version"] = json!(999),
            "digest" => payload["sha256"] = json!("sha256:wrong"),
            "legacy" => payload = payload["projection"].clone(),
            "ahead" => {
                source_cursor += 10;
                payload["projection"]["revision"] = json!(source_cursor);
                payload["sha256"] = json!(format!(
                    "sha256:{:x}",
                    Sha256::digest(payload["projection"].to_string().as_bytes())
                ));
            }
            "misbound" => {
                payload["projection"]["program_id"] = json!("other-program");
                payload["sha256"] = json!(format!(
                    "sha256:{:x}",
                    Sha256::digest(payload["projection"].to_string().as_bytes())
                ));
            }
            _ => unreachable!(),
        }
        store
            .put_projection_checkpoint(&checkpoint_id, source_cursor, &payload, now_ms())
            .unwrap();
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
            decode_read_snapshot(&repaired, "program-1").is_some(),
            "{case}"
        );
        assert!(matches!(
            store.compare_and_repair_projection_checkpoint(
                &checkpoint_id,
                0,
                repaired.revision - 1,
                &json!(null),
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
    first
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
    assert_eq!(read_model.len(), 1);

    let second = AgentActionService::new(store).with_read_model(Arc::clone(&read_model));
    second
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
    assert_eq!(after.teams.len(), 2);
    assert_eq!(before.teams.len(), 1, "a published snapshot is immutable");
    assert!(!Arc::ptr_eq(&before, &after));
    assert!(Arc::ptr_eq(
        &after,
        &first.project_snapshot("program-1").unwrap()
    ));
    assert_eq!(read_model.len(), 1);
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
    assert_eq!(cross_page.entries.len(), 1);
    assert_eq!(
        cross_page.entries[0].entry.summary.as_deref(),
        Some("public cross-Team finding")
    );
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
    assert_eq!(own_team_page.entries.len(), 2);
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
    assert_eq!(seen.len(), 38);
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

#[test]
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
    assert_eq!(focused["entries"].as_array().unwrap().len(), 1);
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
fn legacy_goal_terminal_recovery_is_durable_and_idempotent() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = AgentActionService::new(Arc::clone(&store));
    let goals = Arc::new(crate::execution_core::goal::GoalStore::new(Arc::clone(
        &store,
    )));
    goals
        .create(GoalContract {
            id: "goal:root-execution-1".to_string(),
            session_id: "session-1".to_string(),
            objective: "test objective".to_string(),
            criteria: vec![AcceptanceCriterion {
                id: "terminal_synthesis".to_string(),
                statement: "produce one durable terminal synthesis".to_string(),
                statement_ref: None,
                source_refs: Vec::new(),
                required_evidence: vec!["execution_graph:root-execution-1".to_string()],
                status: AcceptanceStatus::Open,
                waiver: None,
            }],
            constraints: Vec::new(),
            phase: "execution".to_string(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            scope: harness_contract::goal::GoalScope::UserObjective,
            user_intent_criterion_id: Some("terminal_synthesis".to_string()),
            source_intent_ref: Some("session_message:test".to_string()),
            execution_binding: Some(harness_contract::goal::GoalExecutionBinding {
                objective_id: "objective-1".to_string(),
                session_id: "session-1".to_string(),
                turn_id: "turn-1".to_string(),
                root_execution_id: "root-execution-1".to_string(),
                agentic_program_id: "program-1".to_string(),
            }),
            spec_revision: 1,
            spec_digest: "action-service-test".to_string(),
            review_refs: Vec::new(),
            waiting: None,
            participation_requirement: None,
            obligations: Vec::new(),
            recovery: None,
            terminal: None,
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
            reviews: Vec::new(),
        })
        .expect("goal");

    let team_receipt = service
        .apply(&root(
            "create-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Research".to_string(),
                mission: "Investigate evidence".to_string(),
                objective: None,
            }),
        ))
        .expect("team");
    let team_id = team_receipt.changed_refs[0].clone();
    let duplicate = service
        .apply(&root(
            "create-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Research".to_string(),
                mission: "Investigate evidence".to_string(),
                objective: None,
            }),
        ))
        .expect("duplicate");
    assert_eq!(duplicate.revision, team_receipt.revision);

    let agent_receipt = service
        .apply(&root(
            "invite-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team_id.clone(),
                role: "researcher".to_string(),
                mission: "produce evidence".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("agent");
    let agent_id = agent_receipt.changed_refs[0].clone();
    let reviewer_receipt = service
        .apply(&root(
            "invite-reviewer",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team_id.clone(),
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
        .expect("reviewer");
    let reviewer_id = reviewer_receipt.changed_refs[0].clone();
    let task_receipt = service
        .apply(&root(
            "publish-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team_id.clone(),
                title: "Research".to_string(),
                objective: "Read source".to_string(),
                acceptance: "artifact and evidence".to_string(),
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
    let task_id = task_receipt.changed_refs[0].clone();
    service
        .apply(&managed(
            "claim-task",
            &team_id,
            &agent_id,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task_id.clone(),
                reason: None,
            }),
        ))
        .expect("claim");
    let artifact_receipt = service
        .apply(&managed(
            "commit-artifact",
            &team_id,
            &agent_id,
            AgentAction::ArtifactCommit(ArtifactCommitInput {
                content_ref: "artifact://abc".to_string(),
                kind: "research".to_string(),
                title: "Findings".to_string(),
                relates_to: Vec::new(),
            }),
        ))
        .expect("artifact");
    let artifact_ref = artifact_receipt.changed_refs[0].clone();
    assert_eq!(
        service
            .project("program-1")
            .expect("artifact projection")
            .artifacts[&artifact_ref]
            .relates_to,
        vec![task_id.clone()],
        "Runtime must derive the current Task relation from the attested execute binding"
    );
    service
        .apply(&managed(
            "submit-task",
            &team_id,
            &agent_id,
            AgentAction::TaskSubmit(TaskSubmitInput {
                task_ref: task_id.clone(),
                artifact_refs: vec![artifact_ref.clone()],
                evidence_refs: vec!["tool://source-observation".to_string()],
                unresolved: Vec::new(),
            }),
        ))
        .expect("submit");
    let submitted = service.project("program-1").expect("submitted projection");
    assert_eq!(
        submitted.tasks[&task_id].evidence_refs,
        vec![
            "artifact://abc".to_string(),
            "tool://source-observation".to_string(),
        ],
        "Runtime attaches committed artifact content without making the model duplicate it"
    );
    service
        .apply(&managed(
            "review-task",
            &team_id,
            &reviewer_id,
            AgentAction::TaskReview(TaskReviewInput {
                task_ref: task_id.clone(),
                decision: TaskReviewDecision::Accept,
                reason: "verified".to_string(),
                evidence_refs: vec!["tool://independent-inspection".to_string()],
            }),
        ))
        .expect("review");
    let delegated_completion = service
        .apply(&managed(
            "delegated-complete",
            &team_id,
            &reviewer_id,
            AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                result_refs: vec![artifact_ref.clone()],
                evidence_refs: vec!["artifact://abc".to_string()],
                unresolved: Vec::new(),
            }),
        ))
        .expect("delegated completion rejection");
    assert_eq!(delegated_completion.status, AgentActionStatus::Rejected);
    assert_eq!(
        delegated_completion.error.expect("error").code,
        "objective_completion_not_delegated"
    );
    let complete = service
        .apply(&root(
            "complete",
            AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                result_refs: vec![artifact_ref],
                evidence_refs: vec!["artifact://abc".to_string()],
                unresolved: Vec::new(),
            }),
        ))
        .expect("complete");
    assert_eq!(complete.status, AgentActionStatus::Applied, "{complete:?}");
    assert_eq!(
        service.project("program-1").expect("projection").status,
        super::super::program::AgenticProgramStatus::CompletionRequested
    );
    let pending = service.project("program-1").expect("projection");
    assert!(pending.objective_verdict.is_none());
    let objective_supervisor =
        crate::execution_core::goal::ObjectiveSupervisor::new(Arc::clone(&goals));
    let request_revision = pending
        .completion_request
        .as_ref()
        .expect("completion request")
        .program_revision;
    let mut foreign_terminal = goals
        .get("goal:root-execution-1")
        .expect("goal read")
        .expect("goal");
    foreign_terminal.completion = GoalCompletion::Satisfied;
    foreign_terminal.evidence_refs = vec![
        "artifact://abc".to_string(),
        "execution_graph:root-execution-1".to_string(),
    ];
    foreign_terminal.terminal = Some(ObjectiveTerminal {
        kind: ObjectiveTerminalKind::Satisfied,
        terminal_fence: "foreign-presentation-terminal".to_string(),
        authority_revision: request_revision,
        reason: "fault injection: presentation raced Agentic supervision".to_string(),
        evidence_refs: foreign_terminal.evidence_refs.clone(),
        diagnostics: Vec::new(),
        committed_at_ms: 1,
    });
    assert!(matches!(
        service.bind_objective_verdict("program-1", &foreign_terminal),
        Err(AgentActionServiceError::Corrupt(message))
            if message == "objective_verdict_binding_mismatch"
    ));
    assert_eq!(
        service.project("program-1").expect("projection").status,
        crate::AgenticProgramStatus::CompletionRequested,
        "a foreign terminal fence must never verify the Program"
    );
    let rejected = objective_supervisor
        .reconcile(
            "goal:root-execution-1",
            request_revision,
            &format!("agentic-objective:program-1:request:{request_revision}"),
            vec![],
            vec![],
            vec![],
            false,
            "ordinary writer cannot split a new Program conclusion",
        )
        .unwrap_err();
    assert!(rejected.contains("program_terminal_authority"));
    // Historical journal fault injection only. Production now commits Goal
    // and Program atomically; it cannot create this pre-existing split state.
    let mut historical = foreign_terminal;
    historical.terminal.as_mut().unwrap().terminal_fence =
        format!("agentic-objective:program-1:request:{request_revision}");
    historical.terminal.as_mut().unwrap().reason = "historical terminal awaiting recovery".into();
    historical.phase = "completed".into();
    historical.revision += 1;
    historical.criteria[0].status = AcceptanceStatus::Satisfied;
    store
        .append(crate::RuntimeEventInput {
            stream_id: "goal:goal:root-execution-1".into(),
            scope: crate::RuntimeEventScope::Goal,
            kind: "goal.completed".into(),
            status: Some("satisfied".into()),
            actor: Some("test.historical_fault_injection".into()),
            refs: vec![],
            payload: serde_json::json!({"goal":historical}),
        })
        .unwrap();
    assert_eq!(
        service.project("program-1").expect("projection").status,
        crate::AgenticProgramStatus::CompletionRequested,
        "Goal terminal alone must not mutate Program projection"
    );
    let verified = crate::agentic::supervision::reconcile_completion_request(
        &service,
        &objective_supervisor,
        "program-1",
    )
    .expect("Objective supervision")
    .expect("completion request");
    assert_eq!(verified.status, crate::AgenticProgramStatus::Verified);
    assert!(verified.objective_verdict.is_some());
    assert_eq!(
        goals
            .get("goal:root-execution-1")
            .expect("goal projection")
            .expect("goal")
            .completion,
        GoalCompletion::Satisfied
    );
    let replayed = crate::agentic::supervision::reconcile_completion_request(
        &service,
        &objective_supervisor,
        "program-1",
    )
    .expect("replay Objective supervision")
    .expect("verified Program");
    assert_eq!(replayed.revision, verified.revision);
    assert_eq!(replayed.objective_verdict, verified.objective_verdict);
    let terminal_mutation = service
        .apply(&root(
            "late-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Late".to_string(),
                mission: "must not reopen verified truth".to_string(),
                objective: None,
            }),
        ))
        .expect("terminal rejection");
    assert_eq!(terminal_mutation.status, AgentActionStatus::Rejected);
    assert_eq!(
        terminal_mutation.error.expect("error").code,
        "program_terminal"
    );
}

#[tokio::test]
async fn production_artifact_authority_closes_submit_review_and_completion_chain() {
    let temporary = tempfile::tempdir().expect("artifact root");
    let artifacts = Arc::new(crate::ArtifactStore::for_test_default(temporary.path()));
    let content = artifacts
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/markdown".to_string(),
                visibility_scope: "session:session-1".to_string(),
                expected_bytes: None,
                original_name: Some("verified-report.md".to_string()),
            },
            b"verified production evidence",
        )
        .await
        .expect("durable content");
    let service = AgentActionService::new(Arc::new(RuntimeEventStore::for_test()))
        .with_artifact_store(artifacts);
    let team = service
        .apply(&root(
            "durable-team",
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
            "durable-worker",
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
            "durable-reviewer",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "reviewer".to_string(),
                mission: "verify the report".to_string(),
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
            "durable-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Verified report".to_string(),
                objective: "produce a grounded report".to_string(),
                acceptance: "durable report and independent review".to_string(),
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
        .apply(&managed(
            "durable-claim",
            &team,
            &worker,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task.clone(),
                reason: None,
            }),
        ))
        .expect("claim");
    let artifact_receipt = service
        .apply(&managed(
            "durable-commit",
            &team,
            &worker,
            AgentAction::ArtifactCommit(ArtifactCommitInput {
                content_ref: content.selector.clone(),
                kind: "final_report".to_string(),
                title: "Verified report".to_string(),
                relates_to: vec![task.clone()],
            }),
        ))
        .expect("commit");
    let artifact = artifact_receipt.changed_refs[0].clone();
    let access = &artifact_receipt.projection.as_ref().unwrap()["artifact_access"];
    assert_eq!(access["submit_ref"], artifact);
    assert_eq!(access["read_request"]["evidence_ref"], content.selector);
    let inspect = AgentActionService::compact_projection(
        &service.project("program-1").unwrap(),
        Some(&artifact),
    );
    assert_eq!(inspect["read_request"]["evidence_ref"], content.selector);
    let durable_submit = service
        .apply(&managed(
            "durable-submit",
            &team,
            &worker,
            AgentAction::TaskSubmit(TaskSubmitInput {
                task_ref: task.clone(),
                artifact_refs: vec![artifact.clone()],
                evidence_refs: vec![content.selector.clone()],
                unresolved: vec![
                    "Known limitation disclosed in the reviewed artifact; not an acceptance blocker"
                        .to_string(),
                ],
            }),
        ))
        .expect("submit");
    assert_eq!(
        durable_submit.status,
        AgentActionStatus::Applied,
        "{durable_submit:?}"
    );
    assert_eq!(
        service
            .apply(&managed(
                "durable-review",
                &team,
                &reviewer,
                AgentAction::TaskReview(TaskReviewInput {
                    task_ref: task.clone(),
                    decision: TaskReviewDecision::Accept,
                    reason: "content resolved and acceptance was met".to_string(),
                    evidence_refs: vec![content.selector.clone()],
                }),
            ))
            .expect("review")
            .status,
        AgentActionStatus::Applied
    );
    assert_eq!(
        service
            .apply(&root(
                "blocked-objective-complete",
                AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                    result_refs: vec![artifact.clone()],
                    evidence_refs: vec![content.selector.clone()],
                    unresolved: vec!["Objective-level delivery blocker".to_string()],
                }),
            ))
            .expect("objective blocker rejection")
            .status,
        AgentActionStatus::Rejected,
        "objective-level blockers remain authoritative even after Task acceptance"
    );
    let before = service.project("program-1").unwrap();
    let issue = super::super::issues::issues(&before).remove(0);
    assert!(super::super::issues::completion_gap(&before)
        .unwrap()
        .starts_with("issue_requires_classification:"));
    let classify = |action_id: &str, disposition| {
        root(
            action_id,
            AgentAction::MessagePublish(MessagePublishInput {
                topic_ref: "topic:program-1".into(),
                summary: Some("Adjudicate the retained limitation".into()),
                content_ref: None,
                refs: Vec::new(),
                recipients: Vec::new(),
                intent: None,
                issue_dispositions: vec![harness_contract::agent_action::IssueDisposition {
                    issue_ref: issue.issue_ref.clone(),
                    disposition,
                    reason_ref: content.selector.clone(),
                    evidence_refs: vec![content.selector.clone()],
                }],
            }),
        )
    };
    let mut forged = classify(
        "forged-source",
        harness_contract::agent_action::IssueDispositionKind::Disclose,
    );
    if let AgentAction::MessagePublish(input) = &mut forged.action {
        input.issue_dispositions[0].issue_ref = "issue:missing".into();
    }
    assert_eq!(
        service.apply(&forged).unwrap().status,
        AgentActionStatus::Rejected
    );
    let mut unprivileged = classify(
        "member-cannot-adjudicate",
        harness_contract::agent_action::IssueDispositionKind::Disclose,
    );
    unprivileged.actor = managed("member", &team, &reviewer, unprivileged.action.clone()).actor;
    assert_eq!(
        service.apply(&unprivileged).unwrap().status,
        AgentActionStatus::Rejected
    );
    assert_eq!(
        service
            .apply(&classify(
                "must-resolve",
                harness_contract::agent_action::IssueDispositionKind::MustResolve
            ))
            .unwrap()
            .status,
        AgentActionStatus::Applied
    );
    assert!(
        super::super::issues::completion_gap(&service.project("program-1").unwrap())
            .unwrap()
            .starts_with("issue_must_resolve:")
    );
    assert_eq!(
        service
            .apply(&classify(
                "allow-disclosure",
                harness_contract::agent_action::IssueDispositionKind::Disclose
            ))
            .unwrap()
            .status,
        AgentActionStatus::Applied
    );
    let adjudicated = service.project("program-1").unwrap();
    assert!(super::super::issues::completion_gap(&adjudicated).is_none());
    assert_eq!(adjudicated.tasks[&task].status, AgenticTaskStatus::Accepted);
    assert_eq!(
        adjudicated.tasks[&task].unresolved,
        before.tasks[&task].unresolved
    );
    let mut changed = adjudicated.clone();
    changed.tasks.get_mut(&task).unwrap().unresolved[0].push_str(" revised source");
    assert!(super::super::issues::completion_gap(&changed)
        .unwrap()
        .starts_with("issue_requires_classification:"));
    let restarted = AgentActionService::new(Arc::clone(&service.store));
    let recovered =
        super::super::issues::issues(&restarted.project("program-1").unwrap()).remove(0);
    assert_eq!(recovered.adjudicated_by.as_deref(), Some("root-1"));
    assert_eq!(recovered.disposition.unwrap().reason_ref, content.selector);
    assert_eq!(
        service
            .apply(&root(
                "durable-complete",
                AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                    result_refs: vec![artifact],
                    evidence_refs: vec![content.selector],
                    unresolved: Vec::new(),
                }),
            ))
            .expect("completion")
            .status,
        AgentActionStatus::Applied,
        "accepted work and evidence-backed disclosure survive without repeating the task"
    );
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
    for worker in workers {
        assert_eq!(
            worker.join().expect("worker").status,
            AgentActionStatus::Applied
        );
    }
    let projection = service.project("program-1").expect("projection");
    assert_eq!(projection.teams.len(), 24);
    assert_eq!(projection.revision, 25);
}

#[test]
fn program_projection_recovers_exactly_after_service_reconstruction() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let team_id;
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
        service
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
            .expect("commit member");
    }

    let reopened = AgentActionService::new(store);
    let projection = reopened.project("program-1").expect("replay program");
    assert_eq!(projection.teams[&team_id].name, "Recovery Team");
    assert_eq!(projection.teams[&team_id].member_ids.len(), 1);
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
    assert_eq!(projection.tasks.len(), 1);
    assert!(projection
        .tasks
        .values()
        .all(|task| task.claimant.is_none()));
}
