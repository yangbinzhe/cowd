use super::*;

fn projector(store: &Arc<RuntimeEventStore>) -> CollaborationExperienceProjector {
    CollaborationExperienceProjector::new(
        Arc::clone(store),
        crate::ExecutionGraphStateStore::new(Arc::clone(store)),
        "test-workspace".into(),
    )
}

// Aggregation fixtures deliberately exercise the writer, not a manually
// appended episode/pattern event. Source-to-episode verification is separate.
fn episode(program: &str, turn: &str) -> CollaborationExperienceEpisode {
    CollaborationExperienceEpisode {
        schema_version: COLLABORATION_EXPERIENCE_SCHEMA_VERSION,
        episode_id: CollaborationExperienceEpisode::deterministic_id(program, 5),
        session_ref_hash: "sha256:session".into(),
        turn_ref_hash: format!("sha256:{turn}"),
        program_id: program.into(),
        program_revision: 5,
        intent_digest: "sha256:intent".into(),
        binding_digest: "sha256:binding".into(),
        capacity_profile_digest: "sha256:capacity".into(),
        approval_policy_digest: "sha256:policy".into(),
        semantic_signature: CollaborationSemanticSignature {
            normalizer_revision: COLLABORATION_SIGNATURE_NORMALIZER_REVISION,
            workstream_shapes: vec![SemanticWorkstreamShape {
                ordinal: 0,
                multiplicity_min: 1,
                multiplicity_max: 1,
                required_capability_ids: vec!["read".into()],
                required_skill_ids: vec![],
                required_tool_capabilities: vec!["read".into()],
                acceptance_kinds: vec!["independent_task_review".into()],
                result_field_shapes: vec!["artifact_refs".into()],
            }],
            dependency_shapes: vec![],
            required_capability_ids: vec!["read".into()],
            required_skill_ids: vec![],
            required_tool_capabilities: vec!["read".into()],
            acceptance_kinds: vec!["independent_task_review".into()],
            result_field_shapes: vec!["artifact_refs".into()],
        },
        outcome: CollaborationExperienceOutcome::Completed,
        evidence_refs: vec!["sha256:evidence".into()],
        coverage: CollaborationEvidenceCoverage {
            required_obligation_count: 1,
            satisfied_obligation_count: 1,
            coverage_basis_points: 10_000,
            reusable: true,
        },
        latency_ms: 50,
        resource_summary: CollaborationResourceSummary {
            parallel_demand: 1,
            context_reservation_tokens: None,
            output_reservation_tokens: None,
        },
        completed_at_ms: 100,
    }
}

fn patterns(store: &RuntimeEventStore) -> Vec<CollaborationSemanticPattern> {
    store
        .replay_scope_kind(RuntimeEventScope::Evolution, PATTERN_KIND)
        .unwrap()
        .into_iter()
        .map(|event| serde_json::from_value(event.payload["pattern"].clone()).unwrap())
        .collect()
}

#[test]
fn writer_requires_independent_turns_and_is_atomic_and_replay_safe() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let writer = projector(&store);
    for id in ["p1", "p2", "p3"] {
        writer
            .record_episode(&episode(id, "turn-a"), "source")
            .unwrap();
    }
    assert!(
        patterns(&store).is_empty(),
        "three Programs in one Turn are not three experiments"
    );
    writer
        .record_episode(&episode("p4", "turn-b"), "source")
        .unwrap();
    assert!(patterns(&store).is_empty());
    let last = episode("p5", "turn-c");
    writer.record_episode(&last, "source").unwrap();
    let pattern = patterns(&store).pop().unwrap();
    assert!(pattern.is_actionable());
    assert_eq!(pattern.support_count, 3);
    assert!(pattern.qualifying_episode_ids.contains(&last.episode_id));
    let cursor = store.current_commit_cursor();
    projector(&store).record_episode(&last, "source").unwrap();
    assert_eq!(
        store.current_commit_cursor(),
        cursor,
        "restart must not append duplicated evidence"
    );
    let episode_event = store
        .event_by_idempotency_key(
            &format!("evolution:episode:{}", last.episode_id),
            &last.episode_id,
        )
        .unwrap()
        .unwrap();
    let pattern_event = store
        .latest_for_stream_kind(
            &format!("evolution:pattern:{}", pattern.pattern_id),
            PATTERN_KIND,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        episode_event.commit_cursor, pattern_event.commit_cursor,
        "episode and pattern commit atomically"
    );
    let mut conflict = last;
    conflict.latency_ms += 1;
    assert!(writer
        .record_episode(&conflict, "source")
        .unwrap_err()
        .contains("identity conflict"));
}

#[test]
fn failed_and_incomplete_episodes_are_durable_but_never_advisory_support() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let writer = projector(&store);
    for (index, outcome) in [
        CollaborationExperienceOutcome::Failed,
        CollaborationExperienceOutcome::Partial,
        CollaborationExperienceOutcome::Cancelled,
    ]
    .into_iter()
    .enumerate()
    {
        let mut input = episode(&format!("p{index}"), &format!("t{index}"));
        input.outcome = outcome;
        writer.record_episode(&input, "source").unwrap();
    }
    let mut input = episode("missing-binding", "other");
    input.coverage.reusable = false;
    writer.record_episode(&input, "source").unwrap();
    assert_eq!(
        store
            .replay_scope_kind(RuntimeEventScope::Evolution, EPISODE_KIND)
            .unwrap()
            .len(),
        4
    );
    assert!(patterns(&store).is_empty());
    assert!(store
        .replay_scope_kind(RuntimeEventScope::Evolution, SUPPORT_KIND)
        .unwrap()
        .is_empty());
    let encoded = serde_json::to_value(input.resource_summary).unwrap();
    assert!(encoded["context_reservation_tokens"].is_null());
    assert!(encoded["output_reservation_tokens"].is_null());
}

#[test]
fn projector_reconstruction_preserves_checkpoint_and_episode_idempotency() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let checkpoint = {
        let writer = projector(&store);
        for index in 0..2 {
            writer
                .record_episode(
                    &episode(&format!("p{index}"), &format!("t{index}")),
                    "source",
                )
                .unwrap();
        }
        writer.project_available(64).unwrap();
        assert!(read_patterns(&store, 8).unwrap().is_empty());
        store.projection_checkpoint(PROJECTOR_ID).unwrap().unwrap()
    };
    let last = episode("p2", "t2");
    {
        assert_eq!(
            store
                .projection_checkpoint(PROJECTOR_ID)
                .unwrap()
                .unwrap()
                .source_cursor,
            checkpoint.source_cursor
        );
        projector(&store).record_episode(&last, "source").unwrap();
        assert_eq!(read_patterns(&store, 8).unwrap()[0].support_count, 3);
    }
    let before_replay = store.current_commit_cursor();
    projector(&store).record_episode(&last, "source").unwrap();
    assert_eq!(store.current_commit_cursor(), before_replay);
    assert_eq!(
        store
            .replay_scope_kind(RuntimeEventScope::Evolution, EPISODE_KIND)
            .unwrap()
            .len(),
        3
    );
    assert_eq!(read_patterns(&store, 8).unwrap().len(), 1);
}

#[test]
fn unrelated_events_do_not_produce_experience_and_cursor_survives_restart() {
    let store = Arc::new(RuntimeEventStore::for_test());
    store
        .append(RuntimeEventInput {
            stream_id: "task:test".into(),
            scope: RuntimeEventScope::Task,
            kind: "task.activity".into(),
            status: None,
            actor: None,
            refs: vec![],
            payload: json!({}),
        })
        .unwrap();
    projector(&store).project_available(64).unwrap();
    assert!(store
        .replay_scope_kind(RuntimeEventScope::Evolution, EPISODE_KIND)
        .unwrap()
        .is_empty());
    assert_eq!(
        projector(&store)
            .project_available(64)
            .unwrap()
            .matched_events,
        0
    );
}

#[test]
fn invalid_terminal_source_keeps_cursor_and_does_not_manufacture_an_episode() {
    let store = Arc::new(RuntimeEventStore::for_test());
    store
        .append(RuntimeEventInput {
            stream_id: "agentic-program:missing".into(),
            scope: RuntimeEventScope::Program,
            kind: VERDICT_KIND.into(),
            status: Some("verified".into()),
            actor: None,
            refs: vec![],
            payload: json!({"program_id": "missing", "verdict": {}}),
        })
        .unwrap();
    assert!(projector(&store).project_available(64).is_err());
    assert!(store.projection_checkpoint(PROJECTOR_ID).unwrap().is_none());
    assert!(store
        .replay_scope_kind(RuntimeEventScope::Evolution, EPISODE_KIND)
        .unwrap()
        .is_empty());
}

#[test]
fn concurrent_signature_updates_preserve_independent_support() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let threads = (0..3)
        .map(|index| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                projector(&store).record_episode(
                    &episode(&format!("p{index}"), &format!("t{index}")),
                    "source",
                )
            })
        })
        .collect::<Vec<_>>();
    for thread in threads {
        thread.join().unwrap().unwrap();
    }
    assert_eq!(patterns(&store).last().unwrap().support_count, 3);
}

#[test]
fn model_can_read_advice_without_mutating_program_or_gaining_permissions() {
    use harness_contract::agent_action::{
        AgentAction, AgentActionEnvelope, AgentActorBinding, AgentActorKind, StateInspectInput,
    };
    let store = Arc::new(RuntimeEventStore::for_test());
    let writer = projector(&store);
    for index in 0..3 {
        writer
            .record_episode(
                &episode(&format!("p{index}"), &format!("t{index}")),
                "source",
            )
            .unwrap();
    }
    let cursor = store.current_commit_cursor();
    let actor = AgentActorBinding {
        objective_id: "new-objective".into(),
        program_id: "new-program".into(),
        session_id: "new-session".into(),
        turn_id: "new-turn".into(),
        root_execution_id: None,
        required_team_count: 0,
        objective_summary: "Plan useful work".into(),
        model_lease: "test".into(),
        permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
        resource_scopes: vec![],
        actor_id: "root".into(),
        kind: AgentActorKind::Root,
        execution_id: None,
        team_id: None,
        agent_id: None,
    };
    let response = crate::AgentActionService::new(Arc::clone(&store))
        .apply(&AgentActionEnvelope {
            action_id: "read-advice".into(),
            actor,
            expected_revision: None,
            action: AgentAction::StateInspect(StateInspectInput {
                wait_for_workers: false,
                scope_ref: Some("collaboration_patterns".into()),
                after_revision: Some(0),
                page_cursor: None,
                entry_ref: None,
            }),
        })
        .unwrap();
    let projection = response.projection.unwrap();
    assert_eq!(projection["advisory_only"], true);
    assert_eq!(projection["patterns"].as_array().unwrap().len(), 1);
    assert!(
        projection["patterns"][0]
            .get("qualifying_episode_ids")
            .is_none(),
        "do not inject cross-session identities into model history"
    );
    assert_eq!(
        store.current_commit_cursor(),
        cursor,
        "advice cannot open Program, grant tools, or mutate definitions"
    );
    assert!(store
        .list_stream("agentic-program:new-program")
        .unwrap()
        .is_empty());
}
