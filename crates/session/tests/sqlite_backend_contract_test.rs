#![allow(clippy::unwrap_used, clippy::expect_used)]

use harness_contract::turn::InputRoutingDecision;
use session::{
    SessionBranchActivationPhase, SessionBranchActivationTransition, SessionBranchRequest,
    SessionCloseDisposition, SessionDomainEvent, SessionError, SessionEvent,
    SessionLifecycleFenceRequest, SessionLifecyclePhase, SessionLifecyclePlan,
    SessionLifecycleTombstoneRequest, SessionLifecycleTransition, SessionMessage,
    SessionMissionOutboxOperation, SessionMissionOutboxRequest, SessionRecord,
    SessionRuntimeInputStatus, SessionRuntimeOutboxRecord, SessionRuntimeOutboxRequest,
    SessionStoreBackend, SessionTerminalExecutionFence, SessionTerminalTranscriptCommit,
    SqliteSessionStore,
};

fn record(session_id: &str) -> SessionRecord {
    SessionRecord {
        session_id: session_id.to_string(),
        platform: "contract".to_string(),
        chat_id: session_id.to_string(),
        user_id: Some("contract-user".to_string()),
        model: None,
        created_at: "2026-07-26T00:00:00Z".to_string(),
        last_activity: "2026-07-26T00:00:00Z".to_string(),
        message_count: 0,
        reset_policy: "manual".to_string(),
        metadata_json: None,
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
    }
}

fn ingress(
    id: &str,
    decision: InputRoutingDecision,
    classification: &str,
) -> SessionRuntimeOutboxRequest {
    SessionRuntimeOutboxRequest {
        input_id: format!("input-{id}"),
        request_id: format!("request-{id}"),
        turn_id: format!("turn-{id}"),
        message_id: format!("message-{id}"),
        session_generation: 1,
        decision,
        target_turn_id: None,
        classification_json: Some(
            serde_json::json!({"classifier": classification, "version": 1}).to_string(),
        ),
        created_at_ms: 100,
        runtime_options_json: None,
    }
}

fn assert_timeline(
    backend: &dyn SessionStoreBackend,
    session_id: &str,
    expected_final: SessionRuntimeInputStatus,
) {
    let timeline = backend
        .get_session_domain_timeline_limited(session_id, 0, 10)
        .expect("canonical timeline");
    let timeline = timeline
        .iter()
        .map(|event| SessionDomainEvent::from_session_event(event).expect("typed event"))
        .collect::<Vec<_>>();
    assert_eq!(timeline.len(), 3);
    assert_eq!(
        timeline
            .iter()
            .map(|event| (event.kind.as_str(), event.status.as_deref()))
            .collect::<Vec<_>>(),
        vec![
            (
                SessionRuntimeInputStatus::Accepted.timeline_event_kind(),
                Some(SessionRuntimeInputStatus::Accepted.as_str()),
            ),
            (
                SessionRuntimeInputStatus::Classified.timeline_event_kind(),
                Some(SessionRuntimeInputStatus::Classified.as_str()),
            ),
            (
                expected_final.timeline_event_kind(),
                Some(expected_final.as_str()),
            ),
        ]
    );
}

fn assert_rejected_ingress_contract(
    backend: &dyn SessionStoreBackend,
    id: &str,
    decision: InputRoutingDecision,
    expected_status: SessionRuntimeInputStatus,
) {
    let session_id = format!("contract-{id}");
    backend
        .create_session(&record(&session_id))
        .expect("create session");
    let request = ingress(id, decision, "rejected");
    let stored = backend
        .append_ingress_with_runtime_outbox(
            &session_id,
            "user",
            Some(r#"[{"type":"text","text":"rejected but durable"}]"#),
            100,
            &request,
        )
        .expect("rejection must commit");

    assert_eq!(stored.status, expected_status);
    assert!(stored.status.is_terminal());
    assert_eq!(stored.terminal_at_ms, Some(100));
    assert_eq!(backend.get_message_count(&session_id).unwrap(), 1);
    assert!(backend
        .claim_session_runtime_outbox("contract-worker", 100, 1_000, 10)
        .unwrap()
        .is_empty());
    assert!(backend
        .active_session_runtime_outbox(10)
        .unwrap()
        .is_empty());
    assert_timeline(backend, &session_id, expected_status);

    let retried = backend
        .append_ingress_with_runtime_outbox(
            &session_id,
            "user",
            Some(r#"[{"type":"text","text":"rejected but durable"}]"#),
            100,
            &request,
        )
        .expect("identical transport retry is idempotent");
    assert_eq!(retried, stored);
    assert_eq!(backend.get_message_count(&session_id).unwrap(), 1);
}

fn terminal_messages(session_id: &str, id: &str) -> Vec<SessionMessage> {
    vec![
        SessionMessage {
            stable_message_id: format!("tool-{id}"),
            session_id: session_id.to_string(),
            sequence: 0,
            role: "tool".to_string(),
            content_json: r#"[{"type":"text","text":"evidence"}]"#.to_string(),
            blocks_count: 1,
            tool_use_id: Some(format!("tool-use-{id}")),
            tool_name: Some("read".to_string()),
            token_usage_json: None,
            created_at_ms: 0,
        },
        SessionMessage {
            stable_message_id: format!("tool-secondary-{id}"),
            session_id: session_id.to_string(),
            sequence: 0,
            role: "tool".to_string(),
            content_json: r#"[{"type":"text","text":"secondary evidence"}]"#.to_string(),
            blocks_count: 1,
            tool_use_id: Some(format!("tool-use-secondary-{id}")),
            tool_name: Some("read".to_string()),
            token_usage_json: None,
            created_at_ms: 0,
        },
        SessionMessage {
            stable_message_id: format!("assistant-{id}"),
            session_id: session_id.to_string(),
            sequence: 0,
            role: "assistant".to_string(),
            content_json: r#"[{"type":"text","text":"done"}]"#.to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: Some(r#"{"output_tokens":1}"#.to_string()),
            created_at_ms: 0,
        },
    ]
}

fn running_terminal_input(
    store: &SqliteSessionStore,
    session_id: &str,
    id: &str,
    lease_ms: u64,
) -> (
    SessionRuntimeOutboxRequest,
    SessionRuntimeOutboxRecord,
    String,
) {
    let request = ingress(id, InputRoutingDecision::StartNewTurn, "new_turn");
    store
        .append_ingress_with_runtime_outbox(
            session_id,
            "user",
            Some(r#"[{"type":"text","text":"run"}]"#),
            100,
            &request,
        )
        .expect("append terminal ingress");
    let claimed = store
        .claim_session_runtime_outbox("runtime-worker", 101, lease_ms, 1)
        .expect("claim terminal ingress")
        .remove(0);
    let token = claimed.claim_token.clone().expect("claim token");
    let running = store
        .mark_session_runtime_outbox_running(
            &request.request_id,
            "runtime-worker",
            1,
            &token,
            claimed.revision,
            102,
        )
        .expect("mark terminal ingress running");
    (request, running, token)
}

fn terminal_commit(
    request: &SessionRuntimeOutboxRequest,
    running: &SessionRuntimeOutboxRecord,
    token: &str,
    session_id: &str,
    id: &str,
    created_at_ms: u64,
) -> SessionTerminalTranscriptCommit {
    SessionTerminalTranscriptCommit {
        terminal_message_id: format!("assistant-{id}"),
        ingress_message_id: request.message_id.clone(),
        session_id: session_id.to_string(),
        turn_id: request.turn_id.clone(),
        messages: terminal_messages(session_id, id),
        runtime_commit_cursor: 42,
        consumed_input_sequence: running.sequence,
        created_at_ms,
        fence: SessionTerminalExecutionFence {
            request_id: request.request_id.clone(),
            input_sequence: running.sequence,
            session_generation: running.session_generation,
            claim_owner: "runtime-worker".to_string(),
            claim_token: token.to_string(),
            claim_fence_epoch: running
                .claim_fence_epoch
                .expect("running input owns an immutable claim fence"),
        },
    }
}

#[test]
fn sqlite_implements_canonical_ingress_and_rejection_contract() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store =
        SqliteSessionStore::open(&directory.path().join("session.db")).expect("sqlite store");
    let backend: &dyn SessionStoreBackend = &store;

    assert_rejected_ingress_contract(
        backend,
        "duplicate",
        InputRoutingDecision::RejectDuplicate,
        SessionRuntimeInputStatus::RejectedDuplicate,
    );
    assert_rejected_ingress_contract(
        backend,
        "policy",
        InputRoutingDecision::RejectPolicy,
        SessionRuntimeInputStatus::RejectedPolicy,
    );

    let session_id = "contract-runnable";
    backend
        .create_session(&record(session_id))
        .expect("create runnable session");
    let queued = backend
        .append_ingress_with_runtime_outbox(
            session_id,
            "user",
            Some(r#"[{"type":"text","text":"run"}]"#),
            100,
            &ingress("runnable", InputRoutingDecision::StartNewTurn, "new_turn"),
        )
        .expect("queue ingress");
    assert_eq!(queued.status, SessionRuntimeInputStatus::Queued);
    assert_timeline(backend, session_id, SessionRuntimeInputStatus::Queued);
    assert_eq!(
        backend
            .claim_session_runtime_outbox("contract-worker", 100, 1_000, 10)
            .expect("claim runnable")
            .len(),
        1
    );
}

#[test]
fn sqlite_fenced_terminal_commit_is_atomic_identity_bound_and_idempotent() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store =
        SqliteSessionStore::open(&directory.path().join("session.db")).expect("sqlite store");
    let session_id = "contract-terminal-fence";
    store
        .create_session(&record(session_id))
        .expect("create terminal session");
    let (request, running, token) =
        running_terminal_input(&store, session_id, "terminal-fence", 1_000);
    let renewed = store
        .renew_session_runtime_outbox_lease(
            &request.request_id,
            "runtime-worker",
            1,
            &token,
            running.revision,
            103,
            1_000,
        )
        .expect("renew running lease");
    assert!(renewed.revision > running.revision);
    let commit = terminal_commit(
        &request,
        &running,
        &token,
        session_id,
        "terminal-fence",
        104,
    );
    let mut wrong_sequence = commit.clone();
    wrong_sequence.fence.input_sequence = wrong_sequence.fence.input_sequence.saturating_add(1);
    wrong_sequence.consumed_input_sequence = wrong_sequence.fence.input_sequence;
    assert!(matches!(
        store.commit_terminal_transcript_if_fenced(&wrong_sequence),
        Err(SessionError::StaleExecutionFence(_))
    ));
    let receipt = store
        .commit_terminal_transcript_if_fenced(&commit)
        .expect("renewed immutable fence commits");
    assert!(receipt.inserted);
    assert_eq!(receipt.input.status, SessionRuntimeInputStatus::Completed);
    assert_eq!(receipt.input.runtime_commit_cursor, Some(42));
    assert_eq!(receipt.input.revision, renewed.revision + 1);
    assert_eq!(store.get_message_count(session_id).unwrap(), 4);

    let replay = store
        .commit_terminal_transcript_if_fenced(&commit)
        .expect("exact completed replay");
    assert!(!replay.inserted);
    assert_eq!(replay.messages, receipt.messages);
    assert_eq!(store.get_message_count(session_id).unwrap(), 4);

    let mut reordered_intermediate = commit.clone();
    reordered_intermediate.messages.swap(0, 1);
    let reordered_result = store.commit_terminal_transcript_if_fenced(&reordered_intermediate);
    assert!(
        matches!(&reordered_result, Err(SessionError::StaleExecutionFence(_))),
        "{reordered_result:?}"
    );
    assert_eq!(store.get_message_count(session_id).unwrap(), 4);

    let mut different_terminal = commit;
    different_terminal.terminal_message_id = "assistant-terminal-other".to_string();
    different_terminal
        .messages
        .last_mut()
        .expect("terminal message")
        .stable_message_id = different_terminal.terminal_message_id.clone();
    assert!(matches!(
        store.commit_terminal_transcript_if_fenced(&different_terminal),
        Err(SessionError::StaleExecutionFence(_))
    ));
    assert_eq!(store.get_message_count(session_id).unwrap(), 4);
}

#[test]
fn sqlite_stale_reclaim_generation_and_identity_cannot_commit_transcript() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store =
        SqliteSessionStore::open(&directory.path().join("session.db")).expect("sqlite store");
    let session_id = "contract-terminal-stale";
    store
        .create_session(&record(session_id))
        .expect("create terminal session");
    let (request, running, token) =
        running_terminal_input(&store, session_id, "terminal-stale", 50);
    let commit = terminal_commit(
        &request,
        &running,
        &token,
        session_id,
        "terminal-stale",
        150,
    );

    let mut wrong_turn = commit.clone();
    wrong_turn.turn_id = "turn-other".to_string();
    assert!(matches!(
        store.commit_terminal_transcript_if_fenced(&wrong_turn),
        Err(SessionError::StaleExecutionFence(_))
    ));
    assert_eq!(store.get_message_count(session_id).unwrap(), 1);

    let reclaimed = store
        .claim_session_runtime_outbox("runtime-worker-new", 151, 1_000, 1)
        .expect("reclaim expired input")
        .remove(0);
    assert_ne!(reclaimed.claim_token.as_deref(), Some(token.as_str()));
    assert!(matches!(
        store.commit_terminal_transcript_if_fenced(&commit),
        Err(SessionError::StaleExecutionFence(_))
    ));
    assert_eq!(store.get_message_count(session_id).unwrap(), 1);

    store
        .advance_session_input_generation(
            session_id,
            1,
            true,
            "test",
            "replace stale generation",
            152,
        )
        .expect("advance generation");
    assert!(matches!(
        store.commit_terminal_transcript_if_fenced(&commit),
        Err(SessionError::StaleExecutionFence(_))
    ));
    assert_eq!(store.get_message_count(session_id).unwrap(), 1);
    assert!(store
        .get_all_messages(session_id)
        .unwrap()
        .iter()
        .all(|message| !message.stable_message_id.starts_with("assistant-")));
}

#[test]
fn sqlite_one_hundred_inputs_in_one_session_commit_in_durable_fifo_order() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store =
        SqliteSessionStore::open(&directory.path().join("session.db")).expect("sqlite store");
    let session_id = "contract-fifo-100";
    store
        .create_session(&record(session_id))
        .expect("create fifo session");

    for index in 0..100_usize {
        let request = ingress(
            &format!("fifo-{index:03}"),
            InputRoutingDecision::StartNewTurn,
            "new_turn",
        );
        let queued = store
            .append_ingress_with_runtime_outbox(
                session_id,
                "user",
                Some(r#"[{"type":"text","text":"queued"}]"#),
                100 + index as u64,
                &request,
            )
            .expect("append fifo input");
        assert_eq!(queued.sequence, index);
    }

    let mut committed_sequences = Vec::new();
    for index in 0..100_usize {
        let mut claimed = store
            .claim_session_runtime_outbox("fifo-worker", 1_000 + index as u64, 10_000, 100)
            .expect("claim next fifo input");
        assert_eq!(
            claimed.len(),
            1,
            "only the earliest unfinished input in one Session may be claimed"
        );
        let claimed = claimed.remove(0);
        assert_eq!(claimed.sequence, index);
        let claim_token = claimed.claim_token.clone().expect("claim token");
        let running = store
            .mark_session_runtime_outbox_running(
                &claimed.request_id,
                "fifo-worker",
                claimed.session_generation,
                &claim_token,
                claimed.revision,
                1_100 + index as u64,
            )
            .expect("mark fifo input running");
        let request = ingress(
            &format!("fifo-{index:03}"),
            InputRoutingDecision::StartNewTurn,
            "new_turn",
        );
        let mut commit = terminal_commit(
            &request,
            &running,
            &claim_token,
            session_id,
            &format!("fifo-{index:03}"),
            1_200 + index as u64,
        );
        commit.fence.claim_owner = "fifo-worker".to_string();
        commit.runtime_commit_cursor = index as u64 + 1;
        let receipt = store
            .commit_terminal_transcript_if_fenced(&commit)
            .expect("commit fifo terminal");
        assert!(receipt.inserted);
        committed_sequences.push(receipt.input.sequence);
    }

    assert_eq!(committed_sequences, (0..100_usize).collect::<Vec<_>>());
    let terminal_ids = store
        .get_all_messages(session_id)
        .expect("load fifo transcript")
        .into_iter()
        .filter(|message| message.role == "assistant")
        .map(|message| message.stable_message_id)
        .collect::<Vec<_>>();
    assert_eq!(
        terminal_ids,
        (0..100_usize)
            .map(|index| format!("assistant-fifo-{index:03}"))
            .collect::<Vec<_>>()
    );
    assert!(store
        .session_runtime_outbox_for_session(session_id, 200)
        .expect("load fifo outbox")
        .iter()
        .all(|input| input.status == SessionRuntimeInputStatus::Completed));
}

#[test]
fn sqlite_thirty_two_sessions_are_isolated_when_one_session_is_slow() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store =
        SqliteSessionStore::open(&directory.path().join("session.db")).expect("sqlite store");
    for index in 0..32_u64 {
        let session_id = format!("isolation-{index:02}");
        store
            .create_session(&record(&session_id))
            .expect("create isolated session");
        store
            .append_ingress_with_runtime_outbox(
                &session_id,
                "user",
                Some(r#"[{"type":"text","text":"first"}]"#),
                100 + index,
                &ingress(
                    &format!("isolation-{index:02}-first"),
                    InputRoutingDecision::StartNewTurn,
                    "new_turn",
                ),
            )
            .expect("append isolated first input");
    }

    let claimed = store
        .claim_session_runtime_outbox("isolation-worker", 1_000, 10_000, 32)
        .expect("claim one input per session");
    assert_eq!(claimed.len(), 32);
    let mut slow_running = None;
    for claimed in claimed {
        let token = claimed.claim_token.clone().expect("claim token");
        let running = store
            .mark_session_runtime_outbox_running(
                &claimed.request_id,
                "isolation-worker",
                claimed.session_generation,
                &token,
                claimed.revision,
                1_001,
            )
            .expect("mark isolated input running");
        if claimed.session_id == "isolation-00" {
            slow_running = Some((running, token));
        } else {
            store
                .ack_session_runtime_outbox(
                    &claimed.request_id,
                    "isolation-worker",
                    claimed.session_generation,
                    &token,
                    running.revision,
                    SessionRuntimeInputStatus::Completed,
                    claimed.sequence as u64 + 1,
                    1_002,
                )
                .expect("complete fast session");
        }
    }
    assert!(slow_running.is_some(), "slow Session remains running");

    for index in 0..32_u64 {
        let session_id = format!("isolation-{index:02}");
        store
            .append_ingress_with_runtime_outbox(
                &session_id,
                "user",
                Some(r#"[{"type":"text","text":"second"}]"#),
                2_000 + index,
                &ingress(
                    &format!("isolation-{index:02}-second"),
                    InputRoutingDecision::StartNewTurn,
                    "new_turn",
                ),
            )
            .expect("append isolated second input");
    }

    let next = store
        .claim_session_runtime_outbox("isolation-worker-2", 2_100, 10_000, 32)
        .expect("claim fast sessions while one is held");
    assert_eq!(next.len(), 31);
    assert!(next.iter().all(|input| input.session_id != "isolation-00"));
    assert_eq!(
        next.iter()
            .map(|input| input.session_id.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        31
    );
}

#[test]
fn sqlite_restart_preserves_supplement_and_control_target_relationships() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("session.db");
    let session_id = "contract-related-inputs";
    {
        let store = SqliteSessionStore::open(&path).expect("sqlite store");
        store
            .create_session(&record(session_id))
            .expect("create related-input session");
        let mut supplement = ingress(
            "related-supplement",
            InputRoutingDecision::SupplementCurrentTurn,
            "supplement",
        );
        supplement.target_turn_id = Some("turn-primary".to_string());
        store
            .append_ingress_with_runtime_outbox(
                session_id,
                "user",
                Some(r#"[{"type":"text","text":"additional evidence"}]"#),
                100,
                &supplement,
            )
            .expect("append supplement");
        let mut control = ingress(
            "related-control",
            InputRoutingDecision::ControlOrApproval,
            "control",
        );
        control.target_turn_id = Some("turn-primary".to_string());
        store
            .append_ingress_with_runtime_outbox(
                session_id,
                "user",
                Some(r#"[{"type":"text","text":"approve"}]"#),
                101,
                &control,
            )
            .expect("append control");
    }

    let reopened = SqliteSessionStore::open(&path).expect("reopen related-input session");
    let mut inputs = reopened
        .session_runtime_outbox_for_session(session_id, 10)
        .expect("load related inputs");
    inputs.sort_by_key(|input| input.sequence);
    assert_eq!(inputs.len(), 2);
    assert_eq!(
        inputs
            .iter()
            .map(|input| (
                input.decision,
                input.target_turn_id.as_deref(),
                input.classification_json.as_deref()
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                InputRoutingDecision::SupplementCurrentTurn,
                Some("turn-primary"),
                Some(r#"{"classifier":"supplement","version":1}"#)
            ),
            (
                InputRoutingDecision::ControlOrApproval,
                Some("turn-primary"),
                Some(r#"{"classifier":"control","version":1}"#)
            ),
        ]
    );
}

fn lifecycle_event(session_id: &str, kind: &str, at_ms: u64) -> SessionEvent {
    SessionEvent {
        session_id: session_id.to_string(),
        event_type: kind.to_string(),
        event_json: serde_json::json!({"kind": kind, "at_ms": at_ms}).to_string(),
        sequence: 0,
        created_at_ms: at_ms,
    }
}

#[test]
fn sqlite_lifecycle_intent_recovers_every_phase_and_commits_one_tombstone() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("session.db");
    let session_id = "lifecycle-recovery";
    let operation_id = "session-lifecycle:archive:lifecycle-recovery";
    {
        let store = SqliteSessionStore::open(&path).expect("sqlite store");
        store
            .create_session(&record(session_id))
            .expect("create lifecycle session");
        store
            .append_ingress_with_runtime_outbox(
                session_id,
                "user",
                Some(r#"[{"type":"text","text":"pending"}]"#),
                100,
                &ingress(
                    "lifecycle-recovery",
                    InputRoutingDecision::StartNewTurn,
                    "new_turn",
                ),
            )
            .expect("append active input");
        let planned = store
            .plan_session_lifecycle(&SessionLifecyclePlan {
                operation_id: operation_id.to_string(),
                session_id: session_id.to_string(),
                disposition: SessionCloseDisposition::Archive,
                expected_generation: 1,
                created_at_ms: 110,
            })
            .expect("plan lifecycle");
        assert_eq!(planned.phase, SessionLifecyclePhase::Planned);
    }
    let store = SqliteSessionStore::open(&path).expect("reopen planned");
    let planned = store
        .get_session_lifecycle_intent(operation_id)
        .unwrap()
        .expect("planned intent");
    let fenced = store
        .fence_session_lifecycle(&SessionLifecycleFenceRequest {
            transition: SessionLifecycleTransition {
                operation_id: operation_id.to_string(),
                expected_revision: planned.revision,
                expected_phase: SessionLifecyclePhase::Planned,
                next_phase: SessionLifecyclePhase::AdmissionFenced,
                updated_at_ms: 120,
                error: None,
            },
            actor: "contract".to_string(),
            reason: "archive".to_string(),
            transitional_status: "archiving".to_string(),
            event: lifecycle_event(session_id, "session.archive_started", 120),
        })
        .expect("fence lifecycle");
    assert_eq!(fenced.phase, SessionLifecyclePhase::AdmissionFenced);
    assert_eq!(
        store.get_session_input_admission(session_id).unwrap(),
        Some(session::SessionInputAdmission {
            session_id: session_id.to_string(),
            generation: 2,
            open: false,
        })
    );
    assert!(store
        .session_runtime_outbox_for_session(session_id, 10)
        .unwrap()
        .iter()
        .all(|input| input.status == SessionRuntimeInputStatus::Expired));
    let failed = store
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: operation_id.to_string(),
            expected_revision: fenced.revision,
            expected_phase: SessionLifecyclePhase::AdmissionFenced,
            next_phase: SessionLifecyclePhase::Failed,
            updated_at_ms: 121,
            error: Some("simulated process failure".to_string()),
        })
        .expect("record recoverable failure");
    drop(store);

    let store = SqliteSessionStore::open(&path).expect("reopen failed");
    assert_eq!(
        store
            .list_recoverable_session_lifecycle_intents(10)
            .unwrap()
            .len(),
        1
    );
    let resumed = store
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: operation_id.to_string(),
            expected_revision: failed.revision,
            expected_phase: SessionLifecyclePhase::Failed,
            next_phase: SessionLifecyclePhase::AdmissionFenced,
            updated_at_ms: 130,
            error: None,
        })
        .expect("resume failed lifecycle");
    let drained = store
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: operation_id.to_string(),
            expected_revision: resumed.revision,
            expected_phase: SessionLifecyclePhase::AdmissionFenced,
            next_phase: SessionLifecyclePhase::RuntimeDrained,
            updated_at_ms: 140,
            error: None,
        })
        .expect("mark Runtime drained");
    drop(store);

    let store = SqliteSessionStore::open(&path).expect("reopen drained");
    let mut tombstone = store
        .get_session(session_id)
        .unwrap()
        .expect("session record");
    tombstone.status = "archived".to_string();
    tombstone.last_activity = "2026-07-26T00:00:01Z".to_string();
    tombstone.metadata_json = Some(r#"{"tombstone":{"kind":"archived"}}"#.to_string());
    let tombstone_request = SessionLifecycleTombstoneRequest {
        transition: SessionLifecycleTransition {
            operation_id: operation_id.to_string(),
            expected_revision: drained.revision,
            expected_phase: SessionLifecyclePhase::RuntimeDrained,
            next_phase: SessionLifecyclePhase::TombstoneCommitted,
            updated_at_ms: 150,
            error: None,
        },
        record: tombstone,
        mission_outbox: SessionMissionOutboxRequest {
            request_id: "mission:lifecycle-recovery:close".to_string(),
            session_id: session_id.to_string(),
            title: "Lifecycle recovery".to_string(),
            workspace_key: "contract-workspace".to_string(),
            operation: SessionMissionOutboxOperation::Close,
            created_at_ms: 150,
        },
        event: lifecycle_event(session_id, "session.archived", 150),
    };
    let committed = store
        .commit_session_lifecycle_tombstone(&tombstone_request)
        .expect("commit atomic tombstone");
    assert_eq!(committed.phase, SessionLifecyclePhase::TombstoneCommitted);
    assert!(store
        .get_session_mission_outbox("mission:lifecycle-recovery:close")
        .unwrap()
        .is_some());
    drop(store);

    let store = SqliteSessionStore::open(&path).expect("reopen tombstone");
    assert_eq!(
        store
            .get_events(session_id, 0)
            .unwrap()
            .iter()
            .filter(|event| event.event_type == "session.archive_started")
            .count(),
        1
    );
    assert_eq!(
        store
            .get_events(session_id, 0)
            .unwrap()
            .iter()
            .filter(|event| event.event_type == "session.archived")
            .count(),
        1
    );
    let unloaded = store
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: operation_id.to_string(),
            expected_revision: committed.revision,
            expected_phase: SessionLifecyclePhase::TombstoneCommitted,
            next_phase: SessionLifecyclePhase::Unloaded,
            updated_at_ms: 160,
            error: None,
        })
        .expect("mark lifecycle unloaded");
    assert_eq!(unloaded.phase, SessionLifecyclePhase::Unloaded);
    assert!(store
        .list_recoverable_session_lifecycle_intents(10)
        .unwrap()
        .is_empty());
    assert!(store
        .commit_session_lifecycle_tombstone(&tombstone_request)
        .is_err());
    assert_eq!(
        store
            .get_events(session_id, 0)
            .unwrap()
            .iter()
            .filter(|event| event.event_type == "session.archived")
            .count(),
        1
    );
}

#[test]
fn sqlite_delete_lifecycle_recovers_stable_phases_and_commits_one_tombstone() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("session.db");
    let session_id = "delete-lifecycle-recovery";
    let operation_id = "session-lifecycle:delete:delete-lifecycle-recovery";
    {
        let store = SqliteSessionStore::open(&path).expect("sqlite store");
        store
            .create_session(&record(session_id))
            .expect("create delete session");
        store
            .plan_session_lifecycle(&SessionLifecyclePlan {
                operation_id: operation_id.to_string(),
                session_id: session_id.to_string(),
                disposition: SessionCloseDisposition::Delete,
                expected_generation: 1,
                created_at_ms: 100,
            })
            .expect("plan delete lifecycle");
    }

    let store = SqliteSessionStore::open(&path).expect("reopen planned delete");
    let planned = store
        .get_session_lifecycle_intent(operation_id)
        .unwrap()
        .expect("planned delete intent");
    let fenced = store
        .fence_session_lifecycle(&SessionLifecycleFenceRequest {
            transition: SessionLifecycleTransition {
                operation_id: operation_id.to_string(),
                expected_revision: planned.revision,
                expected_phase: SessionLifecyclePhase::Planned,
                next_phase: SessionLifecyclePhase::AdmissionFenced,
                updated_at_ms: 110,
                error: None,
            },
            actor: "contract".to_string(),
            reason: "delete".to_string(),
            transitional_status: "deleting".to_string(),
            event: lifecycle_event(session_id, "session.delete_started", 110),
        })
        .expect("fence delete lifecycle");
    drop(store);

    let store = SqliteSessionStore::open(&path).expect("reopen fenced delete");
    let drained = store
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: operation_id.to_string(),
            expected_revision: fenced.revision,
            expected_phase: SessionLifecyclePhase::AdmissionFenced,
            next_phase: SessionLifecyclePhase::RuntimeDrained,
            updated_at_ms: 120,
            error: None,
        })
        .expect("mark delete Runtime drained");
    drop(store);

    let store = SqliteSessionStore::open(&path).expect("reopen drained delete");
    let mut tombstone = store
        .get_session(session_id)
        .unwrap()
        .expect("delete Session record");
    tombstone.status = "deleted".to_string();
    tombstone.last_activity = "2026-07-26T00:00:01Z".to_string();
    tombstone.metadata_json = Some(r#"{"tombstone":{"kind":"deleted"}}"#.to_string());
    let request = SessionLifecycleTombstoneRequest {
        transition: SessionLifecycleTransition {
            operation_id: operation_id.to_string(),
            expected_revision: drained.revision,
            expected_phase: SessionLifecyclePhase::RuntimeDrained,
            next_phase: SessionLifecyclePhase::TombstoneCommitted,
            updated_at_ms: 130,
            error: None,
        },
        record: tombstone,
        mission_outbox: SessionMissionOutboxRequest {
            request_id: "mission:delete-lifecycle-recovery:close".to_string(),
            session_id: session_id.to_string(),
            title: "Delete lifecycle recovery".to_string(),
            workspace_key: "contract-workspace".to_string(),
            operation: SessionMissionOutboxOperation::Close,
            created_at_ms: 130,
        },
        event: lifecycle_event(session_id, "session.deleted", 130),
    };
    let committed = store
        .commit_session_lifecycle_tombstone(&request)
        .expect("commit delete tombstone");
    drop(store);

    let store = SqliteSessionStore::open(&path).expect("reopen delete tombstone");
    assert_eq!(
        store
            .get_events(session_id, 0)
            .unwrap()
            .iter()
            .filter(|event| event.event_type == "session.deleted")
            .count(),
        1
    );
    let unloaded = store
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: operation_id.to_string(),
            expected_revision: committed.revision,
            expected_phase: SessionLifecyclePhase::TombstoneCommitted,
            next_phase: SessionLifecyclePhase::Unloaded,
            updated_at_ms: 140,
            error: None,
        })
        .expect("unload deleted Session");
    assert_eq!(unloaded.phase, SessionLifecyclePhase::Unloaded);
    assert!(store
        .list_recoverable_session_lifecycle_intents(10)
        .unwrap()
        .is_empty());
    assert!(store.commit_session_lifecycle_tombstone(&request).is_err());
    assert_eq!(
        store
            .get_events(session_id, 0)
            .unwrap()
            .iter()
            .filter(|event| event.event_type == "session.deleted")
            .count(),
        1
    );
}

#[test]
fn sqlite_branch_receipt_retries_committed_target_and_recovers_activation() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("session.db");
    let source = "branch-source";
    let target = "branch-target";
    let operation_id = "session-branch:branch-source:branch-target:2";
    let request = {
        let store = SqliteSessionStore::open(&path).expect("sqlite store");
        store.create_session(&record(source)).unwrap();
        for sequence in 0..2 {
            store
                .insert_message(&SessionMessage {
                    stable_message_id: format!("source-message-{sequence}"),
                    session_id: source.to_string(),
                    sequence,
                    role: "user".to_string(),
                    content_json: format!(r#"[{{"type":"text","text":"{sequence}"}}]"#),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: 100 + sequence as u64,
                })
                .unwrap();
        }
        SessionBranchRequest {
            operation_id: operation_id.to_string(),
            source_session_id: source.to_string(),
            source_message_count: 2,
            target: record(target),
            mission_outbox: SessionMissionOutboxRequest {
                request_id: "mission:branch-target:register".to_string(),
                session_id: target.to_string(),
                title: "Branch target".to_string(),
                workspace_key: "contract-workspace".to_string(),
                operation: SessionMissionOutboxOperation::Register,
                created_at_ms: 200,
            },
            source_event_json: r#"{"kind":"session.branched"}"#.to_string(),
            target_event_json: r#"{"kind":"session.branch_created"}"#.to_string(),
            created_at_ms: 200,
        }
    };
    let store = SqliteSessionStore::open(&path).expect("reopen before branch");
    let result = store
        .branch_session_at_cutoff(&request)
        .expect("commit branch transaction");
    assert_eq!(
        result.activation.phase,
        SessionBranchActivationPhase::BranchCommitted
    );
    assert_eq!(result.copied_message_count, 2);
    drop(store);

    let store = SqliteSessionStore::open(&path).expect("reopen pending activation");
    let replay = store
        .branch_session_at_cutoff(&request)
        .expect("same branch operation resumes committed target");
    assert_eq!(replay.target.session_id, target);
    assert_eq!(replay.copied_message_count, 2);
    assert_eq!(store.get_message_count(target).unwrap(), 2);
    let activation_pending = store
        .transition_session_branch_activation(&SessionBranchActivationTransition {
            operation_id: operation_id.to_string(),
            expected_revision: replay.activation.revision,
            expected_phase: SessionBranchActivationPhase::BranchCommitted,
            next_phase: SessionBranchActivationPhase::ActivationPending,
            updated_at_ms: 205,
            error: None,
        })
        .expect("begin Runtime activation");
    let failed = store
        .transition_session_branch_activation(&SessionBranchActivationTransition {
            operation_id: operation_id.to_string(),
            expected_revision: activation_pending.revision,
            expected_phase: SessionBranchActivationPhase::ActivationPending,
            next_phase: SessionBranchActivationPhase::Failed,
            updated_at_ms: 210,
            error: Some("simulated activation crash".to_string()),
        })
        .expect("persist activation failure");
    drop(store);

    let store = SqliteSessionStore::open(&path).expect("reopen failed activation");
    assert_eq!(
        store
            .list_recoverable_session_branch_activations(10)
            .unwrap()
            .len(),
        1
    );
    let pending = store
        .transition_session_branch_activation(&SessionBranchActivationTransition {
            operation_id: operation_id.to_string(),
            expected_revision: failed.revision,
            expected_phase: SessionBranchActivationPhase::Failed,
            next_phase: SessionBranchActivationPhase::ActivationPending,
            updated_at_ms: 220,
            error: None,
        })
        .expect("retry activation");
    let activated = store
        .transition_session_branch_activation(&SessionBranchActivationTransition {
            operation_id: operation_id.to_string(),
            expected_revision: pending.revision,
            expected_phase: SessionBranchActivationPhase::ActivationPending,
            next_phase: SessionBranchActivationPhase::Activated,
            updated_at_ms: 230,
            error: None,
        })
        .expect("activate branch");
    assert_eq!(activated.phase, SessionBranchActivationPhase::Activated);
    assert!(store
        .list_recoverable_session_branch_activations(10)
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .get_events(source, 0)
            .unwrap()
            .iter()
            .filter(|event| event.event_type == "SessionBranched")
            .count(),
        1
    );
    assert_eq!(
        store
            .get_events(target, 0)
            .unwrap()
            .iter()
            .filter(|event| event.event_type == "BranchCreated")
            .count(),
        1
    );
}
