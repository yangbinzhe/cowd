#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Barrier};

use harness_contract::{
    input_disposition::{
        InputApplicationState, InputDispositionAction, InputWorkRelation,
        SessionInputApplicationReceipt,
    },
    turn::InputRoutingDecision,
};
use session::{
    SessionBranchActivationPhase, SessionBranchActivationTransition, SessionBranchRequest,
    SessionCloseDisposition, SessionDomainEvent, SessionDomainScope, SessionError, SessionEvent,
    SessionLifecycleFenceRequest, SessionLifecyclePhase, SessionLifecyclePlan,
    SessionLifecycleTombstoneRequest, SessionLifecycleTransition, SessionMessage, SessionRecord,
    SessionRuntimeInputStatus, SessionRuntimeOutboxRecord, SessionRuntimeOutboxRequest,
    SessionStoreBackend, SessionTerminalExecutionFence, SessionTerminalTranscriptCommit,
};

/// Backend-neutral fixture boundary. Reopening must retain the same durable
/// database so the suite proves recovery rather than merely exercising one
/// in-memory handle.
pub trait BackendContractFixture {
    fn backend(&self) -> &dyn SessionStoreBackend;
    fn shared_backend(&self) -> Arc<dyn SessionStoreBackend>;
    fn reopen(&mut self);
}

fn record(session_id: &str) -> SessionRecord {
    SessionRecord {
        session_id: session_id.to_string(),
        platform: "backend-contract".to_string(),
        chat_id: session_id.to_string(),
        user_id: Some("contract-user".to_string()),
        model: None,
        created_at: "2026-07-27T00:00:00Z".to_string(),
        last_activity: "2026-07-27T00:00:00Z".to_string(),
        message_count: 0,
        reset_policy: "manual".to_string(),
        metadata_json: None,
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
    }
}

fn ingress(id: &str, generation: u64) -> SessionRuntimeOutboxRequest {
    SessionRuntimeOutboxRequest {
        input_id: format!("input-{id}"),
        request_id: format!("request-{id}"),
        turn_id: format!("turn-{id}"),
        message_id: format!("message-{id}"),
        session_generation: generation,
        decision: InputRoutingDecision::StartNewTurn,
        target_turn_id: None,
        classification_json: Some(r#"{"classifier":"backend-contract","version":1}"#.to_string()),
        task_route_hint: None,
        created_at_ms: 100,
        runtime_options_json: None,
    }
}

fn append_input(
    backend: &dyn SessionStoreBackend,
    session_id: &str,
    request: &SessionRuntimeOutboxRequest,
) {
    backend
        .append_ingress_with_runtime_outbox(
            session_id,
            "user",
            Some(r#"[{"type":"text","text":"contract input"}]"#),
            request.created_at_ms,
            request,
        )
        .expect("append durable Session input");
}

fn terminal_messages(session_id: &str, id: &str) -> Vec<SessionMessage> {
    vec![
        SessionMessage {
            stable_message_id: format!("tool-{id}"),
            session_id: session_id.to_string(),
            sequence: 0,
            role: "tool".to_string(),
            content_json: r#"[{"type":"text","text":"verified evidence"}]"#.to_string(),
            blocks_count: 1,
            tool_use_id: Some(format!("tool-use-{id}")),
            tool_name: Some("read".to_string()),
            token_usage_json: None,
            created_at_ms: 0,
        },
        SessionMessage {
            stable_message_id: format!("assistant-{id}"),
            session_id: session_id.to_string(),
            sequence: 0,
            role: "assistant".to_string(),
            content_json: r#"[{"type":"text","text":"complete"}]"#.to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: Some(r#"{"output_tokens":1}"#.to_string()),
            created_at_ms: 0,
        },
    ]
}

fn running_input(
    backend: &dyn SessionStoreBackend,
    session_id: &str,
    id: &str,
    now_ms: u64,
    lease_ms: u64,
) -> (
    SessionRuntimeOutboxRequest,
    SessionRuntimeOutboxRecord,
    String,
) {
    let request = ingress(id, 1);
    append_input(backend, session_id, &request);
    let claimed = backend
        .claim_session_runtime_outbox("contract-worker", now_ms, lease_ms, 1)
        .expect("claim durable Session input")
        .remove(0);
    let claim_token = claimed.claim_token.clone().expect("claim token");
    let running = backend
        .mark_session_runtime_outbox_running(
            &request.request_id,
            "contract-worker",
            request.session_generation,
            &claim_token,
            claimed.revision,
            now_ms + 1,
        )
        .expect("mark durable Session input running");
    (request, running, claim_token)
}

fn terminal_commit(
    request: &SessionRuntimeOutboxRequest,
    running: &SessionRuntimeOutboxRecord,
    claim_token: &str,
    session_id: &str,
    id: &str,
    now_ms: u64,
) -> SessionTerminalTranscriptCommit {
    SessionTerminalTranscriptCommit {
        terminal_message_id: format!("assistant-{id}"),
        ingress_message_id: request.message_id.clone(),
        session_id: session_id.to_string(),
        turn_id: request.turn_id.clone(),
        messages: terminal_messages(session_id, id),
        runtime_commit_cursor: 42,
        consumed_input_sequence: running.sequence,
        created_at_ms: now_ms,
        fence: SessionTerminalExecutionFence {
            request_id: request.request_id.clone(),
            input_sequence: running.sequence,
            session_generation: running.session_generation,
            claim_owner: "contract-worker".to_string(),
            claim_token: claim_token.to_string(),
            claim_fence_epoch: running
                .claim_fence_epoch
                .expect("running input owns an immutable claim fence"),
        },
    }
}

fn assert_stale_fence(result: session::SessionResult<session::SessionTerminalTranscriptReceipt>) {
    assert!(
        matches!(result, Err(SessionError::StaleExecutionFence(_))),
        "invalid execution identity must be rejected, got {result:?}"
    );
}

/// Proves that input generation, sequence and claim identity form one terminal
/// publication fence. A lease reclaim or generation advance invalidates the
/// old worker, while an exact successful replay remains idempotent.
pub fn input_generation_and_claim_fence(fixture: &mut impl BackendContractFixture) {
    let stale_session = "contract-input-stale";
    fixture
        .backend()
        .create_session(&record(stale_session))
        .expect("create stale-fence Session");

    let wrong_generation = ingress("wrong-generation", 2);
    assert!(fixture
        .backend()
        .append_ingress_with_runtime_outbox(
            stale_session,
            "user",
            Some(r#"[{"type":"text","text":"must roll back"}]"#),
            99,
            &wrong_generation,
        )
        .is_err());
    assert_eq!(
        fixture
            .backend()
            .get_message_count(stale_session)
            .expect("count rolled-back transcript"),
        0
    );

    let (request, running, token) =
        running_input(fixture.backend(), stale_session, "stale", 101, 10);
    let commit = terminal_commit(&request, &running, &token, stale_session, "stale", 120);
    let mut wrong_sequence = commit.clone();
    wrong_sequence.fence.input_sequence += 1;
    wrong_sequence.consumed_input_sequence = wrong_sequence.fence.input_sequence;
    assert_stale_fence(
        fixture
            .backend()
            .commit_terminal_transcript_if_fenced(&wrong_sequence),
    );
    let mut wrong_owner = commit.clone();
    wrong_owner.fence.claim_owner = "foreign-worker".to_string();
    assert_stale_fence(
        fixture
            .backend()
            .commit_terminal_transcript_if_fenced(&wrong_owner),
    );

    let reclaimed = fixture
        .backend()
        .claim_session_runtime_outbox("replacement-worker", 200, 1_000, 1)
        .expect("reclaim expired input")
        .remove(0);
    assert_ne!(reclaimed.claim_token.as_deref(), Some(token.as_str()));
    assert_stale_fence(
        fixture
            .backend()
            .commit_terminal_transcript_if_fenced(&commit),
    );
    fixture
        .backend()
        .advance_session_input_generation(
            stale_session,
            1,
            true,
            "backend-contract",
            "invalidate stale execution",
            201,
        )
        .expect("advance Session generation");
    assert_stale_fence(
        fixture
            .backend()
            .commit_terminal_transcript_if_fenced(&commit),
    );
    assert_eq!(
        fixture
            .backend()
            .get_message_count(stale_session)
            .expect("count stale transcript"),
        1,
        "only the durable ingress may remain after stale terminal attempts"
    );

    let success_session = "contract-input-success";
    fixture
        .backend()
        .create_session(&record(success_session))
        .expect("create success Session");
    let (success_request, success_running, success_token) =
        running_input(fixture.backend(), success_session, "success", 300, 1_000);
    let renewed = fixture
        .backend()
        .renew_session_runtime_outbox_lease(
            &success_request.request_id,
            "contract-worker",
            success_running.session_generation,
            &success_token,
            success_running.revision,
            301,
            1_000,
        )
        .expect("renew mutable lease revision");
    assert!(renewed.revision > success_running.revision);
    assert_eq!(
        renewed.claim_fence_epoch, success_running.claim_fence_epoch,
        "lease heartbeat must not mutate terminal claim identity"
    );
    let success_commit = terminal_commit(
        &success_request,
        &success_running,
        &success_token,
        success_session,
        "success",
        302,
    );
    let mut wrong_epoch = success_commit.clone();
    wrong_epoch.fence.claim_fence_epoch += 1;
    assert_stale_fence(
        fixture
            .backend()
            .commit_terminal_transcript_if_fenced(&wrong_epoch),
    );
    let receipt = fixture
        .backend()
        .commit_terminal_transcript_if_fenced(&success_commit)
        .expect("commit terminal transcript");
    assert!(receipt.inserted);
    assert_eq!(receipt.input.status, SessionRuntimeInputStatus::Completed);
    assert_eq!(receipt.input.runtime_commit_cursor, Some(42));
    let replay = fixture
        .backend()
        .commit_terminal_transcript_if_fenced(&success_commit)
        .expect("replay exact terminal commit");
    assert!(!replay.inserted);
    assert_eq!(replay.messages, receipt.messages);
    assert_eq!(
        fixture
            .backend()
            .get_message_count(success_session)
            .expect("count committed transcript"),
        3
    );
}

/// Proves that a semantic disposition spanning multiple durable inputs is one
/// fenced transaction on every backend, including restart recovery.
pub fn input_application_receipt_is_atomic_and_recoverable(
    fixture: &mut impl BackendContractFixture,
) {
    let session_id = "contract-input-application";
    fixture
        .backend()
        .create_session(&record(session_id))
        .expect("create input-application Session");
    let first_request = ingress("application-first", 1);
    let second_request = ingress("application-second", 1);
    let first = fixture
        .backend()
        .append_ingress_with_runtime_outbox(
            session_id,
            "user",
            Some(r#"[{"type":"text","text":"first update"}]"#),
            100,
            &first_request,
        )
        .expect("append first application input");
    let second = fixture
        .backend()
        .append_ingress_with_runtime_outbox(
            session_id,
            "user",
            Some(r#"[{"type":"text","text":"second update"}]"#),
            101,
            &second_request,
        )
        .expect("append second application input");
    let input_ids = vec![first.input_id.clone(), second.input_id.clone()];
    let prepared = SessionInputApplicationReceipt {
        disposition_id: "disposition-contract".to_string(),
        leader_input_id: first.input_id.clone(),
        input_ids: input_ids.clone(),
        action: InputDispositionAction::AddRequiredTask,
        relation: InputWorkRelation::NewTask,
        state: InputApplicationState::Prepared,
        objective: "materialize one required task".to_string(),
        required: true,
        attempts: 1,
        summary: "prepared".to_string(),
        task_ids: Vec::new(),
        team_ids: Vec::new(),
        agent_ids: Vec::new(),
        execution_ids: Vec::new(),
        target_session_id: None,
        target_session_created: false,
        error: None,
        revision: 0,
        updated_at_ms: 102,
    };
    let prepared_rows = fixture
        .backend()
        .set_session_input_application_receipt(
            &input_ids,
            &[first.revision, second.revision],
            &prepared,
            102,
        )
        .expect("commit prepared receipt atomically");
    assert!(prepared_rows
        .iter()
        .all(|row| row.application_receipt.as_ref() == Some(&prepared)));

    let mut illegal = prepared.clone();
    illegal.state = InputApplicationState::Applied;
    illegal.summary = "must not skip materializing".to_string();
    illegal.revision = 1;
    illegal.updated_at_ms = 103;
    assert!(fixture
        .backend()
        .set_session_input_application_receipt(
            &input_ids,
            &prepared_rows
                .iter()
                .map(|row| row.revision)
                .collect::<Vec<_>>(),
            &illegal,
            103,
        )
        .is_err());
    for row in &prepared_rows {
        let persisted = fixture
            .backend()
            .get_session_runtime_outbox_by_input_id(&row.input_id)
            .expect("read after rejected transition")
            .expect("input remains durable");
        assert_eq!(persisted.revision, row.revision);
        assert_eq!(persisted.application_receipt.as_ref(), Some(&prepared));
    }

    let mut materializing = prepared.clone();
    materializing.state = InputApplicationState::Materializing;
    materializing.summary = "materializing".to_string();
    materializing.revision = 1;
    materializing.updated_at_ms = 104;
    let materializing_rows = fixture
        .backend()
        .set_session_input_application_receipt(
            &input_ids,
            &prepared_rows
                .iter()
                .map(|row| row.revision)
                .collect::<Vec<_>>(),
            &materializing,
            104,
        )
        .expect("commit materializing receipt");
    let mut applied = materializing.clone();
    applied.state = InputApplicationState::Applied;
    applied.summary = "required Task materialized".to_string();
    applied.task_ids = vec!["task:disposition-contract".to_string()];
    applied.execution_ids = vec!["execution:disposition-contract".to_string()];
    applied.revision = 2;
    applied.updated_at_ms = 105;
    fixture
        .backend()
        .set_session_input_application_receipt(
            &input_ids,
            &materializing_rows
                .iter()
                .map(|row| row.revision)
                .collect::<Vec<_>>(),
            &applied,
            105,
        )
        .expect("commit applied receipt");

    fixture.reopen();
    for input_id in &input_ids {
        let recovered = fixture
            .backend()
            .get_session_runtime_outbox_by_input_id(input_id)
            .expect("read recovered input")
            .expect("recovered input exists");
        assert_eq!(recovered.application_receipt.as_ref(), Some(&applied));
        assert_eq!(recovered.decision, InputRoutingDecision::SpawnSubtask);
        assert_eq!(recovered.status, SessionRuntimeInputStatus::Completed);
        assert_eq!(recovered.terminal_at_ms, Some(105));
    }

    let replacement_request = ingress("application-replacement", 1);
    let replacement = fixture
        .backend()
        .append_ingress_with_runtime_outbox(
            session_id,
            "user",
            Some(r#"[{"type":"text","text":"replace the current task"}]"#),
            106,
            &replacement_request,
        )
        .expect("append replacement input");
    let replacement_ids = vec![replacement.input_id.clone()];
    let mut replacement_receipt = SessionInputApplicationReceipt {
        disposition_id: "disposition-replacement".to_string(),
        leader_input_id: replacement.input_id.clone(),
        input_ids: replacement_ids.clone(),
        action: InputDispositionAction::ReplaceCurrentTask,
        relation: InputWorkRelation::NewTask,
        state: InputApplicationState::Prepared,
        objective: "replace the active task".to_string(),
        required: true,
        attempts: 1,
        summary: "prepared".to_string(),
        task_ids: Vec::new(),
        team_ids: Vec::new(),
        agent_ids: Vec::new(),
        execution_ids: Vec::new(),
        target_session_id: None,
        target_session_created: false,
        error: None,
        revision: 0,
        updated_at_ms: 107,
    };
    let rows = fixture
        .backend()
        .set_session_input_application_receipt(
            &replacement_ids,
            &[replacement.revision],
            &replacement_receipt,
            107,
        )
        .expect("prepare replacement receipt");
    replacement_receipt.state = InputApplicationState::Materializing;
    replacement_receipt.revision = 1;
    replacement_receipt.updated_at_ms = 108;
    let rows = fixture
        .backend()
        .set_session_input_application_receipt(
            &replacement_ids,
            &[rows[0].revision],
            &replacement_receipt,
            108,
        )
        .expect("materialize replacement receipt");
    replacement_receipt.state = InputApplicationState::Applied;
    replacement_receipt.summary = "replacement queued".to_string();
    replacement_receipt.task_ids = vec!["task-replaced".to_string()];
    replacement_receipt.execution_ids = vec!["execution-replaced".to_string()];
    replacement_receipt.revision = 2;
    replacement_receipt.updated_at_ms = 109;
    let rows = fixture
        .backend()
        .set_session_input_application_receipt(
            &replacement_ids,
            &[rows[0].revision],
            &replacement_receipt,
            109,
        )
        .expect("apply replacement receipt");
    assert_eq!(rows[0].decision, InputRoutingDecision::StartNewTurn);
    assert_eq!(rows[0].status, SessionRuntimeInputStatus::Reclassified);
    assert_eq!(rows[0].target_turn_id, None);
    assert_eq!(rows[0].terminal_at_ms, None);

    let dispatch_request = ingress("application-new-session", 1);
    let dispatch = fixture
        .backend()
        .append_ingress_with_runtime_outbox(
            session_id,
            "user",
            Some(r#"[{"type":"text","text":"continue in an isolated session"}]"#),
            110,
            &dispatch_request,
        )
        .expect("append isolated Session dispatch input");
    let dispatch_ids = vec![dispatch.input_id.clone()];
    let mut dispatch_receipt = SessionInputApplicationReceipt {
        disposition_id: "disposition-new-session".to_string(),
        leader_input_id: dispatch.input_id.clone(),
        input_ids: dispatch_ids.clone(),
        action: InputDispositionAction::DispatchSession,
        relation: InputWorkRelation::NewSession,
        state: InputApplicationState::Prepared,
        objective: "continue in an isolated Session".to_string(),
        required: true,
        attempts: 1,
        summary: "prepared".to_string(),
        task_ids: Vec::new(),
        team_ids: Vec::new(),
        agent_ids: Vec::new(),
        execution_ids: Vec::new(),
        target_session_id: None,
        target_session_created: false,
        error: None,
        revision: 0,
        updated_at_ms: 111,
    };
    let rows = fixture
        .backend()
        .set_session_input_application_receipt(
            &dispatch_ids,
            &[dispatch.revision],
            &dispatch_receipt,
            111,
        )
        .expect("prepare isolated Session dispatch");
    dispatch_receipt.state = InputApplicationState::Materializing;
    dispatch_receipt.revision = 1;
    dispatch_receipt.updated_at_ms = 112;
    let rows = fixture
        .backend()
        .set_session_input_application_receipt(
            &dispatch_ids,
            &[rows[0].revision],
            &dispatch_receipt,
            112,
        )
        .expect("materialize isolated Session dispatch");
    dispatch_receipt.state = InputApplicationState::Applied;
    dispatch_receipt.summary = "isolated Session handoff completed".to_string();
    dispatch_receipt.execution_ids = vec!["execution-new-session".to_string()];
    dispatch_receipt.target_session_id = Some("session-isolated-contract".to_string());
    dispatch_receipt.target_session_created = true;
    dispatch_receipt.revision = 2;
    dispatch_receipt.updated_at_ms = 113;
    let rows = fixture
        .backend()
        .set_session_input_application_receipt(
            &dispatch_ids,
            &[rows[0].revision],
            &dispatch_receipt,
            113,
        )
        .expect("apply isolated Session dispatch");
    assert_eq!(rows[0].decision, InputRoutingDecision::CreateNewSession);
    assert_eq!(rows[0].status, SessionRuntimeInputStatus::Completed);
    assert_eq!(rows[0].terminal_at_ms, Some(113));
}

/// Proves that the terminal transcript and every supplement it incorporated
/// settle in one transaction. An input beyond the advertised consumed cursor
/// must fence the stale terminal without writing any assistant row.
pub fn terminal_input_cursor_cas(fixture: &mut impl BackendContractFixture) {
    let session_id = "contract-terminal-input-cursor";
    fixture
        .backend()
        .create_session(&record(session_id))
        .expect("create terminal cursor Session");
    let (request, running, token) =
        running_input(fixture.backend(), session_id, "cursor-primary", 100, 10_000);

    let mut supplement = ingress("cursor-supplement", 1);
    supplement.decision = InputRoutingDecision::SupplementCurrentTurn;
    supplement.target_turn_id = Some(request.turn_id.clone());
    append_input(fixture.backend(), session_id, &supplement);
    let supplement_record = fixture
        .backend()
        .get_session_runtime_outbox(&supplement.request_id)
        .expect("read supplement")
        .expect("supplement persists");
    let attached = fixture
        .backend()
        .attach_session_runtime_outbox(
            &supplement_record.input_id,
            supplement_record.session_generation,
            supplement_record.revision,
            &request.turn_id,
            "backend-contract",
            "direct Runtime delivery",
            111,
        )
        .expect("persist direct attachment");
    assert_eq!(attached.status, SessionRuntimeInputStatus::Attached);
    assert!(!attached.status.is_terminal());
    assert_eq!(attached.runtime_commit_cursor, None);
    let related = fixture
        .backend()
        .session_runtime_outbox_for_turn_relation(session_id, 1, &request.turn_id)
        .expect("load exact turn relation");
    assert_eq!(related.len(), 2);
    assert!(related
        .iter()
        .any(|record| record.input_id == running.input_id));
    assert!(related
        .iter()
        .any(|record| record.input_id == attached.input_id));

    let stale = terminal_commit(
        &request,
        &running,
        &token,
        session_id,
        "cursor-primary",
        120,
    );
    assert_stale_fence(
        fixture
            .backend()
            .commit_terminal_transcript_if_fenced(&stale),
    );
    assert_eq!(
        fixture
            .backend()
            .get_message_count(session_id)
            .expect("count transcript after stale cursor"),
        2,
        "only the two user inputs may exist after a stale terminal"
    );

    let mut current = stale;
    current.consumed_input_sequence = attached.sequence;
    let receipt = fixture
        .backend()
        .commit_terminal_transcript_if_fenced(&current)
        .expect("commit terminal at current input cursor");
    assert!(receipt.inserted);
    assert_eq!(receipt.input.status, SessionRuntimeInputStatus::Completed);
    let supplemented = fixture
        .backend()
        .get_session_runtime_outbox(&supplement.request_id)
        .expect("read settled supplement")
        .expect("supplement remains auditable");
    assert_eq!(supplemented.status, SessionRuntimeInputStatus::Supplemented);
    assert_eq!(supplemented.runtime_commit_cursor, Some(42));

    let mut late = ingress("cursor-late-supplement", 1);
    late.decision = InputRoutingDecision::SupplementCurrentTurn;
    late.target_turn_id = Some(request.turn_id.clone());
    append_input(fixture.backend(), session_id, &late);
    let late_record = fixture
        .backend()
        .get_session_runtime_outbox(&late.request_id)
        .expect("read late supplement")
        .expect("late supplement persists");
    let late_attachment = fixture.backend().attach_session_runtime_outbox(
        &late_record.input_id,
        late_record.session_generation,
        late_record.revision,
        &request.turn_id,
        "backend-contract",
        "late direct Runtime delivery",
        130,
    );
    assert!(
        matches!(late_attachment, Err(SessionError::StaleExecutionFence(_))),
        "a terminal target must fence late attachment, got {late_attachment:?}"
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

/// Proves that lifecycle state is durable across backend handle recreation,
/// failed work resumes from its last stable phase, and tombstone publication is
/// atomic and single-shot.
pub fn lifecycle_recovery_and_single_tombstone(fixture: &mut impl BackendContractFixture) {
    let session_id = "contract-lifecycle";
    let operation_id = "session-lifecycle:archive:contract-lifecycle";
    fixture
        .backend()
        .create_session(&record(session_id))
        .expect("create lifecycle Session");
    append_input(
        fixture.backend(),
        session_id,
        &ingress("lifecycle-active", 1),
    );
    let planned = fixture
        .backend()
        .plan_session_lifecycle(&SessionLifecyclePlan {
            operation_id: operation_id.to_string(),
            session_id: session_id.to_string(),
            disposition: SessionCloseDisposition::Archive,
            expected_generation: 1,
            created_at_ms: 110,
        })
        .expect("plan lifecycle");
    assert_eq!(planned.phase, SessionLifecyclePhase::Planned);

    fixture.reopen();
    let planned = fixture
        .backend()
        .get_session_lifecycle_intent(operation_id)
        .expect("read lifecycle")
        .expect("planned lifecycle persists");
    let fenced = fixture
        .backend()
        .fence_session_lifecycle(&SessionLifecycleFenceRequest {
            transition: SessionLifecycleTransition {
                operation_id: operation_id.to_string(),
                expected_revision: planned.revision,
                expected_phase: SessionLifecyclePhase::Planned,
                next_phase: SessionLifecyclePhase::AdmissionFenced,
                updated_at_ms: 120,
                error: None,
            },
            actor: "backend-contract".to_string(),
            reason: "archive".to_string(),
            transitional_status: "archiving".to_string(),
            event: lifecycle_event(session_id, "session.archive_started", 120),
        })
        .expect("fence lifecycle admission");
    let admission = fixture
        .backend()
        .get_session_input_admission(session_id)
        .expect("read fenced admission")
        .expect("Session admission exists");
    assert_eq!(admission.generation, 2);
    assert!(!admission.open);
    assert!(fixture
        .backend()
        .session_runtime_outbox_for_session(session_id, 10)
        .expect("read fenced inputs")
        .iter()
        .all(|input| input.status == SessionRuntimeInputStatus::Expired));

    let failed = fixture
        .backend()
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: operation_id.to_string(),
            expected_revision: fenced.revision,
            expected_phase: SessionLifecyclePhase::AdmissionFenced,
            next_phase: SessionLifecyclePhase::Failed,
            updated_at_ms: 121,
            error: Some("simulated process loss".to_string()),
        })
        .expect("persist lifecycle failure");
    fixture.reopen();
    let recoverable = fixture
        .backend()
        .list_recoverable_session_lifecycle_intents(10)
        .expect("list recoverable lifecycle");
    assert_eq!(recoverable.len(), 1);
    assert_eq!(recoverable[0].operation_id, operation_id);
    assert_eq!(
        recoverable[0].last_stable_phase,
        SessionLifecyclePhase::AdmissionFenced
    );

    let resumed = fixture
        .backend()
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: operation_id.to_string(),
            expected_revision: failed.revision,
            expected_phase: SessionLifecyclePhase::Failed,
            next_phase: SessionLifecyclePhase::AdmissionFenced,
            updated_at_ms: 130,
            error: None,
        })
        .expect("resume lifecycle from stable phase");
    let drained = fixture
        .backend()
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: operation_id.to_string(),
            expected_revision: resumed.revision,
            expected_phase: SessionLifecyclePhase::AdmissionFenced,
            next_phase: SessionLifecyclePhase::RuntimeDrained,
            updated_at_ms: 140,
            error: None,
        })
        .expect("mark Runtime drained");
    fixture.reopen();

    let mut tombstone = fixture
        .backend()
        .get_session(session_id)
        .expect("read Session")
        .expect("Session exists");
    tombstone.status = "archived".to_string();
    tombstone.last_activity = "2026-07-27T00:00:01Z".to_string();
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
        event: lifecycle_event(session_id, "session.archived", 150),
    };
    let committed = fixture
        .backend()
        .commit_session_lifecycle_tombstone(&tombstone_request)
        .expect("commit lifecycle tombstone");
    assert_eq!(committed.phase, SessionLifecyclePhase::TombstoneCommitted);
    fixture.reopen();
    let unloaded = fixture
        .backend()
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
    assert!(fixture
        .backend()
        .list_recoverable_session_lifecycle_intents(10)
        .expect("list completed lifecycle")
        .is_empty());
    assert!(fixture
        .backend()
        .commit_session_lifecycle_tombstone(&tombstone_request)
        .is_err());
    assert_eq!(
        fixture
            .backend()
            .get_events(session_id, 0)
            .expect("read lifecycle events")
            .iter()
            .filter(|event| event.event_type == "session.archived")
            .count(),
        1
    );
    let delete_session_id = "contract-delete-lifecycle";
    let delete_operation_id = "session-lifecycle:delete:contract-delete-lifecycle";
    fixture
        .backend()
        .create_session(&record(delete_session_id))
        .expect("create delete lifecycle Session");
    let planned = fixture
        .backend()
        .plan_session_lifecycle(&SessionLifecyclePlan {
            operation_id: delete_operation_id.to_string(),
            session_id: delete_session_id.to_string(),
            disposition: SessionCloseDisposition::Delete,
            expected_generation: 1,
            created_at_ms: 200,
        })
        .expect("plan delete lifecycle");
    fixture.reopen();
    let fenced = fixture
        .backend()
        .fence_session_lifecycle(&SessionLifecycleFenceRequest {
            transition: SessionLifecycleTransition {
                operation_id: delete_operation_id.to_string(),
                expected_revision: planned.revision,
                expected_phase: SessionLifecyclePhase::Planned,
                next_phase: SessionLifecyclePhase::AdmissionFenced,
                updated_at_ms: 210,
                error: None,
            },
            actor: "backend-contract".to_string(),
            reason: "delete".to_string(),
            transitional_status: "deleting".to_string(),
            event: lifecycle_event(delete_session_id, "session.delete_started", 210),
        })
        .expect("fence delete lifecycle");
    fixture.reopen();
    let drained = fixture
        .backend()
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: delete_operation_id.to_string(),
            expected_revision: fenced.revision,
            expected_phase: SessionLifecyclePhase::AdmissionFenced,
            next_phase: SessionLifecyclePhase::RuntimeDrained,
            updated_at_ms: 220,
            error: None,
        })
        .expect("mark delete Runtime drained");
    fixture.reopen();
    let mut delete_tombstone = fixture
        .backend()
        .get_session(delete_session_id)
        .expect("read delete Session")
        .expect("delete Session exists");
    delete_tombstone.status = "deleted".to_string();
    delete_tombstone.last_activity = "2026-07-27T00:00:02Z".to_string();
    delete_tombstone.metadata_json = Some(r#"{"tombstone":{"kind":"deleted"}}"#.to_string());
    let delete_request = SessionLifecycleTombstoneRequest {
        transition: SessionLifecycleTransition {
            operation_id: delete_operation_id.to_string(),
            expected_revision: drained.revision,
            expected_phase: SessionLifecyclePhase::RuntimeDrained,
            next_phase: SessionLifecyclePhase::TombstoneCommitted,
            updated_at_ms: 230,
            error: None,
        },
        record: delete_tombstone,
        event: lifecycle_event(delete_session_id, "session.deleted", 230),
    };
    let committed = fixture
        .backend()
        .commit_session_lifecycle_tombstone(&delete_request)
        .expect("commit delete tombstone");
    fixture.reopen();
    let unloaded = fixture
        .backend()
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: delete_operation_id.to_string(),
            expected_revision: committed.revision,
            expected_phase: SessionLifecyclePhase::TombstoneCommitted,
            next_phase: SessionLifecyclePhase::Unloaded,
            updated_at_ms: 240,
            error: None,
        })
        .expect("unload deleted Session");
    assert_eq!(unloaded.phase, SessionLifecyclePhase::Unloaded);
    assert!(fixture
        .backend()
        .commit_session_lifecycle_tombstone(&delete_request)
        .is_err());
    assert_eq!(
        fixture
            .backend()
            .get_events(delete_session_id, 0)
            .expect("read delete lifecycle events")
            .iter()
            .filter(|event| event.event_type == "session.deleted")
            .count(),
        1
    );
}

fn message(session_id: &str, sequence: usize) -> SessionMessage {
    SessionMessage {
        stable_message_id: format!("source-message-{sequence}"),
        session_id: session_id.to_string(),
        sequence,
        role: "user".to_string(),
        content_json: format!(r#"[{{"type":"text","text":"{sequence}"}}]"#),
        blocks_count: 1,
        tool_use_id: None,
        tool_name: None,
        token_usage_json: None,
        created_at_ms: 100 + sequence as u64,
    }
}

/// Proves that a branch operation is bound to one immutable source cutoff.
/// Retries return the existing target even after the source grows, while the
/// activation receipt remains recoverable until explicitly activated.
pub fn branch_activation_and_idempotent_cutoff(fixture: &mut impl BackendContractFixture) {
    let source = "contract-branch-source";
    let target = "contract-branch-target";
    let operation_id = "session-branch:contract-source:contract-target:2";
    fixture
        .backend()
        .create_session(&record(source))
        .expect("create branch source");
    for sequence in 0..2 {
        fixture
            .backend()
            .insert_message(&message(source, sequence))
            .expect("insert source message");
    }
    let request = SessionBranchRequest {
        operation_id: operation_id.to_string(),
        source_session_id: source.to_string(),
        source_message_count: 2,
        target: record(target),
        source_event_json: r#"{"kind":"session.branched"}"#.to_string(),
        target_event_json: r#"{"kind":"session.branch_created"}"#.to_string(),
        created_at_ms: 200,
    };
    let first = fixture
        .backend()
        .branch_session_at_cutoff(&request)
        .expect("commit branch transaction");
    assert_eq!(first.copied_message_count, 2);
    assert_eq!(
        first.activation.phase,
        SessionBranchActivationPhase::BranchCommitted
    );

    fixture.reopen();
    fixture
        .backend()
        .insert_message(&message(source, 2))
        .expect("grow source after branch commit");
    let replay = fixture
        .backend()
        .branch_session_at_cutoff(&request)
        .expect("replay exact branch operation");
    assert_eq!(replay.target.session_id, target);
    assert_eq!(replay.copied_message_count, 2);
    assert_eq!(replay.activation, first.activation);
    assert!(fixture
        .backend()
        .list_recoverable_session_branch_activations(10)
        .expect("branch-committed receipt survives restart")
        .iter()
        .any(|activation| {
            activation.operation_id == operation_id
                && activation.phase == SessionBranchActivationPhase::BranchCommitted
        }));
    assert_eq!(
        fixture
            .backend()
            .get_message_count(source)
            .expect("count grown source"),
        3
    );
    assert_eq!(
        fixture
            .backend()
            .get_message_count(target)
            .expect("count immutable branch target"),
        2
    );

    let mut conflicting_retry = request.clone();
    conflicting_retry.source_message_count = 3;
    assert!(fixture
        .backend()
        .branch_session_at_cutoff(&conflicting_retry)
        .is_err());
    let activation_pending = fixture
        .backend()
        .transition_session_branch_activation(&SessionBranchActivationTransition {
            operation_id: operation_id.to_string(),
            expected_revision: replay.activation.revision,
            expected_phase: SessionBranchActivationPhase::BranchCommitted,
            next_phase: SessionBranchActivationPhase::ActivationPending,
            updated_at_ms: 205,
            error: None,
        })
        .expect("fence Gateway activation after durable branch commit");
    let failed = fixture
        .backend()
        .transition_session_branch_activation(&SessionBranchActivationTransition {
            operation_id: operation_id.to_string(),
            expected_revision: activation_pending.revision,
            expected_phase: SessionBranchActivationPhase::ActivationPending,
            next_phase: SessionBranchActivationPhase::Failed,
            updated_at_ms: 210,
            error: Some("simulated activation loss".to_string()),
        })
        .expect("persist branch activation failure");

    fixture.reopen();
    let recoverable = fixture
        .backend()
        .list_recoverable_session_branch_activations(10)
        .expect("list recoverable branch activations");
    assert_eq!(recoverable.len(), 1);
    assert_eq!(recoverable[0].operation_id, operation_id);
    let pending = fixture
        .backend()
        .transition_session_branch_activation(&SessionBranchActivationTransition {
            operation_id: operation_id.to_string(),
            expected_revision: failed.revision,
            expected_phase: SessionBranchActivationPhase::Failed,
            next_phase: SessionBranchActivationPhase::ActivationPending,
            updated_at_ms: 220,
            error: None,
        })
        .expect("retry branch activation");
    let activated = fixture
        .backend()
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
    assert!(fixture
        .backend()
        .list_recoverable_session_branch_activations(10)
        .expect("list completed branch activations")
        .is_empty());
    assert_eq!(
        fixture
            .backend()
            .get_events(source, 0)
            .expect("read source branch event")
            .iter()
            .filter(|event| event.event_type == "SessionBranched")
            .count(),
        1
    );
    assert_eq!(
        fixture
            .backend()
            .get_events(target, 0)
            .expect("read target branch event")
            .iter()
            .filter(|event| event.event_type == "BranchCreated")
            .count(),
        1
    );
}

pub fn domain_event_idempotency_and_kind_query(fixture: &mut impl BackendContractFixture) {
    let session_id = "contract-domain-event";
    fixture
        .backend()
        .create_session(&record(session_id))
        .expect("create domain-event Session");
    let mut outcome = SessionDomainEvent::new(
        session_id,
        0,
        SessionDomainScope::ApplicationTask,
        "application.execution_outcome",
        serde_json::json!({
            "contract_version": 1,
            "outcome_id": "outcome-1",
            "summary": "bounded outcome"
        }),
        500,
    );
    outcome.event_id = "application-execution:v1:producer-a:outcome-1".to_string();
    let wire = outcome.to_session_event().expect("encode domain event");

    let (first, first_replayed) = fixture
        .backend()
        .append_session_domain_event_if_absent_allocating_sequence(&wire, &outcome.event_id)
        .expect("append first domain outcome");
    assert!(!first_replayed);
    let (replay, replayed) = fixture
        .backend()
        .append_session_domain_event_if_absent_allocating_sequence(&wire, &outcome.event_id)
        .expect("replay domain outcome");
    assert!(replayed);
    assert_eq!(replay.sequence, first.sequence);

    let mut conflicting = outcome.clone();
    conflicting.payload["summary"] = serde_json::json!("different bounded outcome");
    let conflict = fixture
        .backend()
        .append_session_domain_event_if_absent_allocating_sequence(
            &conflicting
                .to_session_event()
                .expect("encode conflicting domain outcome"),
            &outcome.event_id,
        )
        .expect_err("same idempotency key with different content must conflict");
    assert!(matches!(
        conflict,
        SessionError::IdempotencyConflict {
            namespace: "session_domain_event",
            ..
        }
    ));

    let mut other_producer = outcome.clone();
    other_producer.event_id = "application-execution:v1:producer-b:outcome-1".to_string();
    let (other_producer, other_replayed) = fixture
        .backend()
        .append_session_domain_event_if_absent_allocating_sequence(
            &other_producer
                .to_session_event()
                .expect("encode second producer outcome"),
            &other_producer.event_id,
        )
        .expect("different producer may write the same outcome id");
    assert!(!other_replayed);
    assert_ne!(other_producer.sequence, first.sequence);

    let other = SessionDomainEvent::new(
        session_id,
        0,
        SessionDomainScope::Context,
        "context.recommendation_action",
        serde_json::json!({"recommendation": "retain evidence"}),
        501,
    )
    .to_session_event()
    .expect("encode context event");
    fixture
        .backend()
        .append_event_allocating_sequence(&other)
        .expect("append other domain kind");

    assert_eq!(
        fixture
            .backend()
            .count_session_domain_events_by_kind_from(
                session_id,
                "application.execution_outcome",
                0,
            )
            .expect("count application outcomes"),
        2
    );
    let queried = fixture
        .backend()
        .get_session_domain_events_by_kind_limited(
            session_id,
            "application.execution_outcome",
            0,
            10,
        )
        .expect("query application outcomes");
    assert_eq!(queried.len(), 2);
    assert_eq!(queried[0].sequence, first.sequence);

    fixture.reopen();
    let (after_restart, replayed_after_restart) = fixture
        .backend()
        .append_session_domain_event_if_absent_allocating_sequence(&wire, &outcome.event_id)
        .expect("replay domain outcome after restart");
    assert!(replayed_after_restart);
    assert_eq!(after_restart.sequence, first.sequence);
}

pub fn application_execution_32_way_semantic_idempotency(
    fixture: &mut impl BackendContractFixture,
) {
    const CONCURRENCY: usize = 32;
    let session_id = "contract-app-idempotency-concurrent";
    fixture
        .backend()
        .create_session(&record(session_id))
        .expect("create concurrent APP outcome Session");

    let mut outcome = SessionDomainEvent::new(
        session_id,
        0,
        SessionDomainScope::ApplicationTask,
        "application.execution_outcome",
        serde_json::json!({
            "contract_version": 1,
            "outcome_id": "shared-outcome",
            "summary": "one canonical result",
            "refs": [
                {"type": "evidence", "id": "evidence-1"},
                {"type": "metric", "id": "metric-1"}
            ]
        }),
        700,
    );
    outcome.event_id = "application-execution:v1:producer-concurrent:shared-outcome".to_string();
    let event = Arc::new(
        outcome
            .to_session_event()
            .expect("encode concurrent APP outcome"),
    );
    let event_id = Arc::new(outcome.event_id.clone());
    let backend = fixture.shared_backend();
    let barrier = Arc::new(Barrier::new(CONCURRENCY));
    let mut threads = Vec::with_capacity(CONCURRENCY);

    for _ in 0..CONCURRENCY {
        let backend = Arc::clone(&backend);
        let barrier = Arc::clone(&barrier);
        let event = Arc::clone(&event);
        let event_id = Arc::clone(&event_id);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            backend.append_session_domain_event_if_absent_allocating_sequence(
                event.as_ref(),
                event_id.as_str(),
            )
        }));
    }

    let mut receipts = Vec::with_capacity(CONCURRENCY);
    for thread in threads {
        receipts.push(
            thread
                .join()
                .expect("concurrent APP outcome writer must not panic")
                .expect("identical concurrent APP outcome must append or replay"),
        );
    }
    assert_eq!(receipts.iter().filter(|(_, replayed)| !replayed).count(), 1);
    assert_eq!(
        receipts.iter().filter(|(_, replayed)| *replayed).count(),
        CONCURRENCY - 1
    );
    let sequence = receipts[0].0.sequence;
    assert!(receipts
        .iter()
        .all(|(stored, _)| stored.sequence == sequence));
    assert_eq!(
        backend
            .count_session_domain_events_by_kind_from(
                session_id,
                "application.execution_outcome",
                0,
            )
            .expect("count concurrent APP outcomes"),
        1
    );

    let mut conflicting = outcome.clone();
    conflicting.payload["summary"] = serde_json::json!("conflicting result");
    assert!(matches!(
        backend.append_session_domain_event_if_absent_allocating_sequence(
            &conflicting
                .to_session_event()
                .expect("encode concurrent conflict"),
            &conflicting.event_id,
        ),
        Err(SessionError::IdempotencyConflict {
            namespace: "session_domain_event",
            ..
        })
    ));

    let mut other_producer = outcome;
    other_producer.event_id =
        "application-execution:v1:producer-independent:shared-outcome".to_string();
    let (_, replayed) = backend
        .append_session_domain_event_if_absent_allocating_sequence(
            &other_producer
                .to_session_event()
                .expect("encode independent producer"),
            &other_producer.event_id,
        )
        .expect("independent producer must commit");
    assert!(!replayed);
    assert_eq!(
        backend
            .count_session_domain_events_by_kind_from(
                session_id,
                "application.execution_outcome",
                0,
            )
            .expect("count producer-scoped APP outcomes"),
        2
    );
}
