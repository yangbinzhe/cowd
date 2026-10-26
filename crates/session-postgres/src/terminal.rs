//! Terminal operations for the PostgresSessionStore adapter.

use super::*;

fn lock_claim_event_append_tx(
    transaction: &mut PostgresTransaction<'_>,
    request: &SessionClaimEventAppend,
) -> session::SessionResult<SessionEvent> {
    let fence = &request.fence;
    if [
        request.session_id.as_str(),
        request.ingress_message_id.as_str(),
        request.turn_id.as_str(),
        fence.request_id.as_str(),
        fence.claim_owner.as_str(),
        fence.claim_token.as_str(),
        request.event.event_id.as_str(),
    ]
    .iter()
    .any(|identity| identity.trim().is_empty())
        || fence.session_generation == 0
        || fence.claim_fence_epoch == 0
        || request.event.session_id != request.session_id
    {
        return Err(session::SessionError::InvalidArgument(
            "claim-fenced event append requires complete, matching Session identity".to_string(),
        ));
    }
    let event = request.event.to_session_event()?;
    // Session first, exact outbox second. Only this owner operation uses
    // NO KEY UPDATE: it excludes admission/generation updates and other event
    // allocators, but permits outbox-first workers' parent FK KEY SHARE locks.
    // Those workers read Session without a row lock and update outbox/history/
    // timeline/recovery rows, not session_records. FOR UPDATE here would add
    // a real Session -> outbox -> Session-FK inverse wait.
    transaction
        .query_opt(
            "SELECT session_id FROM session_records WHERE session_id=$1 FOR NO KEY UPDATE",
            &[&request.session_id],
        )
        .map_err(postgres_error)?
        .ok_or_else(|| {
            session::SessionError::StaleExecutionFence(format!(
                "session `{}` no longer exists",
                request.session_id
            ))
        })?;
    let admission =
        query_input_admission_tx(transaction, &request.session_id, false)?.ok_or_else(|| {
            session::SessionError::StaleExecutionFence(format!(
                "session `{}` no longer exists",
                request.session_id
            ))
        })?;
    let current = runtime_outbox_for_update(transaction, &fence.request_id)?;
    // PostgreSQL's wall clock is sampled after BOTH lock waits. Event time,
    // caller time, transaction_timestamp and mutable row revision are not
    // authority for this decision. Expiry is checked at admission to append.
    let now_ms = i64_to_u64(
        transaction
            .query_one(
                "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
                &[],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?,
        "claim append owner clock",
    )?;
    if current.session_id != request.session_id
        || current.message_id != request.ingress_message_id
        || current.turn_id != request.turn_id
        || current.request_id != fence.request_id
        || current.sequence != fence.input_sequence
        || current.session_generation != fence.session_generation
        || admission.session_id != request.session_id
        || admission.generation != fence.session_generation
        || !admission.open
        || !matches!(
            current.status,
            SessionRuntimeInputStatus::Claimed | SessionRuntimeInputStatus::Running
        )
        || current.claim_owner.as_deref() != Some(fence.claim_owner.as_str())
        || current.claim_token.as_deref() != Some(fence.claim_token.as_str())
        || current.claim_fence_epoch != Some(fence.claim_fence_epoch)
        || !current
            .claim_expires_at_ms
            .is_some_and(|expiry| expiry > now_ms)
    {
        return Err(session::SessionError::StaleExecutionFence(format!(
            "claim event request={} generation={} claim_fence_epoch={} current_status={:?}",
            fence.request_id, fence.session_generation, fence.claim_fence_epoch, current.status
        )));
    }
    request.validate_locked_event_binding()?;
    Ok(event)
}

/// Original event-ID replay comparison, shared by both append operations.
/// Only the store-allocated sequence may differ; changed payloads conflict.
fn matching_domain_event_tx(
    transaction: &mut PostgresTransaction<'_>,
    event: &SessionEvent,
    event_id: &str,
) -> session::SessionResult<Option<SessionEvent>> {
    let Some(row) = transaction
        .query_opt(
            "SELECT session_id, event_type, event_json, sequence, created_at_ms
               FROM session_events
              WHERE session_id=$1
                AND event_type=$2
                AND event_json::jsonb ->> 'event_id'=$3
              LIMIT 1",
            &[
                &event.session_id,
                &session::SESSION_DOMAIN_EVENT_TYPE,
                &event_id,
            ],
        )
        .map_err(postgres_error)?
    else {
        return Ok(None);
    };
    let existing = row_to_event(&row)?;
    if !SessionDomainEvent::semantically_equivalent(&existing, event).map_err(|error| {
        session::SessionError::Store(format!(
            "failed to compare idempotent session-domain event content: {error}"
        ))
    })? {
        return Err(session::SessionError::IdempotencyConflict {
            namespace: "session_domain_event",
            key: event_id.to_string(),
        });
    }
    Ok(Some(existing))
}

fn append_claim_event_tx(
    transaction: &mut PostgresTransaction<'_>,
    request: &SessionClaimEventAppend,
) -> session::SessionResult<SessionEvent> {
    let event = lock_claim_event_append_tx(transaction, request)?;
    match matching_domain_event_tx(transaction, &event, &request.event.event_id)? {
        Some(existing) => Ok(existing),
        None => append_locked_claim_event_unchecked_tx(transaction, &event),
    }
}

#[cfg(test)]
mod claim_append_tests {
    use super::*;
    use crate::claim_append_postgres_scope::PostgresTestScope;
    use std::sync::{Arc, Barrier};
    use std::time::{Duration, Instant};

    struct Fixture {
        scope: PostgresTestScope,
        store: Arc<PostgresSessionStore>,
        request: SessionClaimEventAppend,
        input: SessionRuntimeOutboxRecord,
    }

    fn owner_now(store: &PostgresSessionStore) -> u64 {
        let mut connection = store.executor.checkout_critical().unwrap();
        let value: i64 = connection
            .query_one(
                "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
                &[],
            )
            .unwrap()
            .get(0);
        u64::try_from(value).unwrap()
    }

    impl Fixture {
        fn new() -> Self {
            let scope = PostgresTestScope::new();
            let store = Arc::new(PostgresSessionStore::new(scope.reconnect()).unwrap());
            let session_id = "r194-claim-append";
            store
                .create_session(&SessionRecord {
                    session_id: session_id.into(),
                    platform: "test".into(),
                    chat_id: "chat".into(),
                    user_id: None,
                    model: None,
                    created_at: "2026-10-26T00:00:00Z".into(),
                    last_activity: "2026-10-26T00:00:00Z".into(),
                    message_count: 0,
                    reset_policy: "manual".into(),
                    metadata_json: None,
                    input_tokens: 0,
                    output_tokens: 0,
                    status: "active".into(),
                })
                .unwrap();
            let now = owner_now(&store);
            let ingress = SessionRuntimeOutboxRequest {
                input_id: "input-r194".into(),
                request_id: "request-r194".into(),
                message_id: "message-r194".into(),
                turn_id: "turn-r194".into(),
                session_generation: 1,
                decision: InputRoutingDecision::StartNewTurn,
                target_turn_id: None,
                classification_json: None,
                task_route_hint: None,
                created_at_ms: now,
                runtime_options_json: None,
            };
            store
                .append_ingress_with_runtime_outbox(session_id, "user", Some("[]"), now, &ingress)
                .unwrap();
            let input = store
                .claim_session_runtime_outbox("worker-r194", now, 60_000, 1, &[])
                .unwrap()
                .remove(0);
            let request = SessionClaimEventAppend::observation(
                session_id.into(),
                ingress.message_id,
                ingress.turn_id,
                session::SessionTerminalExecutionFence {
                    request_id: ingress.request_id,
                    input_sequence: input.sequence,
                    session_generation: 1,
                    claim_owner: input.claim_owner.clone().unwrap(),
                    claim_token: input.claim_token.clone().unwrap(),
                    claim_fence_epoch: input.claim_fence_epoch.unwrap(),
                },
                SessionDomainEvent::new(
                    session_id,
                    0,
                    SessionDomainScope::Tool,
                    "r194.observation",
                    serde_json::json!({
                        "physical": "original", "content_digest": "sha256:original"
                    }),
                    now,
                ),
            );
            Self {
                scope,
                store,
                request,
                input,
            }
        }

        fn count(&self) -> i64 {
            // Independent executor/connection, direct SQL oracle, no projection.
            let executor = self.scope.reconnect();
            let mut connection = executor.checkout_critical().unwrap();
            connection
                .query_one(
                    "SELECT count(*) FROM session_events WHERE session_id=$1
                 AND event_json::jsonb ->> 'event_id'=$2",
                    &[&self.request.session_id, &self.request.event.event_id],
                )
                .unwrap()
                .get(0)
        }

        fn revoke(&self) {
            self.store
                .cancel_session_runtime_outbox(
                    &self.input.input_id,
                    1,
                    self.input.revision,
                    "r194-test",
                    "revoke",
                    owner_now(&self.store),
                )
                .unwrap();
        }
    }

    #[test]
    #[ignore = "requires parent-owned isolated COWD_TEST_POSTGRES_URL"]
    fn r194_claim_append_identity_renew_and_exact_replay() {
        let fixture = Fixture::new();
        for field in 0..14 {
            let mut bad = fixture.request.clone();
            match field {
                0 => bad.session_id = "foreign-session".into(),
                1 => bad.ingress_message_id = "foreign-message".into(),
                2 => bad.turn_id = "foreign-turn".into(),
                3 => bad.fence.request_id = "foreign-request".into(),
                4 => bad.fence.input_sequence += 1,
                5 => bad.fence.session_generation += 1,
                6 => bad.fence.claim_owner = "foreign-owner".into(),
                7 => bad.fence.claim_token = "foreign-token".into(),
                8 => bad.fence.claim_fence_epoch += 1,
                9 => bad.event.session_id = "foreign-event-session".into(),
                10 => bad.event.event_id = " ".into(),
                11 => bad.fence.claim_owner.clear(),
                12 => bad.fence.session_generation = 0,
                _ => bad.fence.claim_fence_epoch = 0,
            }
            assert!(fixture
                .store
                .append_session_domain_event_if_claim_current(&bad)
                .is_err());
            assert_eq!(fixture.count(), 0);
        }
        let running = fixture
            .store
            .mark_session_runtime_outbox_running(
                &fixture.input.request_id,
                "worker-r194",
                1,
                fixture.input.claim_token.as_deref().unwrap(),
                fixture.input.revision,
                owner_now(&fixture.store),
            )
            .unwrap();
        let renewed = fixture
            .store
            .renew_session_runtime_outbox_lease(
                &running.request_id,
                "worker-r194",
                1,
                running.claim_token.as_deref().unwrap(),
                running.revision,
                owner_now(&fixture.store),
                60_000,
            )
            .unwrap();
        assert!(renewed.revision > fixture.input.revision);
        assert_eq!(
            renewed.claim_fence_epoch,
            Some(fixture.request.fence.claim_fence_epoch)
        );
        let stored = fixture
            .store
            .append_session_domain_event_if_claim_current(&fixture.request)
            .unwrap();
        let mut replay = fixture.request.clone();
        replay.event.sequence = 9999;
        assert_eq!(
            fixture
                .store
                .append_session_domain_event_if_claim_current(&replay)
                .unwrap(),
            stored
        );
        replay.event.payload = serde_json::json!({"physical": "changed"});
        assert!(matches!(
            fixture
                .store
                .append_session_domain_event_if_claim_current(&replay),
            Err(session::SessionError::IdempotencyConflict { .. })
        ));
        let mut changed_digest = fixture.request.clone();
        changed_digest.event.payload["content_digest"] = serde_json::json!("sha256:changed");
        assert!(matches!(
            fixture
                .store
                .append_session_domain_event_if_claim_current(&changed_digest),
            Err(session::SessionError::IdempotencyConflict { .. })
        ));
        assert_eq!(fixture.count(), 1);
        fixture
            .store
            .cancel_session_runtime_outbox(
                &renewed.input_id,
                1,
                renewed.revision,
                "r194-test",
                "revoke",
                owner_now(&fixture.store),
            )
            .unwrap();
        assert!(fixture
            .store
            .append_session_domain_event_if_claim_current(&fixture.request)
            .is_err());
        assert_eq!(fixture.count(), 1);
    }

    #[test]
    #[ignore = "requires parent-owned isolated COWD_TEST_POSTGRES_URL"]
    fn r194_claim_append_revoke_close_expiry_and_reclaim_reject() {
        for invalidation in 0..4 {
            let fixture = Fixture::new();
            match invalidation {
                0 => fixture.revoke(),
                1 => {
                    fixture
                        .store
                        .close_session_input_admission(
                            &fixture.request.session_id,
                            1,
                            "r194-test",
                            "close",
                            owner_now(&fixture.store),
                        )
                        .unwrap();
                }
                _ => {
                    // Force actual owner-clock expiry without a timing sleep.
                    let executor = fixture.scope.reconnect();
                    executor.checkout_critical().unwrap().execute(
                        "UPDATE session_runtime_outbox SET claim_expires_at_ms=0 WHERE request_id=$1",
                        &[&fixture.input.request_id],
                    ).unwrap();
                    if invalidation == 3 {
                        let reclaimed = fixture
                            .store
                            .claim_session_runtime_outbox(
                                "replacement-r194",
                                owner_now(&fixture.store),
                                60_000,
                                1,
                                &[],
                            )
                            .unwrap()
                            .remove(0);
                        assert_ne!(reclaimed.claim_fence_epoch, fixture.input.claim_fence_epoch);
                    }
                }
            }
            assert!(fixture
                .store
                .append_session_domain_event_if_claim_current(&fixture.request)
                .is_err());
            assert_eq!(fixture.count(), 0);
        }
    }

    #[test]
    #[ignore = "requires parent-owned isolated COWD_TEST_POSTGRES_URL"]
    fn r194_claim_append_verified_then_revoke_cross_connection_zero_insert() {
        let fixture = Fixture::new();
        // Verification completes in its own transaction, as an earlier
        // Runtime check would. The retained request has no lock authority.
        let mut connection = fixture.store.executor.checkout_critical().unwrap();
        let mut tx = connection.transaction().unwrap();
        lock_claim_event_append_tx(&mut tx, &fixture.request).unwrap();
        tx.commit().unwrap();
        drop(connection);
        let executor = fixture.scope.reconnect();
        let gate = Arc::new(Barrier::new(2));
        let worker_gate = gate.clone();
        let input = fixture.input.clone();
        let revoker = std::thread::spawn(move || {
            let store = PostgresSessionStore::new(executor).unwrap();
            store
                .cancel_session_runtime_outbox(
                    &input.input_id,
                    1,
                    input.revision,
                    "r194-test",
                    "revoke after verification",
                    owner_now(&store),
                )
                .unwrap();
            worker_gate.wait();
        });
        gate.wait();
        assert!(matches!(
            fixture
                .store
                .append_session_domain_event_if_claim_current(&fixture.request),
            Err(session::SessionError::StaleExecutionFence(_))
        ));
        revoker.join().unwrap();
        assert_eq!(fixture.count(), 0);
    }

    #[test]
    #[ignore = "requires parent-owned isolated COWD_TEST_POSTGRES_URL"]
    fn r194_claim_append_commit_connection_loss_returns_no_ack() {
        let fixture = Fixture::new();
        let executor = fixture.scope.reconnect();
        let mut oracle = executor.checkout_critical().unwrap();
        // Fixture-local deferred trigger severs the actual append connection
        // at COMMIT. The production wrapper must return its commit error.
        oracle.batch_execute(
            "CREATE FUNCTION r194_lose_commit_connection() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.event_json::jsonb ->> 'kind' = 'r194.observation' THEN
                     PERFORM pg_terminate_backend(pg_backend_pid());
                 END IF;
                 RETURN NEW;
             END $$;
             CREATE CONSTRAINT TRIGGER r194_lose_commit_connection
             AFTER INSERT ON session_events DEFERRABLE INITIALLY DEFERRED
             FOR EACH ROW EXECUTE FUNCTION r194_lose_commit_connection()",
        ).unwrap();
        assert!(fixture
            .store
            .append_session_domain_event_if_claim_current(&fixture.request)
            .is_err());
        assert_eq!(fixture.count(), 0);
        oracle
            .batch_execute(
                "DROP TRIGGER r194_lose_commit_connection ON session_events;
                              DROP FUNCTION r194_lose_commit_connection()",
            )
            .unwrap();
        // Model a durable commit whose acknowledgment was lost by discarding
        // its result, then retry the stable identity through the real method.
        drop(
            fixture
                .store
                .append_session_domain_event_if_claim_current(&fixture.request)
                .unwrap(),
        );
        fixture
            .store
            .append_session_domain_event_if_claim_current(&fixture.request)
            .unwrap();
        assert_eq!(fixture.count(), 1);
        fixture.revoke();
        assert!(fixture
            .store
            .append_session_domain_event_if_claim_current(&fixture.request)
            .is_err());
        assert_eq!(fixture.count(), 1);
    }

    fn wait_until_blocked(executor: &PostgresExecutor, holder: i32) {
        let mut observer = executor.checkout_online_read().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let blocked: bool = observer
                .query_one(
                    "SELECT EXISTS (
                    SELECT 1 FROM pg_stat_activity
                     WHERE $1 = ANY(pg_blocking_pids(pid))
                       AND (query LIKE '%session_runtime_outbox%'
                            OR query LIKE '%session_records%')
                 )",
                    &[&holder],
                )
                .unwrap()
                .get(0);
            if blocked {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "cross-connection lock wait was not observed"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    #[ignore = "requires parent-owned isolated COWD_TEST_POSTGRES_URL"]
    fn r194_claim_append_held_then_revoke_waits_cross_connection() {
        let fixture = Fixture::new();
        let executor = fixture.scope.reconnect();
        let revoker_store = PostgresSessionStore::new(executor).unwrap();
        let mut holder = fixture.store.executor.checkout_critical().unwrap();
        let holder_pid: i32 = holder
            .query_one("SELECT pg_backend_pid()", &[])
            .unwrap()
            .get(0);
        let mut tx = holder.transaction().unwrap();
        // Hold the exact production predicate locks; the append below executes
        // the same full transaction body as the public backend operation.
        lock_claim_event_append_tx(&mut tx, &fixture.request).unwrap();
        let gate = Arc::new(Barrier::new(2));
        let input_id = fixture.input.input_id.clone();
        let revision = fixture.input.revision;
        let worker_gate = gate.clone();
        let revoker = std::thread::spawn(move || {
            let now = owner_now(&revoker_store);
            worker_gate.wait();
            revoker_store
                .cancel_session_runtime_outbox(&input_id, 1, revision, "r194-test", "revoke", now)
                .unwrap();
        });
        gate.wait();
        wait_until_blocked(&fixture.scope.reconnect(), holder_pid);
        append_claim_event_tx(&mut tx, &fixture.request).unwrap();
        tx.commit().unwrap();
        revoker.join().unwrap();
        assert_eq!(fixture.count(), 1);
        let mut fresh = fixture.request.clone();
        fresh.event.event_id.push_str("-after-revoke");
        assert!(fixture
            .store
            .append_session_domain_event_if_claim_current(&fresh)
            .is_err());
        assert!(fixture
            .store
            .get_session_domain_event_by_id(&fresh.session_id, &fresh.event.event_id)
            .unwrap()
            .is_none());
    }

    #[test]
    #[ignore = "requires parent-owned isolated COWD_TEST_POSTGRES_URL"]
    fn r194_claim_append_outbox_first_worker_fk_progresses() {
        let fixture = Fixture::new();
        let executor = fixture.scope.reconnect();
        let append_store = PostgresSessionStore::new(executor).unwrap();
        let now = owner_now(&fixture.store);
        let mut worker = fixture.store.executor.checkout_critical().unwrap();
        let worker_pid: i32 = worker
            .query_one("SELECT pg_backend_pid()", &[])
            .unwrap()
            .get(0);
        let mut tx = worker.transaction().unwrap();
        tx.batch_execute("SET LOCAL lock_timeout='3s'").unwrap();
        let current =
            runtime_outbox_by_input_id_for_update(&mut tx, &fixture.input.input_id).unwrap();
        let request = fixture.request.clone();
        let gate = Arc::new(Barrier::new(2));
        let append_gate = gate.clone();
        let appender = std::thread::spawn(move || {
            append_gate.wait();
            assert!(matches!(
                append_store.append_session_domain_event_if_claim_current(&request),
                Err(session::SessionError::StaleExecutionFence(_))
            ));
        });
        gate.wait();
        // The actual public append now owns its Session lock and waits on
        // this worker's outbox. Continue the original cancel SQL/helper chain
        // on the independently held worker connection, including its triggers.
        wait_until_blocked(&fixture.scope.reconnect(), worker_pid);
        let changed = tx
            .execute(
                "UPDATE session_runtime_outbox
                SET status='cancelled',claim_owner=NULL,claim_token=NULL,claim_fence_epoch=NULL,
                    claim_expires_at_ms=NULL,last_error=$1,terminal_at_ms=$2,
                    updated_at_ms=$2,revision=revision+1
              WHERE input_id=$3 AND session_generation=$4 AND revision=$5
                AND status NOT IN ('rejected_duplicate','rejected_policy','completed',
                                   'supplemented','failed','cancelled','expired')",
                &[
                    &"r194-fk-progress",
                    &to_u64_i64(now, "test clock").unwrap(),
                    &current.input_id,
                    &to_u64_i64(current.session_generation, "test generation").unwrap(),
                    &to_u64_i64(current.revision, "test revision").unwrap(),
                ],
            )
            .unwrap();
        assert_eq!(changed, 1);
        let updated = runtime_outbox_by_input_id_for_update(&mut tx, &current.input_id).unwrap();
        append_runtime_history_tx(
            &mut tx,
            &updated,
            "cancel",
            Some("r194-test"),
            Some(current.revision),
            current.status,
            SessionRuntimeInputStatus::Cancelled,
            Some("r194-fk-progress"),
            now,
        )
        .unwrap();
        append_input_timeline_event_tx(
            &mut tx,
            &request_from_outbox(&updated),
            &updated.session_id,
            updated.sequence,
            "session.input.cancelled.v1",
            updated.status,
            Some("r194-test"),
            Some("r194-fk-progress"),
            now,
        )
        .unwrap();
        tx.commit().unwrap();
        appender.join().unwrap();
        assert_eq!(fixture.count(), 0);
    }

    #[test]
    #[ignore = "requires parent-owned isolated COWD_TEST_POSTGRES_URL"]
    fn r194_claim_append_waits_then_rechecks_owner_clock_cross_connection() {
        let fixture = Fixture::new();
        let mut holder = fixture.store.executor.checkout_critical().unwrap();
        let holder_pid: i32 = holder
            .query_one("SELECT pg_backend_pid()", &[])
            .unwrap()
            .get(0);
        let mut tx = holder.transaction().unwrap();
        query_input_admission_tx(&mut tx, &fixture.request.session_id, true)
            .unwrap()
            .unwrap();
        let executor = fixture.scope.reconnect();
        let request = fixture.request.clone();
        let gate = Arc::new(Barrier::new(2));
        let worker_gate = gate.clone();
        let appender = std::thread::spawn(move || {
            let store = PostgresSessionStore::new(executor).unwrap();
            worker_gate.wait();
            let result = store.append_session_domain_event_if_claim_current(&request);
            assert!(matches!(
                result,
                Err(session::SessionError::StaleExecutionFence(_))
            ));
        });
        gate.wait();
        wait_until_blocked(&fixture.scope.reconnect(), holder_pid);
        // Same tuple, no revision/epoch mutation. Set expiry to the owner's
        // current clock AFTER the append has begun waiting. A clock sampled
        // before that wait must not authorize insertion after lock release.
        // Hold Session first: outbox updates have recovery/FK side effects.
        tx.execute("UPDATE session_runtime_outbox
                    SET claim_expires_at_ms=floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
                    WHERE request_id=$1",
            &[&fixture.input.request_id]).unwrap();
        tx.commit().unwrap();
        appender.join().unwrap();
        assert_eq!(fixture.count(), 0);
    }

    fn authorization(fixture: &Fixture) -> SessionClaimEventAppend {
        use harness_contract::turn::{CancellationCause, CancellationReceipt, CancellationStatus};
        SessionClaimEventAppend::cancellation_authorization(
            fixture.request.session_id.clone(),
            fixture.request.ingress_message_id.clone(),
            fixture.request.turn_id.clone(),
            fixture.request.fence.clone(),
            CancellationReceipt {
                cancellation_id: "r196-cancel".into(),
                session_id: fixture.request.session_id.clone(),
                turn_id: fixture.request.turn_id.clone(),
                execution_id: "original-execution".into(),
                actor_id: "original-actor".into(),
                cause: CancellationCause::UserRequested,
                reason: Some("original-reason".into()),
                requested_at_ms: fixture.request.event.created_at_ms,
                effective_at_ms: None,
                status: CancellationStatus::Requested,
                journal_sequence: 3,
                projection_revision: 4,
            },
            session::SessionCancellationRequestedReference {
                event_id: "original-requested".into(),
                stream_id: "original-stream".into(),
                event_sequence: 3,
                commit_cursor: 4,
                payload_digest: format!("sha256:{}", "a".repeat(64)),
            },
        )
        .unwrap()
    }

    #[test]
    #[ignore = "requires parent-owned isolated COWD_TEST_POSTGRES_URL"]
    fn r196_authorization_reserved_ordinary_writers_reject_before_readback() {
        fn reserved_refusal<T>(result: session::SessionResult<T>) {
            assert!(
                matches!(result,
                Err(session::SessionError::InvalidArgument(reason))
                    if reason.contains("reserved cancellation authorization")),
                "must hit the reserved-kind guard, not a sequence/uniqueness error"
            );
        }
        let mut fixture = Fixture::new();
        fixture.request = authorization(&fixture);
        // An already committed checkpoint must not bypass the kind guard.
        let mut checkpoint = fixture.request.event.clone();
        checkpoint.kind = "r196.ordinary-checkpoint".into();
        checkpoint.event_id = "r196-ordinary-checkpoint".into();
        fixture
            .store
            .append_events_allocating_sequence_if_checkpoint_absent(
                &[checkpoint.to_session_event().unwrap()],
                "r196-checkpoint",
            )
            .unwrap()
            .unwrap();
        let mut ordinary = fixture.request.event.clone();
        ordinary.kind = "r196.ordinary-raw-control".into();
        ordinary.event_id = "r196-ordinary-raw-control".into();
        ordinary.sequence = fixture
            .store
            .next_event_sequence(&ordinary.session_id)
            .unwrap();
        fixture
            .store
            .append_event(&ordinary.to_session_event().unwrap())
            .unwrap();
        // Exercise actual ordinary backend methods both before insertion and
        // after a legitimate row exists: duplicate success must not launder it.
        for existing in [false, true] {
            if existing {
                fixture
                    .store
                    .append_session_domain_event_if_claim_current(&fixture.request)
                    .unwrap();
            }
            let mut domain = fixture.request.event.clone();
            domain.sequence = fixture
                .store
                .next_event_sequence(&domain.session_id)
                .unwrap();
            let event = domain.to_session_event().unwrap();
            reserved_refusal(fixture.store.append_event(&event));
            reserved_refusal(fixture.store.append_event_allocating_sequence(&event));
            reserved_refusal(
                fixture
                    .store
                    .append_events_allocating_sequence(std::slice::from_ref(&event)),
            );
            reserved_refusal(
                fixture
                    .store
                    .append_session_domain_event_if_absent_allocating_sequence(
                        &event,
                        &fixture.request.event.event_id,
                    ),
            );
            reserved_refusal(
                fixture
                    .store
                    .append_events_allocating_sequence_if_checkpoint_absent(
                        std::slice::from_ref(&event),
                        "r196-checkpoint",
                    ),
            );
            reserved_refusal(
                fixture
                    .store
                    .append_context_envelope_event_if_absent(&event),
            );
            reserved_refusal(
                fixture
                    .store
                    .append_context_envelope_event_if_absent_allocating_sequence(&event),
            );
            let mut disguised = event.clone();
            disguised.event_type = "ContextEnvelope".into();
            reserved_refusal(
                fixture
                    .store
                    .append_context_envelope_event_if_absent(&disguised),
            );
            let observation = SessionClaimEventAppend::observation(
                fixture.request.session_id.clone(),
                fixture.request.ingress_message_id.clone(),
                fixture.request.turn_id.clone(),
                fixture.request.fence.clone(),
                fixture.request.event.clone(),
            );
            reserved_refusal(
                fixture
                    .store
                    .append_session_domain_event_if_claim_current(&observation),
            );
            assert_eq!(fixture.count(), i64::from(existing));
        }
    }

    #[test]
    #[ignore = "requires parent-owned isolated COWD_TEST_POSTGRES_URL"]
    fn r196_authorization_revoke_before_auth_zero_rows() {
        let mut fixture = Fixture::new();
        fixture.request = authorization(&fixture);
        fixture.revoke();
        assert!(matches!(
            fixture
                .store
                .append_session_domain_event_if_claim_current(&fixture.request),
            Err(session::SessionError::StaleExecutionFence(_))
        ));
        assert_eq!(fixture.count(), 0);
        assert!(fixture
            .store
            .get_session_domain_event_by_id(
                &fixture.request.session_id,
                &fixture.request.event.event_id
            )
            .unwrap()
            .is_none());
    }

    #[test]
    #[ignore = "requires parent-owned isolated COWD_TEST_POSTGRES_URL"]
    fn r196_authorization_held_before_revoke_historical_exact_read() {
        let mut fixture = Fixture::new();
        fixture.request = authorization(&fixture);
        let revoker_store = PostgresSessionStore::new(fixture.scope.reconnect()).unwrap();
        let mut holder = fixture.store.executor.checkout_critical().unwrap();
        let holder_pid: i32 = holder
            .query_one("SELECT pg_backend_pid()", &[])
            .unwrap()
            .get(0);
        let mut tx = holder.transaction().unwrap();
        lock_claim_event_append_tx(&mut tx, &fixture.request).unwrap();
        let gate = Arc::new(Barrier::new(2));
        let worker_gate = gate.clone();
        let input = fixture.input.clone();
        let revoker = std::thread::spawn(move || {
            let now = owner_now(&revoker_store);
            worker_gate.wait();
            revoker_store
                .cancel_session_runtime_outbox(
                    &input.input_id,
                    1,
                    input.revision,
                    "r196-test",
                    "revoke",
                    now,
                )
                .unwrap();
        });
        gate.wait();
        wait_until_blocked(&fixture.scope.reconnect(), holder_pid);
        let committed = append_claim_event_tx(&mut tx, &fixture.request).unwrap();
        tx.commit().unwrap();
        revoker.join().unwrap();
        assert_eq!(fixture.count(), 1);
        assert!(matches!(
            fixture
                .store
                .append_session_domain_event_if_claim_current(&fixture.request),
            Err(session::SessionError::StaleExecutionFence(_))
        ));
        assert_eq!(
            fixture
                .store
                .get_session_domain_event_by_id(
                    &fixture.request.session_id,
                    &fixture.request.event.event_id
                )
                .unwrap(),
            Some(committed)
        );
        // Reading history does not confer a fresh physical-observation claim.
        let physical = SessionClaimEventAppend::observation(
            fixture.request.session_id.clone(),
            fixture.request.ingress_message_id.clone(),
            fixture.request.turn_id.clone(),
            fixture.request.fence.clone(),
            SessionDomainEvent::new(
                &fixture.request.session_id,
                0,
                SessionDomainScope::Tool,
                "r196.physical-observation",
                serde_json::json!({"physical": "original"}),
                fixture.request.event.created_at_ms,
            ),
        );
        assert!(fixture
            .store
            .append_session_domain_event_if_claim_current(&physical)
            .is_err());
        assert!(fixture
            .store
            .get_session_domain_event_by_id(&physical.session_id, &physical.event.event_id)
            .unwrap()
            .is_none());
    }

    #[test]
    #[ignore = "requires parent-owned isolated COWD_TEST_POSTGRES_URL"]
    fn r196_authorization_changed_payload_same_key_renew_and_replay() {
        let mut fixture = Fixture::new();
        fixture.request = authorization(&fixture);
        let renewed = fixture
            .store
            .renew_session_runtime_outbox_lease(
                &fixture.input.request_id,
                fixture.input.claim_owner.as_deref().unwrap(),
                1,
                fixture.input.claim_token.as_deref().unwrap(),
                fixture.input.revision,
                owner_now(&fixture.store),
                60_000,
            )
            .unwrap();
        assert!(renewed.revision > fixture.input.revision);
        assert_eq!(
            renewed.claim_fence_epoch,
            Some(fixture.request.fence.claim_fence_epoch)
        );
        let committed = fixture
            .store
            .append_session_domain_event_if_claim_current(&fixture.request)
            .unwrap();
        assert_eq!(
            fixture
                .store
                .append_session_domain_event_if_claim_current(&fixture.request)
                .unwrap(),
            committed
        );
        let payload: session::SessionCancellationAuthorizationPayload =
            serde_json::from_value(fixture.request.event.payload.clone()).unwrap();
        for field in 0..5 {
            let mut receipt = payload.receipt.clone();
            let mut reference = payload.requested_ref.clone();
            match field {
                0 => receipt.actor_id.push_str("-changed"),
                1 => receipt.reason = Some("changed-reason".into()),
                2 => receipt.execution_id.push_str("-changed"),
                3 => reference.commit_cursor += 1,
                _ => receipt.requested_at_ms += 1,
            }
            let changed = SessionClaimEventAppend::cancellation_authorization(
                fixture.request.session_id.clone(),
                fixture.request.ingress_message_id.clone(),
                fixture.request.turn_id.clone(),
                fixture.request.fence.clone(),
                receipt,
                reference,
            )
            .unwrap();
            assert_eq!(changed.event.event_id, fixture.request.event.event_id);
            assert!(matches!(
                fixture
                    .store
                    .append_session_domain_event_if_claim_current(&changed),
                Err(session::SessionError::IdempotencyConflict { .. })
            ));
        }
        let mut forged = fixture.request.clone();
        forged.event.payload["receipt"]["actor_id"] = serde_json::json!("forged");
        assert!(fixture
            .store
            .append_session_domain_event_if_claim_current(&forged)
            .is_err());
        let mut wrong_token = fixture.request.clone();
        wrong_token.fence.claim_token.push_str("-foreign");
        assert!(fixture
            .store
            .append_session_domain_event_if_claim_current(&wrong_token)
            .is_err());
        assert_eq!(fixture.count(), 1);
    }

    #[test]
    #[ignore = "requires parent-owned isolated COWD_TEST_POSTGRES_URL"]
    fn r196_authorization_commit_loss_and_lost_result_exact_read() {
        let mut fixture = Fixture::new();
        fixture.request = authorization(&fixture);
        let executor = fixture.scope.reconnect();
        let mut oracle = executor.checkout_critical().unwrap();
        oracle.batch_execute(
            "CREATE FUNCTION r196_lose_commit_connection() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.event_json::jsonb ->> 'kind' = 'session.claim.cancellation_authorized.v1' THEN
                     PERFORM pg_terminate_backend(pg_backend_pid());
                 END IF;
                 RETURN NEW;
             END $$;
             CREATE CONSTRAINT TRIGGER r196_lose_commit_connection
             AFTER INSERT ON session_events DEFERRABLE INITIALLY DEFERRED
             FOR EACH ROW EXECUTE FUNCTION r196_lose_commit_connection()"
        ).unwrap();
        assert!(fixture
            .store
            .append_session_domain_event_if_claim_current(&fixture.request)
            .is_err());
        assert_eq!(fixture.count(), 0);
        oracle
            .batch_execute(
                "DROP TRIGGER r196_lose_commit_connection ON session_events;
            DROP FUNCTION r196_lose_commit_connection()",
            )
            .unwrap();
        // Discard an actual committed result to model acknowledgment loss.
        // This is distinct from the real pre-commit connection loss above.
        drop(
            fixture
                .store
                .append_session_domain_event_if_claim_current(&fixture.request)
                .unwrap(),
        );
        let historical = fixture
            .store
            .get_session_domain_event_by_id(
                &fixture.request.session_id,
                &fixture.request.event.event_id,
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            fixture
                .store
                .append_session_domain_event_if_claim_current(&fixture.request)
                .unwrap(),
            historical
        );
        fixture.revoke();
        assert_eq!(
            fixture
                .store
                .get_session_domain_event_by_id(
                    &fixture.request.session_id,
                    &fixture.request.event.event_id
                )
                .unwrap(),
            Some(historical)
        );
        assert!(fixture
            .store
            .append_session_domain_event_if_claim_current(&fixture.request)
            .is_err());
        assert_eq!(fixture.count(), 1);
    }
}

fn receipt_ack_from_row(
    row: &Row,
) -> session::SessionResult<harness_contract::live::SessionMessageAck> {
    Ok(harness_contract::live::SessionMessageAck {
        session_id: row.try_get(0).map_err(postgres_error)?,
        message_id: row.try_get(1).map_err(postgres_error)?,
        client_id: row.try_get(2).map_err(postgres_error)?,
        runtime_commit_cursor: harness_contract::live::ReceiptCursor::parse(
            &row.try_get::<_, i64>(3)
                .map_err(postgres_error)?
                .to_string(),
        )
        .map_err(|_| lifecycle::receipt_conflict())?,
        content_digest: row.try_get(4).map_err(postgres_error)?,
        received_at_ms: i64_to_u64(row.try_get(5).map_err(postgres_error)?, "receipt time")?,
    })
}

fn receipt_context_tx(
    transaction: &mut PostgresTransaction<'_>,
    query: &session::SessionTerminalReceiptQuery,
    presence: &SessionPresenceProjection,
    now: u64,
    lock: bool,
) -> session::SessionResult<session::SessionTerminalReceiptContext> {
    use harness_contract::live::{
        SessionMessageReceiptContext, SessionMessageReceiptUnavailableReason as Unavailable,
    };
    let authority = &query.authority;
    lifecycle::receipt_attachment_current(presence, authority, now)?;
    let sql = if lock {
        "SELECT sequence,terminal_runtime_commit_cursor,terminal_content_digest,octet_length(content_json),
            CASE WHEN octet_length(content_json)<=8388608 THEN content_json END
         FROM session_messages WHERE session_id=$1 AND stable_message_id=$2 FOR UPDATE"
    } else {
        "SELECT sequence,terminal_runtime_commit_cursor,terminal_content_digest,octet_length(content_json),
            CASE WHEN octet_length(content_json)<=8388608 THEN content_json END
         FROM session_messages WHERE session_id=$1 AND stable_message_id=$2"
    };
    let row = transaction
        .query_opt(sql, &[&authority.session_id, &query.message_id])
        .map_err(postgres_error)?
        .ok_or_else(|| session::SessionError::NotFound("message".into()))?;
    let unavailable = |reason: Unavailable| session::SessionTerminalReceiptContext {
        context: SessionMessageReceiptContext::Unavailable { reason },
        input: None,
    };
    let cursor: Option<i64> = row.try_get(1).map_err(postgres_error)?;
    let digest: Option<String> = row.try_get(2).map_err(postgres_error)?;
    let (Some(cursor), Some(digest)) = (cursor, digest) else {
        return Ok(unavailable(Unavailable::UnboundOrInvalidated));
    };
    let raw: Option<String> = row.try_get(4).map_err(postgres_error)?;
    let Some(raw) = raw else {
        return Ok(unavailable(Unavailable::PayloadTooLarge));
    };
    let Ok(payload) = session::public_terminal_payload_v1(&raw) else {
        return Ok(unavailable(Unavailable::InvalidPublicPayload));
    };
    if digest != payload.content_digest {
        return Ok(unavailable(Unavailable::BindingMismatch));
    }
    let inputs = transaction.query(
        "SELECT request_id FROM session_runtime_outbox WHERE session_id=$1 AND runtime_commit_cursor=$2 AND status='completed' ORDER BY request_id LIMIT 2",
        &[&authority.session_id,&cursor],
    ).map_err(postgres_error)?;
    if inputs.len() != 1 {
        return Ok(unavailable(Unavailable::AmbiguousTerminalInput));
    }
    let request_id: String = inputs[0].try_get(0).map_err(postgres_error)?;
    let input =
        runtime_outbox_tx(transaction, &request_id)?.ok_or_else(lifecycle::receipt_conflict)?;
    if query.message_id != format!("assistant:{}", input.message_id)
        || input.turn_id.is_empty()
        || input.session_generation == 0
        || input.runtime_commit_cursor != Some(i64_to_u64(cursor, "receipt cursor")?)
    {
        return Ok(unavailable(Unavailable::TerminalInputMismatch));
    }
    if let Some(source) = &authority.receipt_source {
        if source.turn_id != input.turn_id {
            return Err(lifecycle::receipt_conflict());
        }
    }
    let receipt = transaction.query_opt(
        "SELECT session_id,message_id,client_id,runtime_commit_cursor,content_digest,received_at_ms
         FROM session_terminal_message_acks WHERE session_id=$1 AND message_id=$2
           AND runtime_commit_cursor=$3 AND content_digest=$4 AND client_id=$5 AND principal_id=$6",
        &[&authority.session_id,&query.message_id,&cursor,&digest,&authority.client_id,&authority.principal_id],
    ).map_err(postgres_error)?.as_ref().map(receipt_ack_from_row).transpose()?;
    let context = SessionMessageReceiptContext::Available {
        payload_kind: harness_contract::live::SessionPublicPayloadKind::SessionPublicV1,
        payload_json: payload.payload_json,
        session_id: authority.session_id.clone(),
        message_id: query.message_id.clone(),
        client_id: authority.client_id.clone(),
        message_sequence: row
            .try_get::<_, i64>(0)
            .map_err(postgres_error)?
            .to_string(),
        runtime_commit_cursor: harness_contract::live::ReceiptCursor::parse(&cursor.to_string())
            .map_err(|_| lifecycle::receipt_conflict())?,
        content_digest: digest,
        receipt,
        session_source: harness_contract::live::SessionReceiptSessionObservation {
            request_id: input.request_id.clone(),
            turn_id: input.turn_id.clone(),
            session_generation: input.session_generation.to_string(),
            input_sequence: input.sequence.to_string(),
            presence_revision: presence.revision.to_string(),
            observed_at_ms: now,
        },
        outbox_observation:
            harness_contract::live::SessionReceiptOutboxObservation::RuntimeOutboxUnknown,
    };
    Ok(session::SessionTerminalReceiptContext {
        context,
        input: Some(input),
    })
}

impl PostgresSessionStore {
    pub fn get_terminal_message_receipt_context(
        &self,
        query: &session::SessionTerminalReceiptQuery,
    ) -> session::SessionResult<session::SessionTerminalReceiptContext> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .map_err(postgres_error)?;
        let presence =
            lifecycle::receipt_presence_tx(&mut transaction, &query.authority.session_id, false)?
                .ok_or_else(lifecycle::receipt_conflict)?;
        let now = lifecycle::receipt_clock_tx(&mut transaction)?;
        let result = receipt_context_tx(&mut transaction, query, &presence, now, false)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(result)
    }

    pub fn record_terminal_message_ack(
        &self,
        command: &session::SessionTerminalAckCommand,
    ) -> session::SessionResult<harness_contract::live::SessionMessageAck> {
        let query = &command.query;
        let authority = &query.authority;
        let cursor = to_u64_i64(command.binding.runtime_commit_cursor, "receipt cursor")?;
        if cursor <= 0 {
            return Err(session::SessionError::InvalidArgument(
                "invalid receipt cursor".into(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        lifecycle::receipt_parent_tx(&mut transaction, &authority.session_id)?;
        let presence =
            lifecycle::receipt_presence_tx(&mut transaction, &authority.session_id, true)?
                .ok_or_else(lifecycle::receipt_conflict)?;
        transaction.query_opt("SELECT stable_message_id FROM session_messages WHERE session_id=$1 AND stable_message_id=$2 FOR UPDATE",
            &[&authority.session_id,&query.message_id]).map_err(postgres_error)?.ok_or_else(|| session::SessionError::NotFound("message".into()))?;
        let now = lifecycle::receipt_clock_tx(&mut transaction)?;
        let context = receipt_context_tx(&mut transaction, query, &presence, now, true)?;
        let harness_contract::live::SessionMessageReceiptContext::Available {
            runtime_commit_cursor,
            content_digest,
            ..
        } = context.context
        else {
            return Err(lifecycle::receipt_conflict());
        };
        if runtime_commit_cursor
            .value()
            .map_err(|_| lifecycle::receipt_conflict())?
            != command.binding.runtime_commit_cursor
            || content_digest != command.binding.content_digest
        {
            return Err(lifecycle::receipt_conflict());
        }
        transaction.execute(
            "INSERT INTO session_terminal_message_acks(session_id,message_id,runtime_commit_cursor,client_id,principal_id,content_digest,received_at_ms)
             VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING",
            &[&authority.session_id,&query.message_id,&cursor,&authority.client_id,&authority.principal_id,&content_digest,&to_u64_i64(now,"receipt time")?],
        ).map_err(postgres_error)?;
        let row = transaction.query_one(
            "SELECT session_id,message_id,client_id,runtime_commit_cursor,content_digest,received_at_ms,principal_id
             FROM session_terminal_message_acks WHERE session_id=$1 AND message_id=$2 AND runtime_commit_cursor=$3 AND client_id=$4",
            &[&authority.session_id,&query.message_id,&cursor,&authority.client_id],
        ).map_err(postgres_error)?;
        let receipt = receipt_ack_from_row(&row)?;
        if receipt.content_digest != content_digest
            || row.try_get::<_, String>(6).map_err(postgres_error)? != authority.principal_id
        {
            return Err(lifecycle::receipt_conflict());
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(receipt)
    }

    pub fn list_terminal_message_acks(
        &self,
        query: &session::SessionTerminalAckListQuery,
    ) -> session::SessionResult<Vec<harness_contract::live::SessionMessageAckHistory>> {
        if !(1..=100).contains(&query.limit) {
            return Err(session::SessionError::InvalidArgument(
                "receipt history limit must be 1..100".into(),
            ));
        }
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .map_err(postgres_error)?;
        let presence =
            lifecycle::receipt_presence_tx(&mut transaction, &query.authority.session_id, false)?
                .ok_or_else(lifecycle::receipt_conflict)?;
        let now = lifecycle::receipt_clock_tx(&mut transaction)?;
        lifecycle::receipt_attachment_current(&presence, &query.authority, now)?;
        let rows = transaction.query(
            "SELECT session_id,message_id,client_id,runtime_commit_cursor,content_digest,received_at_ms
             FROM session_terminal_message_acks WHERE session_id=$1 AND client_id=$2 AND principal_id=$3
               AND runtime_commit_cursor>$4 ORDER BY runtime_commit_cursor,message_id LIMIT $5",
            &[&query.authority.session_id,&query.authority.client_id,&query.authority.principal_id,
              &to_u64_i64(query.after_cursor,"history cursor")?,&to_i64(query.limit,"history limit")?],
        ).map_err(postgres_error)?;
        let result = rows
            .iter()
            .map(|row| {
                receipt_ack_from_row(row).map(|receipt| {
                    harness_contract::live::SessionMessageAckHistory::Historical { receipt }
                })
            })
            .collect::<session::SessionResult<Vec<_>>>()?;
        transaction.commit().map_err(postgres_error)?;
        Ok(result)
    }
}

impl PostgresSessionStore {
    pub fn insert_message(&self, message: &SessionMessage) -> session::SessionResult<()> {
        let sequence = to_i64(message.sequence, "message sequence")?;
        let blocks_count = to_i64(message.blocks_count, "message blocks")?;
        let created_at_ms = i64::try_from(message.created_at_ms)
            .map_err(|_| session::SessionError::Store("message time overflow".to_string()))?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "INSERT INTO session_messages(
                    stable_message_id, session_id, sequence, role, content_json, blocks_count,
                    tool_use_id, tool_name, token_usage_json, created_at_ms
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                 ON CONFLICT(session_id, sequence) DO UPDATE SET
                    role=EXCLUDED.role, content_json=EXCLUDED.content_json,
                    blocks_count=EXCLUDED.blocks_count, tool_use_id=EXCLUDED.tool_use_id,
                    tool_name=EXCLUDED.tool_name, token_usage_json=EXCLUDED.token_usage_json,
                    created_at_ms=EXCLUDED.created_at_ms",
                &[
                    &message.stable_message_id,
                    &message.session_id,
                    &sequence,
                    &message.role,
                    &message.content_json,
                    &blocks_count,
                    &message.tool_use_id,
                    &message.tool_name,
                    &message.token_usage_json,
                    &created_at_ms,
                ],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn get_messages(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        let limit = to_i64(bounded_limit(limit, 1, 500), "message limit")?;
        let offset = to_i64(offset, "message offset")?;
        self.query_messages(
            "WITH page_start AS (
                 SELECT sequence
                   FROM session_messages
                  WHERE session_id=$1
                  ORDER BY sequence ASC
                  LIMIT 1 OFFSET $3
             )
             SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                    tool_use_id, tool_name, token_usage_json, created_at_ms
               FROM session_messages
              WHERE session_id=$1
                AND sequence >= (SELECT sequence FROM page_start)
              ORDER BY sequence ASC LIMIT $2",
            &[&session_id, &limit, &offset],
        )
    }

    pub fn get_messages_from_sequence(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        let from_sequence = to_i64(from_sequence, "message sequence")?;
        let limit = to_i64(bounded_limit(limit, 1, 500), "message limit")?;
        self.query_messages(
            "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                    tool_use_id, tool_name, token_usage_json, created_at_ms
               FROM session_messages WHERE session_id=$1 AND sequence >= $2
              ORDER BY sequence ASC LIMIT $3",
            &[&session_id, &from_sequence, &limit],
        )
    }

    pub fn get_messages_in_ranges(
        &self,
        session_id: &str,
        ranges: &[(usize, usize)],
        limit: usize,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        let limit = to_i64(bounded_limit(limit, 1, 2_048), "message range limit")?;
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        for &(start, end) in ranges.iter().take(128) {
            if start >= end {
                continue;
            }
            starts.push(to_i64(start, "message range start")?);
            ends.push(to_i64(end, "message range end")?);
        }
        if starts.is_empty() {
            return Ok(Vec::new());
        }
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT stable_message_id, session_id, sequence, role, content_json,
                        blocks_count, tool_use_id, tool_name, token_usage_json, created_at_ms
                   FROM session_messages AS message
                  WHERE session_id=$1
                    AND EXISTS (
                        SELECT 1
                          FROM unnest($2::BIGINT[], $3::BIGINT[])
                               AS selected(start_sequence, end_sequence)
                         WHERE message.sequence >= selected.start_sequence
                           AND message.sequence < selected.end_sequence
                    )
                  ORDER BY sequence ASC
                  LIMIT $4",
                &[&session_id, &starts, &ends, &limit],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_message)
            .collect()
    }

    pub fn get_message_by_stable_id(
        &self,
        session_id: &str,
        stable_message_id: &str,
    ) -> session::SessionResult<Option<SessionMessage>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT stable_message_id, session_id, sequence, role, content_json,
                        blocks_count, tool_use_id, tool_name, token_usage_json,
                        created_at_ms
                   FROM session_messages
                  WHERE session_id=$1 AND stable_message_id=$2",
                &[&session_id, &stable_message_id],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_message(&row))
            .transpose()
    }

    pub fn get_message_by_sequence(
        &self,
        session_id: &str,
        sequence: usize,
    ) -> session::SessionResult<Option<SessionMessage>> {
        let sequence = to_i64(sequence, "message sequence")?;
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT stable_message_id, session_id, sequence, role, content_json,
                        blocks_count, tool_use_id, tool_name, token_usage_json,
                        created_at_ms
                   FROM session_messages
                  WHERE session_id=$1 AND sequence=$2",
                &[&session_id, &sequence],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_message(&row))
            .transpose()
    }

    pub fn get_message_metadata_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionMessageMetadata>> {
        let from_sequence = to_i64(from_sequence, "message sequence")?;
        let limit = to_i64(bounded_limit(limit, 1, 2_048), "message metadata limit")?;
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT stable_message_id, session_id, sequence, role,
                        blocks_count, tool_use_id, tool_name, created_at_ms,
                        octet_length(content_json)::BIGINT
                   FROM session_messages
                  WHERE session_id=$1 AND sequence >= $2
                  ORDER BY sequence ASC
                  LIMIT $3",
                &[&session_id, &from_sequence, &limit],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_message_metadata)
            .collect()
    }

    pub fn get_context_index_cards(
        &self,
        session_id: &str,
        limit: usize,
    ) -> session::SessionResult<Vec<ContextIndexCard>> {
        let limit = to_i64(bounded_limit(limit, 1, 2_048), "context index card limit")?;
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT card_id, parent_card_id, session_id,
                        source_start_sequence, source_end_sequence,
                        source_message_count, source_digest, summary, scope,
                        authority, generation, created_at_ms, updated_at_ms
                   FROM session_context_index_cards
                  WHERE session_id=$1
                  ORDER BY
                      CASE WHEN parent_card_id IS NULL THEN 0 ELSE 1 END,
                      source_start_sequence DESC
                  LIMIT $2",
                &[&session_id, &limit],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_context_index_card)
            .collect()
    }

    pub fn reconcile_session_context_index(
        &self,
        session_id: &str,
        card_span: usize,
        parent_span: usize,
        now_ms: u64,
    ) -> session::SessionResult<ContextIndexCoverage> {
        let mut connection = self.executor.checkout_background().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        let messages = transaction
            .query(
                "SELECT stable_message_id, session_id, sequence, role, content_json,
                        blocks_count, tool_use_id, tool_name, token_usage_json,
                        created_at_ms
                   FROM session_messages
                  WHERE session_id=$1
                  ORDER BY sequence ASC",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_message)
            .collect::<session::SessionResult<Vec<_>>>()?;
        let current_generation: i64 = transaction
            .query_one(
                "SELECT index_generation FROM session_recovery_manifest
                  WHERE session_id=$1 FOR UPDATE",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let generation =
            i64_to_u64(current_generation, "context index generation")?.saturating_add(1);
        let cards = build_context_index_cards(
            session_id,
            &messages,
            card_span,
            parent_span,
            generation,
            now_ms,
        );
        transaction
            .execute(
                "DELETE FROM session_context_index_cards WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        for card in &cards {
            transaction
                .execute(
                    "INSERT INTO session_context_index_cards(
                         card_id, parent_card_id, session_id,
                         source_start_sequence, source_end_sequence,
                         source_message_count, source_digest, summary, scope,
                         authority, generation, created_at_ms, updated_at_ms
                     ) VALUES (
                         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
                     )",
                    &[
                        &card.card_id,
                        &card.parent_card_id,
                        &card.session_id,
                        &to_i64(card.source_start_sequence, "card source start")?,
                        &to_i64(card.source_end_sequence, "card source end")?,
                        &to_i64(card.source_message_count, "card source count")?,
                        &card.source_digest,
                        &card.summary,
                        &card.scope,
                        &card.authority,
                        &to_u64_i64(card.generation, "card generation")?,
                        &to_u64_i64(card.created_at_ms, "card created time")?,
                        &to_u64_i64(card.updated_at_ms, "card updated time")?,
                    ],
                )
                .map_err(postgres_error)?;
        }
        let indexed_through_sequence = messages.last().map(|message| message.sequence);
        transaction
            .execute(
                "UPDATE session_recovery_manifest
                    SET index_generation=$2,
                        indexed_through_sequence=$3,
                        index_card_count=$4,
                        index_pending=FALSE,
                        manifest_revision=manifest_revision + 1
                  WHERE session_id=$1",
                &[
                    &session_id,
                    &to_u64_i64(generation, "context index generation")?,
                    &indexed_through_sequence
                        .map(|value| to_i64(value, "indexed through sequence"))
                        .transpose()?,
                    &to_i64(cards.len(), "context card count")?,
                ],
            )
            .map_err(postgres_error)?;
        transaction
            .execute(
                "UPDATE session_context_index_outbox
                    SET status='completed', attempts=attempts + 1,
                        updated_at_ms=$2
                  WHERE session_id=$1 AND status!='completed'",
                &[
                    &session_id,
                    &to_u64_i64(now_ms, "context index update time")?,
                ],
            )
            .map_err(postgres_error)?;
        transaction.commit().map_err(postgres_error)?;
        let leaf_cards = cards
            .iter()
            .filter(|card| card.parent_card_id.is_some() || cards.len() == 1)
            .collect::<Vec<_>>();
        let covered_messages = leaf_cards
            .iter()
            .map(|card| card.source_message_count)
            .sum();
        Ok(ContextIndexCoverage {
            session_id: session_id.to_string(),
            source_messages: messages.len(),
            covered_messages,
            card_count: cards.len(),
            indexed_through_sequence,
            generation,
            complete: covered_messages == messages.len(),
            source_digest: context_index_source_digest(&messages),
            card_digest: context_index_card_digest(&cards),
        })
    }

    pub fn get_message_count(&self, session_id: &str) -> session::SessionResult<usize> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let count: i64 = connection
            .query_one(
                "SELECT COUNT(*) FROM session_messages WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        usize::try_from(count)
            .map_err(|_| session::SessionError::Store("message count overflow".to_string()))
    }

    pub fn delete_messages_from(
        &self,
        session_id: &str,
        from_sequence: usize,
    ) -> session::SessionResult<usize> {
        let from_sequence = to_i64(from_sequence, "message sequence")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let deleted = connection
            .execute(
                "DELETE FROM session_messages WHERE session_id=$1 AND sequence >= $2",
                &[&session_id, &from_sequence],
            )
            .map_err(postgres_error)?;
        Ok(deleted as usize)
    }

    pub fn get_all_messages(
        &self,
        session_id: &str,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        self.query_messages(
            "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                    tool_use_id, tool_name, token_usage_json, created_at_ms
               FROM session_messages WHERE session_id=$1 ORDER BY sequence ASC",
            &[&session_id],
        )
    }

    pub fn insert_messages_batch(&self, messages: &[SessionMessage]) -> session::SessionResult<()> {
        if messages.is_empty() {
            return Ok(());
        }
        let records = messages
            .iter()
            .map(|message| {
                Ok(serde_json::json!({
                    "stable_message_id": if message.stable_message_id.trim().is_empty() {
                        format!("legacy:{}:{}", message.session_id, message.sequence)
                    } else {
                        message.stable_message_id.clone()
                    },
                    "session_id": message.session_id,
                    "sequence": to_i64(message.sequence, "message sequence")?,
                    "role": message.role,
                    "content_json": message.content_json,
                    "blocks_count": to_i64(message.blocks_count, "message blocks")?,
                    "tool_use_id": message.tool_use_id,
                    "tool_name": message.tool_name,
                    "token_usage_json": message.token_usage_json,
                    "created_at_ms": to_u64_i64(message.created_at_ms, "message time")?,
                }))
            })
            .collect::<session::SessionResult<Vec<_>>>()?;
        let records = serde_json::Value::Array(records);
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .execute(
                "INSERT INTO session_messages(
                     stable_message_id,session_id,sequence,role,content_json,blocks_count,
                     tool_use_id,tool_name,token_usage_json,created_at_ms
                 )
                 SELECT stable_message_id,session_id,sequence,role,content_json,blocks_count,
                        tool_use_id,tool_name,token_usage_json,created_at_ms
                   FROM jsonb_to_recordset($1::JSONB) AS input(
                        stable_message_id TEXT,session_id TEXT,sequence BIGINT,role TEXT,
                        content_json TEXT,blocks_count BIGINT,tool_use_id TEXT,tool_name TEXT,
                        token_usage_json TEXT,created_at_ms BIGINT
                   )
                 ON CONFLICT(session_id,sequence) DO UPDATE SET
                    role=EXCLUDED.role,content_json=EXCLUDED.content_json,
                    blocks_count=EXCLUDED.blocks_count,tool_use_id=EXCLUDED.tool_use_id,
                    tool_name=EXCLUDED.tool_name,token_usage_json=EXCLUDED.token_usage_json,
                    created_at_ms=EXCLUDED.created_at_ms",
                &[&records],
            )
            .map_err(postgres_error)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(())
    }

    pub fn copy_session_messages_at_cutoff(
        &self,
        source_session_id: &str,
        target_session_id: &str,
        source_message_count: usize,
    ) -> session::SessionResult<usize> {
        if source_session_id.trim().is_empty()
            || target_session_id.trim().is_empty()
            || source_session_id == target_session_id
        {
            return Err(session::SessionError::Store(
                "branch copy requires distinct non-empty source and target sessions".to_string(),
            ));
        }
        let cutoff = to_i64(source_message_count, "branch cutoff")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        // Lock in stable lexical order so concurrent reciprocal branch requests
        // cannot deadlock.
        let (first, second) = if source_session_id < target_session_id {
            (source_session_id, target_session_id)
        } else {
            (target_session_id, source_session_id)
        };
        let rows = transaction
            .query(
                "SELECT session_id FROM session_records
                  WHERE session_id IN ($1,$2)
                  ORDER BY session_id FOR UPDATE",
                &[&first, &second],
            )
            .map_err(postgres_error)?;
        if rows.len() != 2 {
            return Err(session::SessionError::Store(
                "branch source and target sessions must both exist".to_string(),
            ));
        }
        let target_count: i64 = transaction
            .query_one(
                "SELECT COUNT(*) FROM session_messages WHERE session_id=$1",
                &[&target_session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        if target_count != 0 {
            return Err(session::SessionError::Store(format!(
                "branch target `{target_session_id}` already contains messages"
            )));
        }
        let copied = transaction
            .execute(
                "INSERT INTO session_messages(
                     stable_message_id,session_id,sequence,role,content_json,blocks_count,
                     tool_use_id,tool_name,token_usage_json,created_at_ms
                 )
                 SELECT 'branch:' || $2 || ':' || stable_message_id,
                        $2,sequence,role,content_json,blocks_count,
                        tool_use_id,tool_name,token_usage_json,created_at_ms
                   FROM session_messages
                  WHERE session_id=$1 AND sequence < $3
                  ORDER BY sequence",
                &[&source_session_id, &target_session_id, &cutoff],
            )
            .map_err(postgres_error)?;
        let last_created_at: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(created_at_ms),0)
                   FROM session_messages WHERE session_id=$1",
                &[&target_session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        refresh_session_message_summary_tx(
            &mut transaction,
            target_session_id,
            i64_to_u64(last_created_at.max(0), "branch message time")?,
        )?;
        refresh_session_usage_summary_tx(&mut transaction, target_session_id)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(copied as usize)
    }

    pub fn branch_session_at_cutoff(
        &self,
        request: &SessionBranchRequest,
    ) -> session::SessionResult<SessionBranchResult> {
        for event_json in [&request.source_event_json, &request.target_event_json] {
            SessionEvent {
                session_id: request.source_session_id.clone(),
                event_type: "SessionBranched".into(),
                event_json: event_json.clone(),
                sequence: 0,
                created_at_ms: request.created_at_ms,
            }
            .reject_reserved_cancellation_authorization()?;
        }
        if request.operation_id.trim().is_empty()
            || request.source_session_id.trim().is_empty()
            || request.target.session_id.trim().is_empty()
            || request.source_session_id == request.target.session_id
        {
            return Err(session::SessionError::Store(
                "branch requires distinct source and target identities".to_string(),
            ));
        }

        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let source = transaction
            .query_opt(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&request.source_session_id],
            )
            .map_err(postgres_error)?;
        if source.is_none() {
            return Err(session::SessionError::Store(format!(
                "branch source `{}` does not exist",
                request.source_session_id
            )));
        }
        if let Some(existing) =
            query_branch_activation_tx(&mut transaction, &request.operation_id, true)?
        {
            if existing.source_session_id != request.source_session_id
                || existing.target_session_id != request.target.session_id
                || existing.source_message_count != request.source_message_count
            {
                return Err(session::SessionError::Store(format!(
                    "branch operation `{}` is bound to another source/cutoff/target",
                    request.operation_id
                )));
            }
            let target = transaction
                .query_opt(
                    "SELECT session_id,platform,chat_id,user_id,model,created_at,last_activity,
                            message_count,reset_policy,metadata_json,input_tokens,output_tokens,
                            status
                       FROM session_records WHERE session_id=$1",
                    &[&existing.target_session_id],
                )
                .map_err(postgres_error)?
                .map(|row| row_to_session(&row))
                .transpose()?
                .ok_or_else(|| {
                    session::SessionError::Store(format!(
                        "branch operation `{}` lost target `{}`",
                        existing.operation_id, existing.target_session_id
                    ))
                })?;
            let copied_message_count =
                usize::try_from(target.message_count.max(0)).map_err(|_| {
                    session::SessionError::Store(
                        "branch target message count exceeds usize".to_string(),
                    )
                })?;
            transaction.commit().map_err(postgres_error)?;
            return Ok(SessionBranchResult {
                target,
                copied_message_count,
                source_message_count: existing.source_message_count,
                activation: existing,
            });
        }
        let target_exists: bool = transaction
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM session_records WHERE session_id=$1)",
                &[&request.target.session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        if target_exists {
            return Err(session::SessionError::Store(format!(
                "branch target `{}` already exists",
                request.target.session_id
            )));
        }

        let source_count: i64 = transaction
            .query_one(
                "SELECT COUNT(*) FROM session_messages WHERE session_id=$1",
                &[&request.source_session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let source_count = from_i64(source_count, "branch source message count")?;
        let cutoff = request.source_message_count;
        if cutoff > source_count {
            return Err(session::SessionError::Store(format!(
                "branch cutoff {cutoff} exceeds source message count {source_count}"
            )));
        }
        let cutoff_i64 = to_i64(cutoff, "branch cutoff")?;

        transaction
            .execute(
                "INSERT INTO session_records(
                     session_id,platform,chat_id,user_id,model,created_at,last_activity,
                     message_count,reset_policy,metadata_json,input_tokens,output_tokens,
                     status,created_at_ms,updated_at_ms
                 ) VALUES($1,$2,$3,$4,$5,$6,$7,0,$8,$9,0,0,$10,
                     cowd_safe_session_epoch_ms($6),cowd_safe_session_epoch_ms($7))",
                &[
                    &request.target.session_id,
                    &request.target.platform,
                    &request.target.chat_id,
                    &request.target.user_id,
                    &request.target.model,
                    &request.target.created_at,
                    &request.target.last_activity,
                    &request.target.reset_policy,
                    &request.target.metadata_json,
                    &request.target.status,
                ],
            )
            .map_err(postgres_error)?;
        let copied = transaction
            .execute(
                "INSERT INTO session_messages(
                     stable_message_id,session_id,sequence,role,content_json,blocks_count,
                     tool_use_id,tool_name,token_usage_json,created_at_ms
                 )
                 SELECT 'branch:' || $2 || ':' || stable_message_id,
                        $2,sequence,role,content_json,blocks_count,
                        tool_use_id,tool_name,token_usage_json,created_at_ms
                   FROM session_messages
                  WHERE session_id=$1 AND sequence < $3
                  ORDER BY sequence",
                &[
                    &request.source_session_id,
                    &request.target.session_id,
                    &cutoff_i64,
                ],
            )
            .map_err(postgres_error)?;
        let copied = usize::try_from(copied).map_err(|_| {
            session::SessionError::Store("branch copied message count exceeds usize".to_string())
        })?;
        let last_created_at: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(created_at_ms),0)
                   FROM session_messages WHERE session_id=$1",
                &[&request.target.session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        refresh_session_message_summary_tx(
            &mut transaction,
            &request.target.session_id,
            i64_to_u64(last_created_at.max(0), "branch message time")?,
        )?;
        refresh_session_usage_summary_tx(&mut transaction, &request.target.session_id)?;

        for (session_id, event_type, event_json) in [
            (
                request.source_session_id.as_str(),
                "SessionBranched",
                request.source_event_json.as_str(),
            ),
            (
                request.target.session_id.as_str(),
                "BranchCreated",
                request.target_event_json.as_str(),
            ),
        ] {
            let sequence: i64 = transaction
                .query_one(
                    "SELECT COALESCE(MAX(sequence) + 1, 0)
                       FROM session_events WHERE session_id=$1",
                    &[&session_id],
                )
                .map_err(postgres_error)?
                .try_get(0)
                .map_err(postgres_error)?;
            let stored_sequence = from_i64(sequence, "branch event sequence")?;
            let event = SessionEvent {
                session_id: session_id.to_string(),
                event_type: event_type.to_string(),
                event_json: event_json.to_string(),
                sequence: stored_sequence,
                created_at_ms: request.created_at_ms,
            };
            event.reject_reserved_cancellation_authorization()?;
            let allocated_json = event_json_with_allocated_sequence(&event, stored_sequence)?;
            transaction
                .execute(
                    "INSERT INTO session_events(
                         session_id,sequence,event_type,event_json,created_at_ms
                     ) VALUES($1,$2,$3,$4,$5)",
                    &[
                        &session_id,
                        &sequence,
                        &event_type,
                        &allocated_json,
                        &to_u64_i64(request.created_at_ms, "branch event time")?,
                    ],
                )
                .map_err(postgres_error)?;
        }
        transaction
            .execute(
                "INSERT INTO session_branch_activations(
                     operation_id,source_session_id,target_session_id,source_message_count,
                     phase,created_at_ms,updated_at_ms,last_error,revision
                 ) VALUES($1,$2,$3,$4,'branch_committed',$5,$5,NULL,0)",
                &[
                    &request.operation_id,
                    &request.source_session_id,
                    &request.target.session_id,
                    &cutoff_i64,
                    &to_u64_i64(request.created_at_ms, "branch activation time")?,
                ],
            )
            .map_err(postgres_error)?;
        let activation =
            query_branch_activation_tx(&mut transaction, &request.operation_id, false)?
                .ok_or_else(|| {
                    session::SessionError::Store(
                        "branch transaction produced no activation receipt".to_string(),
                    )
                })?;
        transaction.commit().map_err(postgres_error)?;

        let mut target = request.target.clone();
        target.message_count = i64::try_from(copied).map_err(|_| {
            session::SessionError::Store("branch message count exceeds i64".to_string())
        })?;
        Ok(SessionBranchResult {
            target,
            copied_message_count: copied,
            source_message_count: cutoff,
            activation,
        })
    }

    pub fn get_session_branch_activation(
        &self,
        operation_id: &str,
    ) -> session::SessionResult<Option<SessionBranchActivation>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT operation_id,source_session_id,target_session_id,
                        source_message_count,phase,created_at_ms,updated_at_ms,
                        last_error,revision
                   FROM session_branch_activations WHERE operation_id=$1",
                &[&operation_id],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_branch_activation(&row))
            .transpose()
    }

    pub fn list_recoverable_session_branch_activations(
        &self,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionBranchActivation>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT operation_id,source_session_id,target_session_id,
                        source_message_count,phase,created_at_ms,updated_at_ms,
                        last_error,revision
                   FROM session_branch_activations
                  WHERE phase != 'activated'
                  ORDER BY updated_at_ms ASC,operation_id ASC LIMIT $1",
                &[&to_i64(limit.max(1), "branch activation recovery limit")?],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_branch_activation)
            .collect()
    }

    pub fn transition_session_branch_activation(
        &self,
        transition: &SessionBranchActivationTransition,
    ) -> session::SessionResult<SessionBranchActivation> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current = query_branch_activation_tx(&mut transaction, &transition.operation_id, true)?
            .ok_or_else(|| {
                session::SessionError::Store(format!(
                    "Session branch activation `{}` does not exist",
                    transition.operation_id
                ))
            })?;
        transition.validate(&current)?;
        let changed = transaction
            .execute(
                "UPDATE session_branch_activations
                    SET phase=$1,updated_at_ms=$2,last_error=$3,revision=revision+1
                  WHERE operation_id=$4 AND phase=$5 AND revision=$6",
                &[
                    &transition.next_phase.as_str(),
                    &to_u64_i64(
                        transition.updated_at_ms,
                        "branch activation transition time",
                    )?,
                    &transition.error,
                    &transition.operation_id,
                    &transition.expected_phase.as_str(),
                    &to_u64_i64(transition.expected_revision, "branch activation revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "Session branch activation `{}` changed during transition",
                transition.operation_id
            )));
        }
        let activation =
            query_branch_activation_tx(&mut transaction, &transition.operation_id, false)?
                .ok_or_else(|| {
                    session::SessionError::Store(format!(
                        "Session branch activation `{}` disappeared after transition",
                        transition.operation_id
                    ))
                })?;
        transaction.commit().map_err(postgres_error)?;
        Ok(activation)
    }

    pub fn commit_terminal_transcript_if_fenced(
        &self,
        request: &SessionTerminalTranscriptCommit,
    ) -> session::SessionResult<SessionTerminalTranscriptReceipt> {
        validate_terminal_transcript(
            &request.terminal_message_id,
            &request.ingress_message_id,
            &request.session_id,
            &request.messages,
        )?;
        validate_terminal_commit(request)?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let admission = query_input_admission_tx(&mut transaction, &request.session_id, true)?
            .ok_or_else(|| {
                session::SessionError::StaleExecutionFence(format!(
                    "session `{}` no longer exists",
                    request.session_id
                ))
            })?;
        let current = runtime_outbox_for_update(&mut transaction, &request.fence.request_id)?;
        if current.status == SessionRuntimeInputStatus::Completed
            && current.runtime_commit_cursor == Some(request.runtime_commit_cursor)
        {
            if current.session_id != request.session_id
                || current.message_id != request.ingress_message_id
                || current.turn_id != request.turn_id
                || current.sequence != request.fence.input_sequence
                || current.session_generation != request.fence.session_generation
                || current.claim_owner.as_deref() != Some(request.fence.claim_owner.as_str())
                || current.claim_token.as_deref() != Some(request.fence.claim_token.as_str())
                || current.claim_fence_epoch != Some(request.fence.claim_fence_epoch)
            {
                return Err(session::SessionError::StaleExecutionFence(format!(
                    "completed input `{}` identity does not match terminal replay",
                    request.fence.request_id
                )));
            }
            let messages = load_committed_terminal_transcript_tx(
                &mut transaction,
                &request.terminal_message_id,
                &request.messages,
            )?;
            verify_terminal_message_binding_tx(
                &mut transaction,
                &request.session_id,
                &request.terminal_message_id,
                request.runtime_commit_cursor,
            )?;
            transaction.commit().map_err(postgres_error)?;
            return Ok(SessionTerminalTranscriptReceipt {
                messages,
                inserted: false,
                input: current,
            });
        }
        let fence_valid = current.session_id == request.session_id
            && current.message_id == request.ingress_message_id
            && current.turn_id == request.turn_id
            && current.sequence == request.fence.input_sequence
            && current.status == SessionRuntimeInputStatus::Running
            && current.session_generation == request.fence.session_generation
            && admission.generation == request.fence.session_generation
            && admission.open
            && current.claim_owner.as_deref() == Some(request.fence.claim_owner.as_str())
            && current.claim_token.as_deref() == Some(request.fence.claim_token.as_str())
            && current.claim_fence_epoch == Some(request.fence.claim_fence_epoch)
            && current
                .claim_expires_at_ms
                .is_some_and(|expires| expires > request.created_at_ms);
        if !fence_valid {
            return Err(session::SessionError::StaleExecutionFence(format!(
                "request={} generation={} claim_fence_epoch={} current_status={:?} current_revision={}",
                request.fence.request_id,
                request.fence.session_generation,
                request.fence.claim_fence_epoch,
                current.status,
                current.revision
            )));
        }
        let newest_pending_sequence = transaction
            .query_one(
                "SELECT MAX(sequence)
                   FROM session_runtime_outbox
                  WHERE session_id=$1 AND session_generation=$2
                    AND sequence>$3
                    AND status NOT IN (
                      'rejected_duplicate','rejected_policy','completed',
                      'supplemented','failed','cancelled','expired'
                    )
                    AND decision IN (
                      'supplement_current_turn',
                      'interrupt_and_replan',
                      'control_or_approval'
                    )",
                &[
                    &request.session_id,
                    &to_u64_i64(request.fence.session_generation, "session generation")?,
                    &to_i64(request.fence.input_sequence, "input sequence")?,
                ],
            )
            .map_err(postgres_error)?
            .try_get::<_, Option<i64>>(0)
            .map_err(postgres_error)?
            .map(|value| value.max(0) as usize);
        if newest_pending_sequence
            .is_some_and(|sequence| sequence > request.consumed_input_sequence)
        {
            return Err(session::SessionError::StaleExecutionFence(format!(
                "terminal input cursor {} is behind pending Session input {}",
                request.consumed_input_sequence,
                newest_pending_sequence.unwrap_or_default()
            )));
        }
        let consumed_rows = transaction
            .query(
                "SELECT request_id
                   FROM session_runtime_outbox
                  WHERE session_id=$1 AND session_generation=$2
                    AND sequence>$3 AND sequence<=$4
                    AND status IN (
                      'accepted','classified','queued','claimed','running',
                      'reclassified','attached'
                    )
                    AND decision IN (
                      'supplement_current_turn',
                      'interrupt_and_replan',
                      'control_or_approval'
                    )
                  ORDER BY sequence ASC
                  FOR UPDATE",
                &[
                    &request.session_id,
                    &to_u64_i64(request.fence.session_generation, "session generation")?,
                    &to_i64(request.fence.input_sequence, "input sequence")?,
                    &to_i64(request.consumed_input_sequence, "consumed input sequence")?,
                ],
            )
            .map_err(postgres_error)?;
        for row in consumed_rows {
            let request_id = row.try_get::<_, String>(0).map_err(postgres_error)?;
            let before = runtime_outbox_tx(&mut transaction, &request_id)?.ok_or_else(|| {
                session::SessionError::Store(format!(
                    "consumed Session input `{request_id}` disappeared during terminal commit"
                ))
            })?;
            let changed = transaction
                .execute(
                    "UPDATE session_runtime_outbox
                        SET status='supplemented',terminal_at_ms=$1,
                            runtime_commit_cursor=$2,
                            claim_owner=NULL,claim_token=NULL,
                            claim_fence_epoch=NULL,claim_expires_at_ms=NULL,
                            failure_class=NULL,last_error=NULL,
                            updated_at_ms=$1,revision=revision+1
                      WHERE request_id=$3 AND revision=$4
                        AND status IN (
                          'accepted','classified','queued','claimed','running',
                          'reclassified','attached'
                        )",
                    &[
                        &to_u64_i64(request.created_at_ms, "terminal commit time")?,
                        &to_u64_i64(request.runtime_commit_cursor, "runtime cursor")?,
                        &request_id,
                        &to_u64_i64(before.revision, "input revision")?,
                    ],
                )
                .map_err(postgres_error)?;
            if changed != 1 {
                return Err(session::SessionError::StaleExecutionFence(format!(
                    "consumed Session input `{request_id}` changed during terminal commit"
                )));
            }
            let supplemented =
                runtime_outbox_tx(&mut transaction, &request_id)?.ok_or_else(|| {
                    session::SessionError::Store(format!(
                        "supplemented Session input `{request_id}` disappeared"
                    ))
                })?;
            append_runtime_history_tx(
                &mut transaction,
                &supplemented,
                "terminal_input_cursor_commit",
                Some(&request.fence.claim_owner),
                Some(before.revision),
                before.status,
                SessionRuntimeInputStatus::Supplemented,
                None,
                request.created_at_ms,
            )?;
            append_input_timeline_event_tx(
                &mut transaction,
                &request_from_outbox(&supplemented),
                &supplemented.session_id,
                supplemented.sequence,
                SessionRuntimeInputStatus::Supplemented.timeline_event_kind(),
                SessionRuntimeInputStatus::Supplemented,
                Some(&request.fence.claim_owner),
                None,
                request.created_at_ms,
            )?;
        }
        let (messages, inserted) = append_terminal_transcript_tx(
            &mut transaction,
            &request.terminal_message_id,
            &request.ingress_message_id,
            &request.session_id,
            &request.messages,
            request.created_at_ms,
            request.runtime_commit_cursor,
        )?;
        let terminal_status = SessionRuntimeInputStatus::Completed.as_str();
        let changed = transaction
            .execute(
                "UPDATE session_runtime_outbox
                    SET status=$1,runtime_commit_cursor=$2,
                        claim_expires_at_ms=NULL,terminal_at_ms=$3,
                        failure_class=NULL,last_error=NULL,updated_at_ms=$3,revision=revision+1
                  WHERE request_id=$4 AND sequence=$5 AND status='running'
                    AND session_generation=$6
                    AND claim_owner=$7 AND claim_token=$8
                    AND claim_fence_epoch=$9 AND revision=$10",
                &[
                    &terminal_status,
                    &to_u64_i64(request.runtime_commit_cursor, "runtime commit cursor")?,
                    &to_u64_i64(request.created_at_ms, "terminal commit time")?,
                    &request.fence.request_id,
                    &to_i64(request.fence.input_sequence, "input sequence")?,
                    &to_u64_i64(request.fence.session_generation, "session generation")?,
                    &request.fence.claim_owner,
                    &request.fence.claim_token,
                    &to_u64_i64(request.fence.claim_fence_epoch, "claim fence epoch")?,
                    &to_u64_i64(current.revision, "input revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::StaleExecutionFence(format!(
                "input `{}` changed during terminal commit",
                request.fence.request_id
            )));
        }
        let completed = runtime_outbox_tx(&mut transaction, &request.fence.request_id)?
            .ok_or_else(|| {
                session::SessionError::Store(format!(
                    "completed input `{}` disappeared",
                    request.fence.request_id
                ))
            })?;
        append_runtime_history_tx(
            &mut transaction,
            &completed,
            "terminal_commit",
            Some(&request.fence.claim_owner),
            Some(current.revision),
            SessionRuntimeInputStatus::Running,
            SessionRuntimeInputStatus::Completed,
            None,
            request.created_at_ms,
        )?;
        append_input_timeline_event_tx(
            &mut transaction,
            &request_from_outbox(&completed),
            &completed.session_id,
            completed.sequence,
            SessionRuntimeInputStatus::Completed.timeline_event_kind(),
            SessionRuntimeInputStatus::Completed,
            Some(&request.fence.claim_owner),
            None,
            request.created_at_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(SessionTerminalTranscriptReceipt {
            messages,
            inserted,
            input: completed,
        })
    }

    pub fn search_messages(
        &self,
        query: &str,
        session_id: Option<&str>,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        let limit = to_i64(bounded_limit(limit, 1, 500), "message search limit")?;
        self.query_messages(
            "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                    tool_use_id, tool_name, token_usage_json, created_at_ms
               FROM session_messages
              WHERE ($2::text IS NULL OR session_id=$2)
                AND (to_tsvector('simple', coalesce(role,'') || ' ' || coalesce(content_json,'') || ' ' || coalesce(tool_name,''))
                      @@ websearch_to_tsquery('simple', $1)
                     OR content_json ILIKE '%' || $1 || '%')
              ORDER BY sequence ASC LIMIT $3",
            &[&query, &session_id, &limit],
        )
    }

    pub fn search_messages_in_sessions(
        &self,
        query: &str,
        session_ids: &[String],
        limit: usize,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        if session_ids.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let scope = session_ids.to_vec();
        let limit = to_i64(limit.min(500), "message search limit")?;
        self.query_messages(
            "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                    tool_use_id, tool_name, token_usage_json, created_at_ms
               FROM session_messages
              WHERE session_id=ANY($2::text[])
                AND (to_tsvector('simple', coalesce(role,'') || ' ' || coalesce(content_json,'') || ' ' || coalesce(tool_name,''))
                      @@ websearch_to_tsquery('simple', $1)
                     OR content_json ILIKE '%' || $1 || '%')
              ORDER BY sequence ASC LIMIT $3",
            &[&query, &scope, &limit],
        )
    }

    pub fn search_messages_visible(
        &self,
        query: &str,
        owner_principal_id: Option<&str>,
        visible_session_ids: &[String],
        unrestricted: bool,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        let limit = to_i64(bounded_limit(limit, 1, 500), "message search limit")?;
        self.query_messages(
            "SELECT message.stable_message_id, message.session_id, message.sequence,
                    message.role, message.content_json, message.blocks_count,
                    message.tool_use_id, message.tool_name,
                    message.token_usage_json, message.created_at_ms
               FROM session_messages AS message
               JOIN session_records AS session ON session.session_id=message.session_id
              WHERE session.status NOT IN ('deleted','deleting')
                AND ($4::boolean
                     OR session.metadata_json::jsonb ->> 'owner_principal_id'=$2
                     OR session.session_id=ANY($3::text[]))
                AND (to_tsvector('simple',
                         coalesce(message.role,'') || ' ' ||
                         coalesce(message.content_json,'') || ' ' ||
                         coalesce(message.tool_name,''))
                     @@ websearch_to_tsquery('simple', $1)
                     OR message.content_json ILIKE '%' || $1 || '%')
              ORDER BY message.created_at_ms DESC,message.session_id,message.sequence
              LIMIT $5",
            &[
                &query,
                &owner_principal_id,
                &visible_session_ids,
                &unrestricted,
                &limit,
            ],
        )
    }

    pub fn append_event(&self, event: &SessionEvent) -> session::SessionResult<()> {
        event.reject_reserved_cancellation_authorization()?;
        let sequence = to_i64(event.sequence, "event sequence")?;
        let created_at_ms = i64::try_from(event.created_at_ms)
            .map_err(|_| session::SessionError::Store("event time overflow".to_string()))?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "INSERT INTO session_events(session_id, sequence, event_type, event_json, created_at_ms)
                 VALUES ($1,$2,$3,$4,$5)",
                &[
                    &event.session_id,
                    &sequence,
                    &event.event_type,
                    &event.event_json,
                    &created_at_ms,
                ],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    /// Allocate a contiguous, session-local event sequence under the row lock
    /// of its canonical session record. Independent sessions use different
    /// rows and therefore do not serialize behind a process-wide mutex.
    pub fn append_events_allocating_sequence(
        &self,
        events: &[SessionEvent],
    ) -> session::SessionResult<Vec<SessionEvent>> {
        for event in events {
            event.reject_reserved_cancellation_authorization()?;
        }
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let session_id = events[0].session_id.as_str();
        if session_id.trim().is_empty() || events.iter().any(|event| event.session_id != session_id)
        {
            return Err(session::SessionError::Store(
                "session event batch must have one non-empty session id".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        let next: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let mut allocated = Vec::with_capacity(events.len());
        for (index, event) in events.iter().enumerate() {
            let sequence = next
                .checked_add(i64::try_from(index).map_err(|_| {
                    session::SessionError::Store("event batch index overflow".to_string())
                })?)
                .ok_or_else(|| {
                    session::SessionError::Store("event sequence overflow".to_string())
                })?;
            let created_at_ms = i64::try_from(event.created_at_ms)
                .map_err(|_| session::SessionError::Store("event time overflow".to_string()))?;
            let stored_sequence = from_i64(sequence, "event sequence")?;
            let event_json = event_json_with_allocated_sequence(event, stored_sequence)?;
            transaction
                .execute(
                    "INSERT INTO session_events(session_id, sequence, event_type, event_json, created_at_ms)
                     VALUES ($1,$2,$3,$4,$5)",
                    &[
                        &event.session_id,
                        &sequence,
                        &event.event_type,
                        &event_json,
                        &created_at_ms,
                    ],
                )
                .map_err(postgres_error)?;
            let mut event = event.clone();
            event.sequence = stored_sequence;
            event.event_json = event_json;
            allocated.push(event);
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(allocated)
    }

    pub fn append_event_allocating_sequence(
        &self,
        event: &SessionEvent,
    ) -> session::SessionResult<SessionEvent> {
        self.append_events_allocating_sequence(std::slice::from_ref(event))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                session::SessionError::Store("event allocation returned no row".to_string())
            })
    }

    /// Append under the original Session admission and exact claim locks.
    /// Even a replay requires a current claim; stale callers receive no Ack.
    pub fn append_session_domain_event_if_claim_current(
        &self,
        request: &SessionClaimEventAppend,
    ) -> session::SessionResult<SessionEvent> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let stored = append_claim_event_tx(&mut transaction, request)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(stored)
    }

    pub fn append_session_domain_event_if_absent_allocating_sequence(
        &self,
        event: &SessionEvent,
        event_id: &str,
    ) -> session::SessionResult<(SessionEvent, bool)> {
        event.reject_reserved_cancellation_authorization()?;
        if event.event_type != session::SESSION_DOMAIN_EVENT_TYPE || event_id.trim().is_empty() {
            return Err(session::SessionError::Store(
                "idempotent domain append requires SessionDomainEvent and a non-empty event_id"
                    .to_string(),
            ));
        }
        let encoded_event_id = serde_json::from_str::<serde_json::Value>(&event.event_json)
            .ok()
            .and_then(|value| {
                value
                    .get("event_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .ok_or_else(|| {
                session::SessionError::Store(
                    "idempotent domain append requires event_json.event_id".to_string(),
                )
            })?;
        if encoded_event_id != event_id {
            return Err(session::SessionError::Store(
                "idempotent domain append event_id does not match event_json".to_string(),
            ));
        }

        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&event.session_id],
            )
            .map_err(postgres_error)?;
        if let Some(existing) = matching_domain_event_tx(&mut transaction, event, event_id)? {
            transaction.commit().map_err(postgres_error)?;
            return Ok((existing, true));
        }

        let sequence: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id=$1",
                &[&event.session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let stored_sequence = from_i64(sequence, "event sequence")?;
        let event_json = event_json_with_allocated_sequence(event, stored_sequence)?;
        transaction
            .execute(
                "INSERT INTO session_events(session_id, sequence, event_type, event_json, created_at_ms)
                 VALUES ($1,$2,$3,$4,$5)",
                &[
                    &event.session_id,
                    &sequence,
                    &event.event_type,
                    &event_json,
                    &to_u64_i64(event.created_at_ms, "event time")?,
                ],
            )
            .map_err(postgres_error)?;
        transaction.commit().map_err(postgres_error)?;
        let mut stored = event.clone();
        stored.sequence = stored_sequence;
        stored.event_json = event_json;
        Ok((stored, false))
    }

    pub fn get_session_domain_event_by_id(
        &self,
        session_id: &str,
        event_id: &str,
    ) -> session::SessionResult<Option<SessionEvent>> {
        if event_id.trim().is_empty() {
            return Ok(None);
        }
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT session_id, event_type, event_json, sequence, created_at_ms
                   FROM session_events
                  WHERE session_id=$1
                    AND event_type=$2
                    AND event_json::jsonb ->> 'event_id'=$3
                  LIMIT 1",
                &[&session_id, &session::SESSION_DOMAIN_EVENT_TYPE, &event_id],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_event(&row))
            .transpose()
    }

    pub fn append_events_allocating_sequence_if_checkpoint_absent(
        &self,
        events: &[SessionEvent],
        checkpoint_id: &str,
    ) -> session::SessionResult<Option<Vec<SessionEvent>>> {
        for event in events {
            event.reject_reserved_cancellation_authorization()?;
        }
        if events.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let session_id = events[0].session_id.as_str();
        if session_id.trim().is_empty() || events.iter().any(|event| event.session_id != session_id)
        {
            return Err(session::SessionError::Store(
                "atomic session event batch must contain one non-empty session_id".to_string(),
            ));
        }
        if checkpoint_id.trim().is_empty() {
            return Err(session::SessionError::Store(
                "checkpoint-aware event batch requires a non-empty checkpoint_id".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        let exists: bool = transaction
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM session_event_checkpoints WHERE session_id=$1 AND checkpoint_id=$2)",
                &[&session_id, &checkpoint_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        if exists {
            transaction.commit().map_err(postgres_error)?;
            return Ok(None);
        }
        transaction
            .execute(
                "INSERT INTO session_event_checkpoints(session_id,checkpoint_id) VALUES($1,$2)",
                &[&session_id, &checkpoint_id],
            )
            .map_err(postgres_error)?;
        let next: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let mut allocated = Vec::with_capacity(events.len());
        for (offset, event) in events.iter().enumerate() {
            let sequence = next
                .checked_add(i64::try_from(offset).map_err(|_| {
                    session::SessionError::Store("event batch offset overflow".to_string())
                })?)
                .ok_or_else(|| {
                    session::SessionError::Store("event sequence overflow".to_string())
                })?;
            let stored_sequence = from_i64(sequence, "event sequence")?;
            let event_json = event_json_with_allocated_sequence(event, stored_sequence)?;
            transaction.execute(
                "INSERT INTO session_events(session_id, sequence, event_type, event_json, created_at_ms)
                 VALUES ($1,$2,$3,$4,$5)",
                &[&event.session_id, &sequence, &event.event_type, &event_json,
                  &to_u64_i64(event.created_at_ms, "event time")?],
            ).map_err(postgres_error)?;
            let mut stored = event.clone();
            stored.sequence = stored_sequence;
            stored.event_json = event_json;
            allocated.push(stored);
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(Some(allocated))
    }

    pub fn append_context_envelope_event_if_absent_allocating_sequence(
        &self,
        event: &SessionEvent,
    ) -> session::SessionResult<Option<SessionEvent>> {
        event.reject_reserved_cancellation_authorization()?;
        if event.event_type != "ContextEnvelope" {
            return self.append_event_allocating_sequence(event).map(Some);
        }
        let envelope_id = context_envelope_id(&event.event_json)?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&event.session_id],
            )
            .map_err(postgres_error)?;
        let exists: bool = transaction.query_one(
            "SELECT EXISTS(SELECT 1 FROM session_events WHERE event_type='ContextEnvelope'
              AND COALESCE(event_json::jsonb #>> '{envelope,id}', event_json::jsonb ->> 'envelope_id')=$1)",
            &[&envelope_id],
        ).map_err(postgres_error)?.try_get(0).map_err(postgres_error)?;
        if exists {
            transaction.commit().map_err(postgres_error)?;
            return Ok(None);
        }
        let sequence: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id=$1",
                &[&event.session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        transaction.execute(
            "INSERT INTO session_events(session_id, sequence, event_type, event_json, created_at_ms)
             VALUES ($1,$2,$3,$4,$5)",
            &[&event.session_id, &sequence, &event.event_type, &event.event_json,
              &to_u64_i64(event.created_at_ms, "event time")?],
        ).map_err(postgres_error)?;
        transaction.commit().map_err(postgres_error)?;
        let mut stored = event.clone();
        stored.sequence = from_i64(sequence, "event sequence")?;
        Ok(Some(stored))
    }

    pub fn append_context_envelope_event_if_absent(
        &self,
        event: &SessionEvent,
    ) -> session::SessionResult<bool> {
        self.append_context_envelope_event_if_absent_allocating_sequence(event)
            .map(|stored| stored.is_some())
    }

    pub fn get_events(
        &self,
        session_id: &str,
        from_seq: usize,
    ) -> session::SessionResult<Vec<SessionEvent>> {
        self.query_events(
            "SELECT session_id, event_type, event_json, sequence, created_at_ms FROM session_events
             WHERE session_id=$1 AND sequence >= $2 ORDER BY sequence ASC",
            &[&session_id, &to_i64(from_seq, "event sequence")?],
        )
    }

    pub fn get_events_limited(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionEvent>> {
        self.query_events(
            "SELECT session_id, event_type, event_json, sequence, created_at_ms FROM session_events
             WHERE session_id=$1 AND sequence >= $2 ORDER BY sequence ASC LIMIT $3",
            &[
                &session_id,
                &to_i64(from_seq, "event sequence")?,
                &to_i64(limit, "event limit")?,
            ],
        )
    }

    pub fn get_session_domain_timeline_limited(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionEvent>> {
        self.get_events_by_type_limited(
            session_id,
            session::SESSION_DOMAIN_EVENT_TYPE,
            from_seq,
            limit,
        )
    }

    pub fn count_session_domain_timeline_from(
        &self,
        session_id: &str,
        from_seq: usize,
    ) -> session::SessionResult<usize> {
        self.count_events_by_type_from(session_id, session::SESSION_DOMAIN_EVENT_TYPE, from_seq)
    }

    pub fn get_session_domain_events_by_kind_limited(
        &self,
        session_id: &str,
        kind: &str,
        from_seq: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionEvent>> {
        self.query_events(
            "SELECT session_id, event_type, event_json, sequence, created_at_ms
               FROM session_events
              WHERE session_id=$1
                AND event_type=$2
                AND event_json::jsonb ->> 'kind'=$3
                AND sequence >= $4
              ORDER BY sequence ASC
              LIMIT $5",
            &[
                &session_id,
                &session::SESSION_DOMAIN_EVENT_TYPE,
                &kind,
                &to_i64(from_seq, "event sequence")?,
                &to_i64(limit, "event limit")?,
            ],
        )
    }

    pub fn get_session_domain_events_for_epoch(
        &self,
        session_id: &str,
        kind: &str,
        execution_id: &str,
        turn_id: &str,
    ) -> session::SessionResult<Vec<SessionEvent>> {
        self.query_events(
            "SELECT session_id, event_type, event_json, sequence, created_at_ms
               FROM session_events
              WHERE session_id=$1
                AND event_type=$2
                AND event_json::jsonb ->> 'kind'=$3
                AND event_json::jsonb #>> '{payload,execution_id}'=$4
                AND event_json::jsonb #>> '{payload,turn_id}'=$5
              ORDER BY sequence ASC",
            &[
                &session_id,
                &session::SESSION_DOMAIN_EVENT_TYPE,
                &kind,
                &execution_id,
                &turn_id,
            ],
        )
    }

    pub fn get_latest_session_domain_event_by_kind(
        &self,
        session_id: &str,
        kind: &str,
    ) -> session::SessionResult<Option<SessionEvent>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT session_id, event_type, event_json, sequence, created_at_ms
                   FROM session_events
                  WHERE session_id=$1
                    AND event_type=$2
                    AND event_json::jsonb ->> 'kind'=$3
                  ORDER BY sequence DESC
                  LIMIT 1",
                &[&session_id, &session::SESSION_DOMAIN_EVENT_TYPE, &kind],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_event(&row))
            .transpose()
    }

    pub fn count_session_domain_events_by_kind_from(
        &self,
        session_id: &str,
        kind: &str,
        from_seq: usize,
    ) -> session::SessionResult<usize> {
        self.count_events_sql(
            "SELECT COUNT(*) FROM session_events
              WHERE session_id=$1
                AND event_type=$2
                AND event_json::jsonb ->> 'kind'=$3
                AND sequence >= $4",
            &[
                &session_id,
                &session::SESSION_DOMAIN_EVENT_TYPE,
                &kind,
                &to_i64(from_seq, "event sequence")?,
            ],
        )
    }

    pub fn has_session_domain_event_kind(&self, kind: &str) -> session::SessionResult<bool> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_one(
                "SELECT EXISTS(
                    SELECT 1 FROM session_events
                     WHERE event_type=$1
                       AND event_json::jsonb ->> 'kind'=$2
                     LIMIT 1
                )",
                &[&session::SESSION_DOMAIN_EVENT_TYPE, &kind],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)
    }

    pub fn has_session_with_domain_event_kinds(
        &self,
        kinds: &[String],
    ) -> session::SessionResult<bool> {
        if kinds.is_empty() {
            return Ok(false);
        }
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let required = to_i64(kinds.len(), "event kind count")?;
        connection
            .query_one(
                "SELECT EXISTS(
                    SELECT session_id
                      FROM session_events
                     WHERE event_type=$1
                       AND event_json::jsonb ->> 'kind'=ANY($2::text[])
                     GROUP BY session_id
                    HAVING COUNT(DISTINCT event_json::jsonb ->> 'kind') >= $3
                     LIMIT 1
                )",
                &[&session::SESSION_DOMAIN_EVENT_TYPE, &kinds, &required],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)
    }

    pub fn get_events_by_type_limited(
        &self,
        session_id: &str,
        event_type: &str,
        from_seq: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionEvent>> {
        self.query_events(
            "SELECT session_id, event_type, event_json, sequence, created_at_ms FROM session_events
             WHERE session_id=$1 AND event_type=$2 AND sequence >= $3 ORDER BY sequence ASC LIMIT $4",
            &[&session_id, &event_type, &to_i64(from_seq, "event sequence")?, &to_i64(limit, "event limit")?],
        )
    }

    pub fn count_events_from(
        &self,
        session_id: &str,
        from_seq: usize,
    ) -> session::SessionResult<usize> {
        self.count_events_sql(
            "SELECT COUNT(*) FROM session_events WHERE session_id=$1 AND sequence >= $2",
            &[&session_id, &to_i64(from_seq, "event sequence")?],
        )
    }

    pub fn count_events_by_type_from(
        &self,
        session_id: &str,
        event_type: &str,
        from_seq: usize,
    ) -> session::SessionResult<usize> {
        self.count_events_sql("SELECT COUNT(*) FROM session_events WHERE session_id=$1 AND event_type=$2 AND sequence >= $3", &[&session_id, &event_type, &to_i64(from_seq, "event sequence")?])
    }

    pub fn get_context_event_by_envelope_id(
        &self,
        envelope_id: &str,
    ) -> session::SessionResult<Option<SessionEvent>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection.query_opt(
            "SELECT session_id, event_type, event_json, sequence, created_at_ms FROM session_events
             WHERE event_type='ContextEnvelope' AND COALESCE(event_json::jsonb #>> '{envelope,id}', event_json::jsonb ->> 'envelope_id')=$1
             ORDER BY created_at_ms DESC LIMIT 1",
            &[&envelope_id],
        ).map_err(postgres_error)?.map(|row| row_to_event(&row)).transpose()
    }

    pub fn next_event_sequence(&self, session_id: &str) -> session::SessionResult<usize> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let value: i64 = connection
            .query_one(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        from_i64(value, "event sequence")
    }

    pub fn delete_events_from(
        &self,
        session_id: &str,
        from_sequence: usize,
    ) -> session::SessionResult<usize> {
        self.delete_events_sql(
            "DELETE FROM session_events WHERE session_id=$1 AND sequence >= $2",
            &[&session_id, &to_i64(from_sequence, "event sequence")?],
        )
    }

    pub fn delete_events_by_type_from(
        &self,
        session_id: &str,
        event_type: &str,
        from_sequence: usize,
    ) -> session::SessionResult<usize> {
        self.delete_events_sql(
            "DELETE FROM session_events WHERE session_id=$1 AND event_type=$2 AND sequence >= $3",
            &[
                &session_id,
                &event_type,
                &to_i64(from_sequence, "event sequence")?,
            ],
        )
    }

    pub fn save_snapshot(&self, snapshot: &SessionSnapshot) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection.execute(
            "INSERT INTO session_snapshots(session_id,event_idx,messages_json,created_at_ms) VALUES($1,$2,$3,$4)
             ON CONFLICT(session_id,event_idx) DO UPDATE SET messages_json=EXCLUDED.messages_json, created_at_ms=EXCLUDED.created_at_ms",
            &[&snapshot.session_id, &to_i64(snapshot.event_idx, "snapshot index")?, &snapshot.messages_json, &to_u64_i64(snapshot.created_at_ms, "snapshot time")?],
        ).map_err(postgres_error)?;
        Ok(())
    }

    pub fn get_latest_snapshot(
        &self,
        session_id: &str,
    ) -> session::SessionResult<Option<SessionSnapshot>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection.query_opt(
            "SELECT session_id,event_idx,messages_json,created_at_ms FROM session_snapshots WHERE session_id=$1 ORDER BY event_idx DESC LIMIT 1",
            &[&session_id],
        ).map_err(postgres_error)?.map(|row| row_to_snapshot(&row)).transpose()
    }

    pub fn prune_before(&self, cutoff_iso8601: &str) -> session::SessionResult<usize> {
        let mut connection = self.executor.checkout_background().map_err(storage_error)?;
        let deleted = connection
            .execute(
                "DELETE FROM session_records WHERE last_activity < $1",
                &[&cutoff_iso8601],
            )
            .map_err(postgres_error)?;
        Ok(deleted as usize)
    }

    pub fn plan_session_lifecycle(
        &self,
        plan: &SessionLifecyclePlan,
    ) -> session::SessionResult<SessionLifecycleIntent> {
        validate_plan_identity(
            &plan.operation_id,
            &plan.session_id,
            plan.expected_generation,
        )?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        // Session is the aggregate lock root for both lifecycle and input rows.
        let admission = query_input_admission_tx(&mut transaction, &plan.session_id, true)?
            .ok_or_else(|| {
                session::SessionError::Store(format!("session `{}` not found", plan.session_id))
            })?;
        if let Some(existing) =
            query_lifecycle_intent_tx(&mut transaction, &plan.operation_id, true)?
        {
            if existing.session_id == plan.session_id
                && existing.disposition == plan.disposition
                && existing.expected_generation == plan.expected_generation
            {
                transaction.commit().map_err(postgres_error)?;
                return Ok(existing);
            }
            return Err(session::SessionError::Store(format!(
                "Session lifecycle operation `{}` is bound to another identity",
                plan.operation_id
            )));
        }
        if admission.generation != plan.expected_generation || !admission.open {
            return Err(session::SessionError::Store(format!(
                "Session lifecycle plan `{}` expected open generation {}, found generation {} open={}",
                plan.operation_id,
                plan.expected_generation,
                admission.generation,
                admission.open
            )));
        }
        let created_at_ms = to_u64_i64(plan.created_at_ms, "lifecycle plan time")?;
        transaction
            .execute(
                "INSERT INTO session_lifecycle_intents(
                     operation_id,session_id,disposition,phase,last_stable_phase,
                     expected_generation,created_at_ms,updated_at_ms,last_error,revision
                 ) VALUES($1,$2,$3,'planned','planned',$4,$5,$5,NULL,0)",
                &[
                    &plan.operation_id,
                    &plan.session_id,
                    &plan.disposition.as_str(),
                    &to_u64_i64(plan.expected_generation, "lifecycle expected generation")?,
                    &created_at_ms,
                ],
            )
            .map_err(postgres_error)?;
        let intent = query_lifecycle_intent_tx(&mut transaction, &plan.operation_id, false)?
            .ok_or_else(|| {
                session::SessionError::Store(
                    "Session lifecycle plan produced no readable row".to_string(),
                )
            })?;
        transaction.commit().map_err(postgres_error)?;
        Ok(intent)
    }

    pub fn get_session_lifecycle_intent(
        &self,
        operation_id: &str,
    ) -> session::SessionResult<Option<SessionLifecycleIntent>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT operation_id,session_id,disposition,phase,last_stable_phase,
                        expected_generation,created_at_ms,updated_at_ms,last_error,revision
                   FROM session_lifecycle_intents WHERE operation_id=$1",
                &[&operation_id],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_lifecycle_intent(&row))
            .transpose()
    }

    pub fn list_recoverable_session_lifecycle_intents(
        &self,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionLifecycleIntent>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT operation_id,session_id,disposition,phase,last_stable_phase,
                        expected_generation,created_at_ms,updated_at_ms,last_error,revision
                   FROM session_lifecycle_intents
                  WHERE phase != 'unloaded'
                  ORDER BY updated_at_ms ASC,operation_id ASC LIMIT $1",
                &[&to_i64(limit.max(1), "lifecycle recovery limit")?],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_lifecycle_intent)
            .collect()
    }

    pub fn fence_session_lifecycle(
        &self,
        request: &SessionLifecycleFenceRequest,
    ) -> session::SessionResult<SessionLifecycleIntent> {
        request.event.reject_reserved_cancellation_authorization()?;
        validate_fence_metadata(
            &request.actor,
            &request.reason,
            &request.transitional_status,
        )?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current =
            query_lifecycle_intent_tx(&mut transaction, &request.transition.operation_id, true)?
                .ok_or_else(|| {
                    session::SessionError::Store(format!(
                        "Session lifecycle intent `{}` does not exist",
                        request.transition.operation_id
                    ))
                })?;
        request.transition.validate(&current)?;
        if request.transition.next_phase != SessionLifecyclePhase::AdmissionFenced
            || request.event.session_id != current.session_id
        {
            return Err(session::SessionError::Store(
                "Session lifecycle fence identity or phase is invalid".to_string(),
            ));
        }
        let admission = query_input_admission_tx(&mut transaction, &current.session_id, true)?
            .ok_or_else(|| {
                session::SessionError::Store(format!("session `{}` not found", current.session_id))
            })?;
        if admission.generation != current.expected_generation || !admission.open {
            return Err(session::SessionError::Store(format!(
                "Session lifecycle fence `{}` lost generation authority",
                current.operation_id
            )));
        }
        let active = transaction
            .query(
                "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,
                        session_generation,decision,target_turn_id,classification_json,task_route_hint_json,status,
                        runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                        claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                        updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json
                   FROM session_runtime_outbox
                  WHERE session_id=$1 AND session_generation=$2
                    AND status IN (
                        'accepted','classified','queued','claimed',
                        'running','reclassified','blocked'
                    )
                  ORDER BY sequence ASC,request_id ASC FOR UPDATE",
                &[
                    &current.session_id,
                    &to_u64_i64(current.expected_generation, "lifecycle generation")?,
                ],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_runtime_outbox)
            .collect::<session::SessionResult<Vec<_>>>()?;
        let next_generation = current.expected_generation.checked_add(1).ok_or_else(|| {
            session::SessionError::Store("Session generation overflow".to_string())
        })?;
        let updated_at_ms = to_u64_i64(request.transition.updated_at_ms, "lifecycle fence time")?;
        let updated_at = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(updated_at_ms)
            .unwrap_or_else(chrono::Utc::now)
            .to_rfc3339();
        let changed = transaction
            .execute(
                "UPDATE session_records
                    SET input_generation=$1,input_admission_open=FALSE,status=$2,
                        last_activity=$3,updated_at_ms=GREATEST(updated_at_ms,$4)
                  WHERE session_id=$5 AND input_generation=$6
                    AND input_admission_open=TRUE",
                &[
                    &to_u64_i64(next_generation, "next Session generation")?,
                    &request.transitional_status,
                    &updated_at,
                    &updated_at_ms,
                    &current.session_id,
                    &to_u64_i64(current.expected_generation, "lifecycle generation")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "Session lifecycle fence `{}` changed during admission close",
                current.operation_id
            )));
        }
        for before in active {
            let changed = transaction
                .execute(
                    "UPDATE session_runtime_outbox
                        SET status='expired',claim_owner=NULL,claim_token=NULL,
                            claim_fence_epoch=NULL,
                            claim_expires_at_ms=NULL,last_error=$1,terminal_at_ms=$2,
                            updated_at_ms=$2,revision=revision+1
                      WHERE request_id=$3 AND session_generation=$4 AND revision=$5",
                    &[
                        &request.reason,
                        &updated_at_ms,
                        &before.request_id,
                        &to_u64_i64(current.expected_generation, "lifecycle input generation")?,
                        &to_u64_i64(before.revision, "lifecycle input revision")?,
                    ],
                )
                .map_err(postgres_error)?;
            if changed != 1 {
                return Err(session::SessionError::Store(format!(
                    "Session lifecycle fence lost input `{}`",
                    before.request_id
                )));
            }
            let mut expired = before.clone();
            expired.status = SessionRuntimeInputStatus::Expired;
            expired.claim_owner = None;
            expired.claim_token = None;
            expired.claim_expires_at_ms = None;
            expired.last_error = Some(request.reason.clone());
            expired.terminal_at_ms = Some(request.transition.updated_at_ms);
            expired.updated_at_ms = request.transition.updated_at_ms;
            expired.revision = before.revision.saturating_add(1);
            append_runtime_history_tx(
                &mut transaction,
                &expired,
                "lifecycle_fence",
                Some(&request.actor),
                Some(before.revision),
                before.status,
                SessionRuntimeInputStatus::Expired,
                Some(&request.reason),
                request.transition.updated_at_ms,
            )?;
        }
        let closed = SessionInputAdmission {
            session_id: current.session_id.clone(),
            generation: next_generation,
            open: false,
        };
        append_admission_timeline_event_tx(
            &mut transaction,
            &current.session_id,
            current.expected_generation,
            &closed,
            &request.actor,
            &request.reason,
            request.transition.updated_at_ms,
        )?;
        append_allocated_event_tx(&mut transaction, &request.event)?;
        let intent = transition_lifecycle_intent_tx(&mut transaction, &request.transition)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(intent)
    }

    pub fn transition_session_lifecycle(
        &self,
        transition: &SessionLifecycleTransition,
    ) -> session::SessionResult<SessionLifecycleIntent> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let intent = transition_lifecycle_intent_tx(&mut transaction, transition)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(intent)
    }

    pub fn commit_session_lifecycle_tombstone(
        &self,
        request: &SessionLifecycleTombstoneRequest,
    ) -> session::SessionResult<SessionLifecycleIntent> {
        request.event.reject_reserved_cancellation_authorization()?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current =
            query_lifecycle_intent_tx(&mut transaction, &request.transition.operation_id, true)?
                .ok_or_else(|| {
                    session::SessionError::Store(format!(
                        "Session lifecycle intent `{}` does not exist",
                        request.transition.operation_id
                    ))
                })?;
        request.transition.validate(&current)?;
        if request.transition.next_phase != SessionLifecyclePhase::TombstoneCommitted
            || request.record.session_id != current.session_id
            || request.event.session_id != current.session_id
        {
            return Err(session::SessionError::Store(
                "Session lifecycle tombstone identity or phase is invalid".to_string(),
            ));
        }
        query_input_admission_tx(&mut transaction, &current.session_id, true)?.ok_or_else(
            || session::SessionError::Store(format!("session `{}` not found", current.session_id)),
        )?;
        let changed = transaction
            .execute(
                "UPDATE session_records SET
                     platform=$2,chat_id=$3,user_id=$4,model=$5,last_activity=$6,
                     message_count=$7,reset_policy=$8,metadata_json=$9,input_tokens=$10,
                     output_tokens=$11,status=$12,updated_at_ms=$13
                   WHERE session_id=$1 AND input_generation=$14
                     AND input_admission_open=FALSE",
                &[
                    &request.record.session_id,
                    &request.record.platform,
                    &request.record.chat_id,
                    &request.record.user_id,
                    &request.record.model,
                    &request.record.last_activity,
                    &request.record.message_count,
                    &request.record.reset_policy,
                    &request.record.metadata_json,
                    &request.record.input_tokens,
                    &request.record.output_tokens,
                    &request.record.status,
                    &to_u64_i64(request.transition.updated_at_ms, "lifecycle tombstone time")?,
                    &to_u64_i64(
                        current.expected_generation.saturating_add(1),
                        "fenced Session generation",
                    )?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "Session lifecycle tombstone `{}` lost fenced Session authority",
                current.operation_id
            )));
        }
        append_allocated_event_tx(&mut transaction, &request.event)?;
        let intent = transition_lifecycle_intent_tx(&mut transaction, &request.transition)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(intent)
    }
}
