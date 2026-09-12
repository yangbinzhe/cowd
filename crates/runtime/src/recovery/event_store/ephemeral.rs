//! Explicit process-local Runtime event backend for reducer and orchestration tests.
//! It is never selected by production composition and intentionally offers no restart claims.

use super::*;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[derive(Debug, Default)]
struct EphemeralState {
    events: Vec<DurableRuntimeEvent>,
    transactions: BTreeMap<String, AppendTransactionReceipt>,
    transaction_hashes: BTreeMap<String, String>,
    checkpoints: BTreeMap<String, RuntimeProjectionCheckpoint>,
    consumed_leases: BTreeSet<String>,
    terminals: BTreeMap<String, RuntimeSessionOutboxRecord>,
    next_cursor: u64,
}

#[derive(Debug, Default)]
pub struct EphemeralRuntimeEventStore {
    state: StdMutex<EphemeralState>,
}

impl EphemeralRuntimeEventStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn append_inner(
        &self,
        request: AppendTransactionRequest,
        terminal: Option<SessionTerminalInput>,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        validate_runtime_event_transaction(&request)?;
        if let Some(terminal) = &terminal {
            validate_runtime_fenced_terminal(terminal)?;
        }
        let hash = runtime_event_request_hash_with_terminal(&request, terminal.as_ref())?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(receipt) = state.transactions.get(&request.transaction_id) {
            if state.transaction_hashes.get(&request.transaction_id) != Some(&hash) {
                return Err(RuntimeEventStoreError::TransactionConflict {
                    transaction_id: request.transaction_id,
                });
            }
            let mut duplicate = receipt.clone();
            duplicate.duplicate = true;
            return Ok(duplicate);
        }
        for expected in &request.expected_streams {
            let actual = state
                .events
                .iter()
                .filter(|event| event.stream_id == expected.stream_id)
                .map(|event| event.sequence)
                .max()
                .unwrap_or(0);
            if actual != expected.expected_revision {
                return Err(RuntimeEventStoreError::StaleRevision {
                    stream_id: expected.stream_id.clone(),
                    expected: expected.expected_revision,
                    actual,
                });
            }
        }
        state.next_cursor += 1;
        let cursor = state.next_cursor;
        let mut revisions: BTreeMap<String, u64> = request
            .expected_streams
            .iter()
            .map(|value| (value.stream_id.clone(), value.expected_revision))
            .collect();
        let mut event_ids = Vec::with_capacity(request.events.len());
        for (index, input) in request.events.iter().enumerate() {
            let sequence = revisions.get(&input.event.stream_id).copied().unwrap_or(0) + 1;
            revisions.insert(input.event.stream_id.clone(), sequence);
            let event_id = format!("runtime-event-{}", uuid::Uuid::new_v4());
            event_ids.push(event_id.clone());
            state.events.push(DurableRuntimeEvent {
                event_id,
                stream_id: input.event.stream_id.clone(),
                sequence,
                scope: input.event.scope,
                kind: input.event.kind.clone(),
                status: input.event.status.clone(),
                actor: input.event.actor.clone(),
                refs: input.event.refs.clone(),
                payload: input.event.payload.clone(),
                created_at_ms: now_ms(),
                commit_cursor: cursor,
                transaction_id: request.transaction_id.clone(),
                transaction_index: index as u32,
                schema_version: input.schema_version,
                idempotency_key: input.idempotency_key.clone(),
            });
        }
        if let Some(terminal) = terminal {
            state.terminals.insert(
                terminal.terminal_id.clone(),
                terminal_record(&terminal, cursor),
            );
        }
        let receipt = AppendTransactionReceipt {
            commit_cursor: cursor,
            transaction_id: request.transaction_id.clone(),
            request_hash: hash.clone(),
            stream_revisions: request
                .expected_streams
                .iter()
                .map(|expected| CommittedStreamRevision {
                    stream_id: expected.stream_id.clone(),
                    expected_revision: expected.expected_revision,
                    committed_revision: revisions
                        .get(&expected.stream_id)
                        .copied()
                        .unwrap_or(expected.expected_revision),
                })
                .collect(),
            event_ids,
            duplicate: false,
        };
        state
            .transaction_hashes
            .insert(request.transaction_id.clone(), hash);
        state
            .transactions
            .insert(request.transaction_id, receipt.clone());
        Ok(receipt)
    }

    fn matching(
        &self,
        predicate: impl Fn(&DurableRuntimeEvent) -> bool,
    ) -> Vec<DurableRuntimeEvent> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .events
            .iter()
            .filter(|event| predicate(event))
            .cloned()
            .collect()
    }
}

fn after(event: &DurableRuntimeEvent, position: Option<(u64, u32)>) -> bool {
    position.is_none_or(|position| (event.commit_cursor, event.transaction_index) > position)
}

fn terminal_record(input: &SessionTerminalInput, cursor: u64) -> RuntimeSessionOutboxRecord {
    RuntimeSessionOutboxRecord {
        terminal_id: input.terminal_id.clone(),
        message_id: input.message_id.clone(),
        session_id: input.session_id.clone(),
        commit_cursor: cursor,
        payload_ref: input.payload_ref.clone(),
        execution_id: input.execution_id.clone(),
        turn_id: input.turn_id.clone(),
        request_id: input.request_id.clone(),
        session_generation: input.session_generation,
        input_sequence: input.input_sequence,
        input_claim_owner: input.input_claim_owner.clone(),
        input_claim_token: input.input_claim_token.clone(),
        input_claim_revision: input.input_claim_revision,
        status: "pending".into(),
        attempts: 0,
        next_attempt_at_ms: None,
        claim_owner: None,
        claim_expires_at_ms: None,
        failure_class: None,
        last_error: None,
        materialized_at_ms: None,
        revision: 1,
    }
}

impl RuntimeEventStoreBackend for EphemeralRuntimeEventStore {
    fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String> {
        let stream = input.stream_id.clone();
        let revision = self.stream_revision(&stream).map_err(|e| e.to_string())?;
        let receipt = self
            .append_inner(
                AppendTransactionRequest {
                    transaction_id: format!("runtime-tx-{}", uuid::Uuid::new_v4()),
                    expected_streams: vec![ExpectedStreamRevision {
                        stream_id: stream.clone(),
                        expected_revision: revision,
                    }],
                    events: vec![input.into()],
                },
                None,
            )
            .map_err(|e| e.to_string())?;
        self.state
            .lock()
            .map_err(|_| "ephemeral event store lock poisoned".to_string())?
            .events
            .iter()
            .find(|event| receipt.event_ids.contains(&event.event_id))
            .cloned()
            .ok_or_else(|| "committed event missing".to_string())
    }
    fn append_transaction(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        self.append_inner(request, None)
    }
    fn append_transaction_with_terminal(
        &self,
        request: AppendTransactionRequest,
        terminal: SessionTerminalInput,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        self.append_inner(request, Some(terminal))
    }
    fn consume_verified_decision_lease(
        &self,
        lease_id: &str,
        principal_id: &str,
        review_id: &str,
        action: &str,
        scope: &str,
        evidence_digest: &str,
        _credential_epoch: u64,
        _consumed_at_ms: u64,
    ) -> RuntimeEventStoreResult<()> {
        validate_runtime_decision_lease_claims(
            lease_id,
            principal_id,
            review_id,
            action,
            scope,
            evidence_digest,
        )?;
        if !self
            .state
            .lock()
            .unwrap()
            .consumed_leases
            .insert(lease_id.to_string())
        {
            return Err(RuntimeEventStoreError::DecisionLeaseAlreadyConsumed {
                lease_id: lease_id.to_string(),
            });
        }
        Ok(())
    }
    fn append_transaction_with_verified_decision_lease(
        &self,
        request: AppendTransactionRequest,
        lease: &crate::VerifiedDecisionLease,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let receipt = self.append_inner(request, None)?;
        let mut state = self.state.lock().unwrap();
        if !state.consumed_leases.insert(lease.lease_id().to_string()) && !receipt.duplicate {
            return Err(RuntimeEventStoreError::DecisionLeaseAlreadyConsumed {
                lease_id: lease.lease_id().to_string(),
            });
        }
        Ok(receipt)
    }
    fn append_batch_if_revision(
        &self,
        stream_id: String,
        expected_revision: u64,
        transaction_id: String,
        events: Vec<RuntimeTransactionEventInput>,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        self.append_inner(
            AppendTransactionRequest {
                transaction_id,
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id,
                    expected_revision,
                }],
                events,
            },
            None,
        )
    }
    fn events_after_cursor(
        &self,
        cursor: u64,
        max_commits: usize,
    ) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>> {
        let state = self.state.lock().unwrap();
        let mut grouped: BTreeMap<u64, Vec<_>> = BTreeMap::new();
        for event in state
            .events
            .iter()
            .filter(|event| event.commit_cursor > cursor)
        {
            grouped
                .entry(event.commit_cursor)
                .or_default()
                .push(event.clone());
        }
        Ok(grouped
            .into_iter()
            .take(max_commits)
            .map(|(commit_cursor, events)| CommittedEventBatch {
                transaction_id: events[0].transaction_id.clone(),
                commit_cursor,
                events,
            })
            .collect())
    }
    fn projection_checkpoint(
        &self,
        id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeProjectionCheckpoint>> {
        Ok(self.state.lock().unwrap().checkpoints.get(id).cloned())
    }
    fn projection_checkpoints_with_prefix(
        &self,
        prefix: &str,
    ) -> RuntimeEventStoreResult<Vec<RuntimeProjectionCheckpoint>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .checkpoints
            .values()
            .filter(|v| v.projection_id.starts_with(prefix))
            .cloned()
            .collect())
    }
    fn put_projection_checkpoint(
        &self,
        id: &str,
        cursor: u64,
        payload: &serde_json::Value,
        at: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        let expected = self.projection_checkpoint(id)?.map_or(0, |v| v.revision);
        self.compare_and_put_projection_checkpoint(id, cursor, expected, payload, at)
    }
    fn compare_and_put_projection_checkpoint(
        &self,
        id: &str,
        cursor: u64,
        expected: u64,
        payload: &serde_json::Value,
        at: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        let mut s = self.state.lock().unwrap();
        let actual = s.checkpoints.get(id).map_or(0, |v| v.revision);
        if actual != expected {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: id.into(),
                expected,
                actual,
            });
        }
        if let Some(current) = s.checkpoints.get(id) {
            if cursor < current.source_cursor {
                return Err(RuntimeEventStoreError::StaleRevision {
                    stream_id: format!("projection-source:{id}"),
                    expected: cursor,
                    actual: current.source_cursor,
                });
            }
            if cursor == current.source_cursor && current.payload == *payload {
                return Ok(current.clone());
            }
        }
        let value = RuntimeProjectionCheckpoint {
            projection_id: id.into(),
            source_cursor: cursor,
            revision: actual + 1,
            payload: payload.clone(),
            updated_at_ms: at,
        };
        s.checkpoints.insert(id.into(), value.clone());
        Ok(value)
    }
    fn compare_and_repair_projection_checkpoint(
        &self,
        id: &str,
        cursor: u64,
        expected: u64,
        payload: &serde_json::Value,
        at: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        let mut s = self.state.lock().unwrap();
        let actual = s.checkpoints.get(id).map_or(0, |v| v.revision);
        if actual == 0 || actual != expected {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: id.into(),
                expected,
                actual,
            });
        }
        let value = RuntimeProjectionCheckpoint {
            projection_id: id.into(),
            source_cursor: cursor,
            revision: actual + 1,
            payload: payload.clone(),
            updated_at_ms: at,
        };
        s.checkpoints.insert(id.into(), value.clone());
        Ok(value)
    }

    fn delete_projection_checkpoint(&self, id: &str) -> RuntimeEventStoreResult<bool> {
        Ok(self.state.lock().unwrap().checkpoints.remove(id).is_some())
    }
    fn event_by_idempotency_key(
        &self,
        stream: &str,
        key: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>> {
        Ok(self
            .matching(|e| e.stream_id == stream && e.idempotency_key.as_deref() == Some(key))
            .into_iter()
            .next())
    }
    fn stream_revision(&self, stream: &str) -> RuntimeEventStoreResult<u64> {
        Ok(self
            .matching(|e| e.stream_id == stream)
            .into_iter()
            .map(|e| e.sequence)
            .max()
            .unwrap_or(0))
    }
    fn list_stream(&self, stream: &str) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self.matching(|e| e.stream_id == stream))
    }
    fn list_stream_page_desc(
        &self,
        stream: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut v = self.matching(|e| e.stream_id == stream);
        v.reverse();
        Ok(v.into_iter().skip(offset).take(limit).collect())
    }
    fn stream_event_count(&self, stream: &str) -> Result<usize, String> {
        Ok(self.matching(|e| e.stream_id == stream).len())
    }
    fn execution_events_for_session(
        &self,
        id: &str,
        pos: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self
            .matching(|e| after(e, pos) && e.refs.iter().any(|r| r.kind == "session" && r.id == id))
            .into_iter()
            .take(limit)
            .collect())
    }
    fn events_for_root_execution(
        &self,
        id: &str,
        pos: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self
            .matching(|e| {
                after(e, pos)
                    && e.activity_binding()
                        .is_some_and(|b| b.root_execution_id == id)
            })
            .into_iter()
            .take(limit)
            .collect())
    }
    fn events_for_root_execution_kind(
        &self,
        id: &str,
        kind: &str,
        pos: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self
            .matching(|e| {
                e.kind == kind
                    && after(e, pos)
                    && e.activity_binding()
                        .is_some_and(|b| b.root_execution_id == id)
            })
            .into_iter()
            .take(limit)
            .collect())
    }
    fn events_for_activity(
        &self,
        id: &str,
        pos: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self
            .matching(|e| {
                after(e, pos) && e.activity_binding().is_some_and(|b| b.activity_id == id)
            })
            .into_iter()
            .take(limit)
            .collect())
    }
    fn list_scope(
        &self,
        scope: RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut v = self.matching(|e| e.scope == scope);
        v.reverse();
        v.truncate(limit);
        Ok(v)
    }
    fn list_scope_page_asc(
        &self,
        scope: RuntimeEventScope,
        pos: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self
            .matching(|e| e.scope == scope && after(e, pos))
            .into_iter()
            .take(limit)
            .collect())
    }
    fn list_scope_stream_prefix_page_asc(
        &self,
        scope: RuntimeEventScope,
        prefix: &str,
        pos: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self
            .matching(|e| e.scope == scope && e.stream_id.starts_with(prefix) && after(e, pos))
            .into_iter()
            .take(limit)
            .collect())
    }
    fn list_scope_kind_page_asc(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        pos: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self
            .matching(|e| e.scope == scope && e.kind == kind && after(e, pos))
            .into_iter()
            .take(limit)
            .collect())
    }
    fn stream_ids_for_scope(
        &self,
        scope: RuntimeEventScope,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        Ok(self
            .matching(|e| e.scope == scope)
            .into_iter()
            .map(|e| e.stream_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }
    fn stream_ids_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        Ok(self
            .matching(|e| e.scope == scope && e.kind == kind && e.sequence == sequence)
            .into_iter()
            .map(|e| e.stream_id)
            .collect())
    }
    fn stream_ids_for_scope_kind_at_sequence_page(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
        after_key: Option<(u64, String)>,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<(String, u64)>> {
        let mut v = self
            .matching(|e| e.scope == scope && e.kind == kind && e.sequence == sequence)
            .into_iter()
            .map(|e| (e.stream_id, e.commit_cursor))
            .collect::<Vec<_>>();
        v.sort_by_key(|(id, c)| (*c, id.clone()));
        Ok(v.into_iter()
            .filter(|(id, c)| {
                after_key
                    .as_ref()
                    .is_none_or(|(ac, ai)| (*c, id.clone()) > (*ac, ai.clone()))
            })
            .take(limit)
            .collect())
    }
    fn latest_stream_statuses_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<(String, Option<String>)>> {
        let ids = self.stream_ids_for_scope_kind_at_sequence(scope, kind, sequence)?;
        Ok(ids
            .into_iter()
            .map(|id| {
                let status = self
                    .matching(|e| e.stream_id == id)
                    .last()
                    .and_then(|e| e.status.clone());
                (id, status)
            })
            .collect())
    }
    fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut v = self.matching(|_| true);
        v.reverse();
        v.truncate(limit);
        Ok(v)
    }
    fn latest_for_stream(&self, stream: &str) -> Result<Option<DurableRuntimeEvent>, String> {
        Ok(self.matching(|e| e.stream_id == stream).pop())
    }
    fn latest_for_stream_kind(
        &self,
        stream: &str,
        kind: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        Ok(self
            .matching(|e| e.stream_id == stream && e.kind == kind)
            .pop())
    }
    fn enqueue_session_terminal(
        &self,
        id: &str,
        message: &str,
        session: &str,
        cursor: u64,
        payload: &str,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        let mut s = self.state.lock().unwrap();
        if let Some(v) = s.terminals.get(id) {
            return Ok(v.clone());
        }
        let v = RuntimeSessionOutboxRecord {
            terminal_id: id.into(),
            message_id: message.into(),
            session_id: session.into(),
            commit_cursor: cursor,
            payload_ref: payload.into(),
            execution_id: None,
            turn_id: None,
            request_id: None,
            session_generation: None,
            input_sequence: None,
            input_claim_owner: None,
            input_claim_token: None,
            input_claim_revision: None,
            status: "pending".into(),
            attempts: 0,
            next_attempt_at_ms: None,
            claim_owner: None,
            claim_expires_at_ms: None,
            failure_class: None,
            last_error: None,
            materialized_at_ms: None,
            revision: 1,
        };
        s.terminals.insert(id.into(), v.clone());
        Ok(v)
    }
    fn claim_session_terminals(
        &self,
        worker: &str,
        now: u64,
        lease: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        let mut s = self.state.lock().unwrap();
        let mut out = Vec::new();
        for v in s
            .terminals
            .values_mut()
            .filter(|v| v.status == "pending" || v.status == "retry_scheduled")
            .take(limit)
        {
            v.status = "claimed".into();
            v.claim_owner = Some(worker.into());
            v.claim_expires_at_ms = Some(now + lease);
            v.attempts += 1;
            v.revision += 1;
            out.push(v.clone())
        }
        Ok(out)
    }
    fn session_terminal(
        &self,
        id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
        Ok(self.state.lock().unwrap().terminals.get(id).cloned())
    }
    fn has_unsettled_session_terminals(&self, session: &str) -> RuntimeEventStoreResult<bool> {
        Ok(self.state.lock().unwrap().terminals.values().any(|v| {
            v.session_id == session && !matches!(v.status.as_str(), "materialized" | "suppressed")
        }))
    }
    fn materialized_session_terminals_after(
        &self,
        session: &str,
        cursor: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .terminals
            .values()
            .filter(|v| {
                v.session_id == session && v.status == "materialized" && v.commit_cursor > cursor
            })
            .take(limit)
            .cloned()
            .collect())
    }
    fn session_terminal_health(&self) -> RuntimeEventStoreResult<RuntimeSessionOutboxHealth> {
        let s = self.state.lock().unwrap();
        let mut h = RuntimeSessionOutboxHealth::default();
        for v in s.terminals.values() {
            match v.status.as_str() {
                "pending" => h.pending += 1,
                "claimed" => h.claimed += 1,
                "retry_scheduled" => h.retry_scheduled += 1,
                "materialized" => h.materialized += 1,
                "suppressed" => h.suppressed += 1,
                _ => h.blocked += 1,
            }
        }
        Ok(h)
    }
    fn blocked_session_terminals(
        &self,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .terminals
            .values()
            .filter(|v| {
                matches!(
                    v.status.as_str(),
                    "blocked" | "authorization_blocked" | "corrupt_payload"
                )
            })
            .take(limit)
            .cloned()
            .collect())
    }
    fn retry_session_terminal(
        &self,
        id: &str,
        _actor: &str,
        reason: &str,
        now: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.mutate_terminal(id, None, None, |v| {
            v.status = "retry_scheduled".into();
            v.next_attempt_at_ms = Some(now);
            v.last_error = Some(reason.into())
        })
    }
    fn adopt_session_terminal_fence(
        &self,
        r: &RuntimeSessionTerminalFenceAdoption,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.mutate_terminal(
            &r.terminal_id,
            None,
            Some(r.expected_terminal_revision),
            |v| {
                v.request_id = Some(r.request_id.clone());
                v.turn_id = Some(r.turn_id.clone());
                v.session_generation = Some(r.session_generation);
                v.input_sequence = Some(r.input_sequence);
                v.input_claim_owner = Some(r.claim_owner.clone());
                v.input_claim_token = Some(r.claim_token.clone());
                v.input_claim_revision = Some(r.claim_revision)
            },
        )
    }
    fn ack_session_terminal(
        &self,
        id: &str,
        worker: &str,
        rev: u64,
        now: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.mutate_terminal(id, Some(worker), Some(rev), |v| {
            v.status = "materialized".into();
            v.materialized_at_ms = Some(now);
            v.claim_owner = None;
            v.claim_expires_at_ms = None
        })
    }
    fn suppress_session_terminal(
        &self,
        id: &str,
        worker: &str,
        rev: u64,
        reason: &str,
        _now: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.mutate_terminal(id, Some(worker), Some(rev), |v| {
            v.status = "suppressed".into();
            v.last_error = Some(reason.into())
        })
    }
    fn fail_session_terminal(
        &self,
        id: &str,
        worker: &str,
        rev: u64,
        class: RuntimeSessionOutboxFailureClass,
        error: &str,
        retry: u64,
        max: u32,
        _now: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.mutate_terminal(id, Some(worker), Some(rev), |v| {
            v.failure_class = Some(class.as_str().into());
            v.last_error = Some(error.into());
            if matches!(class, RuntimeSessionOutboxFailureClass::Retryable) && v.attempts < max {
                v.status = "retry_scheduled".into();
                v.next_attempt_at_ms = Some(retry)
            } else {
                v.status = "blocked".into()
            }
        })
    }
}

impl EphemeralRuntimeEventStore {
    fn mutate_terminal(
        &self,
        id: &str,
        worker: Option<&str>,
        revision: Option<u64>,
        change: impl FnOnce(&mut RuntimeSessionOutboxRecord),
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        let mut s = self.state.lock().unwrap();
        let v = s
            .terminals
            .get_mut(id)
            .ok_or_else(|| RuntimeEventStoreError::Corrupt(format!("terminal `{id}` not found")))?;
        if revision.is_some_and(|r| r != v.revision)
            || worker.is_some_and(|w| v.claim_owner.as_deref() != Some(w))
        {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: id.into(),
                expected: revision.unwrap_or(v.revision),
                actual: v.revision,
            });
        }
        change(v);
        v.revision += 1;
        Ok(v.clone())
    }
}
