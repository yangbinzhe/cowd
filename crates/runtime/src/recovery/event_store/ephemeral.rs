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
    #[cfg(test)]
    sequence_hydrates: usize,
    #[cfg(test)]
    recent_clones: usize,
    event_keys: BTreeMap<(String, String), usize>,
    artifact_roots: BTreeSet<String>,
    events: Vec<DurableRuntimeEvent>,
    stream_positions: BTreeMap<String, BTreeMap<u64, usize>>,
    commit_positions: BTreeMap<u64, Vec<usize>>,
    encoded_bytes: Vec<usize>,
    position_indexes: BTreeMap<(String, String), BTreeMap<(u64, u32), usize>>,
    transactions: BTreeMap<String, AppendTransactionReceipt>,
    transaction_hashes: BTreeMap<String, String>,
    checked_evidence: BTreeMap<String, CheckedSourceEvidence>,
    checkpoints: BTreeMap<String, RuntimeProjectionCheckpoint>,
    consumed_leases: BTreeSet<String>,
    terminals: BTreeMap<String, RuntimeSessionOutboxRecord>,
    terminal_sources: BTreeMap<(String, String, String), BTreeSet<String>>,
    next_cursor: u64,
    source_fence: Option<RuntimeSourceMutationFence>,
}

impl EphemeralState {
    fn source_mutation_fence(&mut self) -> RuntimeSourceMutationFence {
        self.source_fence.get_or_insert_with(|| RuntimeSourceMutationFence {
            incarnation: uuid::Uuid::new_v4().to_string(),
            mutation_epoch: 0,
        }).clone()
    }

    fn terminal_source(record: &RuntimeSessionOutboxRecord) -> Option<(String, String, String)> {
        Some((
            record.session_id.clone(),
            record.execution_id.clone()?,
            record.turn_id.clone()?,
        ))
    }

    fn insert_terminal(&mut self, record: RuntimeSessionOutboxRecord) {
        if let Some(previous) = self.terminals.get(&record.terminal_id) {
            if let Some(key) = Self::terminal_source(previous) {
                if let Some(identities) = self.terminal_sources.get_mut(&key) {
                    identities.remove(&record.terminal_id);
                    if identities.is_empty() {
                        self.terminal_sources.remove(&key);
                    }
                }
            }
        }
        if let Some(key) = Self::terminal_source(&record) {
            self.terminal_sources
                .entry(key)
                .or_default()
                .insert(record.terminal_id.clone());
        }
        self.terminals.insert(record.terminal_id.clone(), record);
    }
}

#[cfg(test)]
mod terminal_source_tests {
    use super::*;

    #[test]
    fn r1_terminal_discovery_requires_unique_exact_execution_source() {
        let store = EphemeralRuntimeEventStore::default();
        let mut expected = store
            .enqueue_session_terminal("target", "message", "session-a", 7, "payload")
            .unwrap();
        expected.execution_id = Some("execution-a".into());
        expected.turn_id = Some("turn-a".into());
        {
            let mut state = store.state.lock().unwrap();
            state.insert_terminal(expected.clone());
            for position in 0..2048 {
                let mut unrelated = expected.clone();
                unrelated.terminal_id = format!("unrelated-{position}");
                unrelated.session_id = format!("other-{position}");
                unrelated.commit_cursor = 100_000 + position;
                state.insert_terminal(unrelated);
            }
        }
        assert_eq!(
            store
                .session_terminal_for_execution("session-a", "execution-a", "turn-a")
                .unwrap(),
            Some(expected.clone())
        );
        assert_eq!(expected.status, "pending");
        assert!(expected.materialized_at_ms.is_none());
        for tuple in [
            ("session-b", "execution-a", "turn-a"),
            ("session-a", "execution-b", "turn-a"),
            ("session-a", "execution-a", "turn-b"),
        ] {
            assert!(store
                .session_terminal_for_execution(tuple.0, tuple.1, tuple.2)
                .unwrap()
                .is_none());
        }
        for invalid in ["", " ", " execution-a", "execution-a\0"] {
            assert!(matches!(
                store.session_terminal_for_execution("session-a", invalid, "turn-a"),
                Err(RuntimeEventStoreError::InvalidTransaction(_))
            ));
        }
        let updated = store
            .mutate_terminal("target", None, Some(1), |record| {
                record.turn_id = Some("turn-new".into())
            })
            .unwrap();
        assert!(store
            .session_terminal_for_execution("session-a", "execution-a", "turn-a")
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .session_terminal_for_execution("session-a", "execution-a", "turn-new")
                .unwrap(),
            Some(updated.clone())
        );
        let store = EphemeralRuntimeEventStore {
            state: StdMutex::new(std::mem::take(&mut *store.state.lock().unwrap())),
        };
        assert_eq!(
            store
                .session_terminal_for_execution("session-a", "execution-a", "turn-new")
                .unwrap(),
            Some(updated.clone())
        );
        let mut conflicting = updated;
        conflicting.terminal_id = "conflict".into();
        store.state.lock().unwrap().insert_terminal(conflicting);
        assert!(matches!(
            store.session_terminal_for_execution("session-a", "execution-a", "turn-new"),
            Err(RuntimeEventStoreError::Corrupt(_))
        ));
        let owner_restart = EphemeralRuntimeEventStore {
            state: StdMutex::new(std::mem::take(&mut *store.state.lock().unwrap())),
        };
        assert!(matches!(
            owner_restart.session_terminal_for_execution("session-a", "execution-a", "turn-new"),
            Err(RuntimeEventStoreError::Corrupt(_))
        ));
        assert!(owner_restart
            .session_terminal_for_execution("session-missing", "execution-a", "turn-new")
            .unwrap()
            .is_none());
    }
}

#[derive(Debug, Default)]
pub struct EphemeralRuntimeEventStore {
    state: StdMutex<EphemeralState>,
}

#[cfg(test)]
mod bounded_read_contract_tests {
    use super::*;

    #[test]
    fn fixture_evidence_try_lock_and_bytes_reject_before_clone() {
        let backend = Arc::new(EphemeralRuntimeEventStore::new());
        let store = RuntimeEventStore::from_backend(backend.clone());
        store.append(RuntimeEventInput {
            stream_id:"agent:bounded".into(), scope:RuntimeEventScope::Agent,
            kind:"agent.local_exit".into(), status:None, actor:None, refs:vec![],
            payload:serde_json::json!({"body":"x".repeat(1024 * 1024)}),
        }).unwrap();
        let request = FixtureEvidencePageRequest {
            stream_id:"agent:bounded", after_sequence:0, through_sequence:u64::MAX,
            max_events:100, max_bytes:1024 * 1024,
            deadline:Instant::now() + Duration::from_secs(5),
        };
        let guard = backend.state.lock().unwrap();
        assert!(matches!(store.fixture_evidence_page(&request), Err("Unavailable: event store lock busy or poisoned")));
        drop(guard);
        let page = store.fixture_evidence_page(&request).unwrap();
        assert!(page.records.is_empty());
        assert_eq!(page.encoded_bytes, 0);
        assert_eq!(page.next_sequence, 0);
        assert_eq!(page.head, 1);
        assert_eq!(page.stop, Some("bytes"));
        let expired = FixtureEvidencePageRequest { deadline:Instant::now(), ..request };
        assert!(store.fixture_evidence_page(&expired).is_err());
    }

    #[test]
    fn r1_bounded_history_ephemeral_sequence_and_recent_zero_budget_do_not_hydrate() {
        let backend = Arc::new(EphemeralRuntimeEventStore::new());
        let store = RuntimeEventStore::from_backend(backend.clone());
        let mut expected = Vec::new();
        for index in 0..120 {
            let event = store.append(RuntimeEventInput { stream_id: "mission:bounded".into(),scope: RuntimeEventScope::Session,
                kind:"session.bounded_probe".into(),status:None,actor:Some(format!("actor-{index}")),
                refs:vec![RuntimeEventRef {kind:"task".into(),id:"selected-task".into()}],
                payload:serde_json::json!({"index":index,"body":if index==0 {"x".repeat(2*1024*1024)} else {String::new()}}),
            }).unwrap();
            expected.push(harness_contract::mission::MissionControlEventLine {
                event_id: event.event_id,
                stream_id: event.stream_id,
                cursor: event.commit_cursor,
                transaction_index: event.transaction_index,
                scope: "session".into(),
                kind: event.kind,
                status: event.status,
                actor: event.actor,
                created_at_ms: event.created_at_ms,
            });
        }
        let through = store.durable_commit_cursor().unwrap();
        let references = vec![RuntimeEventRef {
            kind: "task".into(),
            id: "selected-task".into(),
        }];
        let lines = store
            .recent_event_lines("mission:bounded", &references, through, 50, 1024 * 1024)
            .unwrap();
        expected.reverse();
        expected.truncate(50);
        assert_eq!(lines, expected);
        assert_eq!(backend.state.lock().unwrap().recent_clones, 50);
        for (events, bytes) in [(0, 1024), (50, 0)] {
            assert!(store
                .list_stream_after("mission:bounded", 0, 120, events, bytes)
                .unwrap()
                .is_empty());
            assert!(store
                .list_stream_before("mission:bounded", 120, events, bytes)
                .unwrap()
                .is_empty());
            assert!(store
                .recent_event_lines("mission:bounded", &references, through, events, bytes)
                .unwrap()
                .is_empty());
        }
        assert!(store
            .list_stream_after("mission:bounded", 120, 120, 50, 1024)
            .unwrap()
            .is_empty());
        assert!(store
            .list_stream_after("mission:bounded", u64::MAX, 120, 0, 0)
            .is_err());
        assert!(store
            .list_stream_before("mission:bounded", u64::MAX, 0, 0)
            .is_err());
        assert!(store
            .recent_event_lines("mission:bounded", &references, u64::MAX, 0, 0)
            .is_err());
        assert!(store
            .list_stream_after("mission:bounded", 0, 120, 50, 1)
            .unwrap_err()
            .starts_with("HistoryBudgetExceeded:"));
        assert!(store
            .list_stream_before("mission:bounded", 120, 50, 1)
            .unwrap_err()
            .starts_with("HistoryBudgetExceeded:"));
        assert!(store
            .recent_event_lines("mission:bounded", &references, through, 50, 1)
            .unwrap_err()
            .starts_with("HistoryBudgetExceeded:"));
        assert_eq!(backend.state.lock().unwrap().sequence_hydrates, 0);
        assert_eq!(backend.state.lock().unwrap().recent_clones, 50);
        let next = store
            .append(RuntimeEventInput {
                stream_id: "mission:bounded".into(),
                scope: RuntimeEventScope::Session,
                kind: "session.after_h".into(),
                status: None,
                actor: Some("x".repeat(1024 * 1024 + 1)),
                refs: Vec::new(),
                payload: serde_json::json!({}),
            })
            .unwrap();
        assert_eq!(
            store
                .recent_event_lines("mission:bounded", &references, through, 50, 1024 * 1024)
                .unwrap(),
            lines
        );
        let clones = backend.state.lock().unwrap().recent_clones;
        assert!(store
            .recent_event_lines(
                "mission:bounded",
                &references,
                next.commit_cursor,
                50,
                1024 * 1024
            )
            .unwrap_err()
            .starts_with("HistoryBudgetExceeded:"));
        assert_eq!(backend.state.lock().unwrap().recent_clones, clones);
    }
}

impl EphemeralRuntimeEventStore {
    fn sequence_page(
        &self,
        stream: &str,
        after_sequence: u64,
        through_sequence: u64,
        max_events: usize,
        max_bytes: usize,
        descending: bool,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        i64::try_from(through_sequence).map_err(|_| "through_sequence overflow".to_string())?;
        i64::try_from(after_sequence).map_err(|_| "after_sequence overflow".to_string())?;
        if max_events == 0 || max_bytes == 0 || through_sequence <= after_sequence {
            return Ok(Vec::new());
        }
        #[allow(unused_mut)]
        let mut state = self
            .state
            .lock()
            .map_err(|_| "event store lock poisoned".to_string())?;
        let Some(positions) = state.stream_positions.get(stream) else {
            return Ok(Vec::new());
        };
        let range = positions.range((
            std::ops::Bound::Excluded(after_sequence),
            std::ops::Bound::Included(through_sequence),
        ));
        let candidates: Vec<usize> = if descending {
            range
                .rev()
                .take(max_events.min(100))
                .map(|(_, position)| *position)
                .collect()
        } else {
            range
                .take(max_events.min(100))
                .map(|(_, position)| *position)
                .collect()
        };
        let budget = max_bytes.min(1024 * 1024);
        let mut total = 0usize;
        let mut selected = Vec::new();
        for position in candidates {
            let bytes = state.encoded_bytes[position];
            if bytes > budget.saturating_sub(total) {
                if selected.is_empty() {
                    return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
                        state.events[position].event_id.clone(),
                    )
                    .to_string());
                }
                break;
            }
            total += bytes;
            selected.push(position);
        }
        #[cfg(test)]
        {
            state.sequence_hydrates += selected.len();
        }
        Ok(selected
            .into_iter()
            .map(|position| state.events[position].clone())
            .collect())
    }
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn append_inner(
        &self,
        request: AppendTransactionRequest,
        terminal: Option<SessionTerminalInput>,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        self.append_inner_checked(request, terminal, None)
            .map(|(receipt, _)| receipt)
    }

    fn append_inner_checked(
        &self,
        request: AppendTransactionRequest,
        terminal: Option<SessionTerminalInput>,
        checked: Option<(&CheckedAppendRequest, &dyn CheckedSourceValidator)>,
    ) -> RuntimeEventStoreResult<(AppendTransactionReceipt, Option<CheckedSourceEvidence>)> {
        validate_runtime_event_transaction(&request)?;
        RuntimeEventStore::validate_current_content_inputs(&request)?;
        if let Some(terminal) = &terminal {
            validate_runtime_fenced_terminal(terminal)?;
        }
        let hash = match checked {
            Some((request, validator)) => {
                if request.frame_digest != validator.frame_digest() {
                    return Err(RuntimeEventStoreError::InvalidTransaction(
                        "source frame mismatch".into(),
                    ));
                }
                request.identity_hash()?
            }
            None => runtime_event_request_hash_with_terminal(&request, terminal.as_ref())?,
        };
        let held_since = std::time::Instant::now();
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
            return Ok((
                duplicate,
                state.checked_evidence.get(&request.transaction_id).cloned(),
            ));
        }
        for input in &request.events {
            if input.idempotency_key.as_ref().is_some_and(|key| {
                state.events.iter().any(|event| {
                    event.stream_id == input.event.stream_id
                        && event.idempotency_key.as_ref() == Some(key)
                })
            }) {
                return Err(RuntimeEventStoreError::InvalidTransaction(
                    "event idempotency key already belongs to another transaction".into(),
                ));
            }
        }
        let mut evidence = checked.map(|(request, _)| CheckedSourceEvidence {
            frame_digest: request.frame_digest.clone(),
            sources: Vec::new(),
            events_read: 0,
            bytes_read: 0,
            pages_read: 0,
        });
        let mut revisions = BTreeMap::new();
        for source in &request.expected_streams {
            let positions = state.stream_positions.get(&source.stream_id);
            let actual = positions
                .and_then(|positions| positions.last_key_value())
                .map_or(0, |(sequence, _)| *sequence);
            if let Some((checked, validator)) = checked {
                let evidence = evidence.as_mut().expect("checked evidence initialized");
                let count = actual
                    .checked_sub(source.expected_revision)
                    .ok_or_else(|| {
                        RuntimeEventStoreError::Corrupt("source cut exceeds current head".into())
                    })?;
                let count = usize::try_from(count).map_err(|_| {
                    RuntimeEventStoreError::HistoryBudgetExceeded("source count overflow".into())
                })?;
                evidence.events_read = evidence.events_read.saturating_add(count);
                evidence.pages_read = evidence.pages_read.saturating_add(count.div_ceil(64));
                if evidence.events_read > checked.budget.max_events
                    || evidence.pages_read > checked.budget.max_pages
                {
                    return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
                        "cumulative source tail budget".into(),
                    ));
                }
                let selected = positions
                    .into_iter()
                    .flat_map(|positions| {
                        positions.range((
                            std::ops::Bound::Excluded(source.expected_revision),
                            std::ops::Bound::Included(actual),
                        ))
                    })
                    .map(|(sequence, position)| (*sequence, *position))
                    .collect::<Vec<_>>();
                if selected.len() != count {
                    return Err(RuntimeEventStoreError::Corrupt(
                        "incomplete source tail".into(),
                    ));
                }
                for (offset, (sequence, position)) in selected.iter().enumerate() {
                    if *sequence != source.expected_revision + offset as u64 + 1 {
                        return Err(RuntimeEventStoreError::Corrupt(
                            "discontinuous source tail".into(),
                        ));
                    }
                    evidence.bytes_read = evidence
                        .bytes_read
                        .saturating_add(16)
                        .saturating_add(state.encoded_bytes[*position]);
                }
                if evidence.bytes_read > checked.budget.max_bytes
                    || held_since.elapsed().as_millis() > u128::from(checked.budget.max_lock_millis)
                {
                    return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
                        "cumulative source bytes or held time".into(),
                    ));
                }
                let events = selected
                    .iter()
                    .map(|(_, position)| state.events[*position].clone())
                    .collect::<Vec<_>>();
                validator.validate_tail(source, actual, &events)?;
                if held_since.elapsed().as_millis() > u128::from(checked.budget.max_lock_millis) {
                    return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
                        "source validator held time".into(),
                    ));
                }
                evidence.sources.push(CommittedStreamRevision {
                    stream_id: source.stream_id.clone(),
                    expected_revision: source.expected_revision,
                    committed_revision: actual,
                });
            } else if actual != source.expected_revision {
                return Err(RuntimeEventStoreError::StaleRevision {
                    stream_id: source.stream_id.clone(),
                    expected: source.expected_revision,
                    actual,
                });
            }
            revisions.insert(source.stream_id.clone(), actual);
        }
        let cursor = state.next_cursor + 1;
        let mut event_ids = Vec::with_capacity(request.events.len());
        let mut prepared_events = Vec::with_capacity(request.events.len());
        for (index, input) in request.events.iter().enumerate() {
            let sequence = revisions.get(&input.event.stream_id).copied().unwrap_or(0) + 1;
            revisions.insert(input.event.stream_id.clone(), sequence);
            let event_id = format!("runtime-event-{}", uuid::Uuid::new_v4());
            event_ids.push(event_id.clone());
            let event = DurableRuntimeEvent {
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
            };
            let encoded_bytes = serde_json::to_vec(&event)?.len();
            prepared_events.push((event, encoded_bytes));
        }
        if checked.is_some_and(|(request, _)| {
            held_since.elapsed().as_millis() > u128::from(request.budget.max_lock_millis)
        }) {
            return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
                "checked transaction preparation held time".into(),
            ));
        }
        // Serialization and all fallible validation finish before publishing
        // any event, index or cursor under the original state lock.
        state.next_cursor = cursor;
        for (index, (input, (event, encoded_bytes))) in
            request.events.iter().zip(prepared_events).enumerate()
        {
            let sequence = event.sequence;
            state.events.push(event);
            let position = state.events.len() - 1;
            state
                .commit_positions
                .entry(cursor)
                .or_default()
                .push(position);
            state.encoded_bytes.push(encoded_bytes);
            state
                .stream_positions
                .entry(input.event.stream_id.clone())
                .or_default()
                .insert(sequence, position);
            let mut keys = BTreeSet::new();
            keys.insert(("session".to_string(), input.event.stream_id.clone()));
            keys.insert(("root".to_string(), input.event.stream_id.clone()));
            if let Some(binding) = input.event.activity_binding() {
                keys.insert(("session".to_string(), binding.session_id));
                keys.insert(("root".to_string(), binding.root_execution_id));
                keys.insert(("activity".to_string(), binding.activity_id));
            }
            for reference in &input.event.refs {
                if reference.kind == "artifact" {
                    state.artifact_roots.insert(reference.id.clone());
                }
                if reference.kind == "session" {
                    keys.insert(("session".to_string(), reference.id.clone()));
                }
                if matches!(reference.kind.as_str(), "execution_graph" | "execution") {
                    keys.insert(("root".to_string(), reference.id.clone()));
                }
            }
            if let Some(key) = &input.idempotency_key {
                state
                    .event_keys
                    .insert((input.event.stream_id.clone(), key.clone()), position);
            }
            if input.event.scope == RuntimeEventScope::ExecutionGraph {
                keys.insert(("root".to_string(), input.event.stream_id.clone()));
            }
            if input.event.scope == RuntimeEventScope::Session {
                keys.insert((
                    "session".to_string(),
                    input
                        .event
                        .stream_id
                        .strip_prefix("session:")
                        .unwrap_or(&input.event.stream_id)
                        .to_string(),
                ));
            }
            for key in keys {
                let kind_key = (format!("{}:{}", key.0, input.event.kind), key.1.clone());
                state
                    .position_indexes
                    .entry(key)
                    .or_default()
                    .insert((cursor, index as u32), position);
                state
                    .position_indexes
                    .entry(kind_key)
                    .or_default()
                    .insert((cursor, index as u32), position);
            }
        }
        if let Some(terminal) = terminal {
            state.insert_terminal(terminal_record(&terminal, cursor));
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
        if let Some(evidence) = &evidence {
            state
                .checked_evidence
                .insert(request.transaction_id.clone(), evidence.clone());
        }
        state
            .transactions
            .insert(request.transaction_id, receipt.clone());
        Ok((receipt, evidence))
    }

    fn matching(
        &self,
        predicate: impl Fn(&DurableRuntimeEvent) -> bool,
    ) -> Vec<DurableRuntimeEvent> {
        self.matching_limited(predicate, usize::MAX, false)
    }

    fn matching_limited(
        &self,
        predicate: impl Fn(&DurableRuntimeEvent) -> bool,
        limit: usize,
        descending: bool,
    ) -> Vec<DurableRuntimeEvent> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if descending {
            state
                .events
                .iter()
                .rev()
                .filter(|event| predicate(event))
                .take(limit)
                .cloned()
                .collect()
        } else {
            state
                .events
                .iter()
                .filter(|event| predicate(event))
                .take(limit)
                .cloned()
                .collect()
        }
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
    #[cfg(any(test, feature = "test-fixtures"))]
    fn fixture_evidence_page(
        &self,
        request: &FixtureEvidencePageRequest<'_>,
    ) -> Result<FixtureEvidencePage, &'static str> {
        if Instant::now() >= request.deadline {
            return Err("Unavailable: deadline exhausted before lock");
        }
        let state = self.state.try_lock().map_err(|_| "Unavailable: event store lock busy or poisoned")?;
        let positions = state.stream_positions.get(request.stream_id);
        let head = positions.and_then(|positions| positions.last_key_value()).map_or(0, |(sequence, _)| *sequence);
        let mut page = FixtureEvidencePage {
            head, next_sequence: request.after_sequence, encoded_bytes: 0,
            records: Vec::new(), stop: None,
        };
        let through = request.through_sequence.min(head);
        if through <= request.after_sequence {
            return Ok(page);
        }
        if let Some(positions) = positions {
            for (sequence, position) in positions.range((
                std::ops::Bound::Excluded(request.after_sequence),
                std::ops::Bound::Included(through),
            )) {
                if Instant::now() >= request.deadline {
                    page.stop = Some("deadline");
                    break;
                }
                if page.records.len() >= request.max_events.min(100) {
                    page.stop = Some("events");
                    break;
                }
                let bytes = state.encoded_bytes[*position];
                if bytes > request.max_bytes.min(1024 * 1024).saturating_sub(page.encoded_bytes) {
                    page.stop = Some("bytes");
                    break;
                }
                page.encoded_bytes += bytes;
                page.records.push(state.events[*position].clone());
                page.next_sequence = *sequence;
            }
        }
        Ok(page)
    }

    fn bounded_event_by_idempotency_key(
        &self,
        stream: &str,
        key: &str,
        through: u64,
        max_bytes: usize,
    ) -> RuntimeEventStoreResult<RuntimeBoundedEventLookup> {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(position) = state.event_keys.get(&(stream.into(), key.into())) else {
            return Ok(RuntimeBoundedEventLookup::Absent);
        };
        let event = &state.events[*position];
        if event.commit_cursor > through {
            return Ok(RuntimeBoundedEventLookup::Absent);
        }
        let bytes = state.encoded_bytes[*position];
        if bytes > max_bytes {
            return Ok(RuntimeBoundedEventLookup::Deferred { required_bytes: bytes });
        }
        Ok(RuntimeBoundedEventLookup::Record { record: Box::new(event.clone()), encoded_bytes: bytes })
    }

    fn recent_event_lines(
        &self,
        stream_id: &str,
        references: &[RuntimeEventRef],
        through_cursor: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<harness_contract::mission::MissionControlEventLine>, String> {
        i64::try_from(through_cursor).map_err(|_| "through_cursor overflow".to_string())?;
        let directory_bytes = references
            .iter()
            .try_fold(stream_id.len(), |bytes, reference| {
                bytes
                    .checked_add(reference.kind.len())?
                    .checked_add(reference.id.len())
            })
            .ok_or_else(|| "HistoryBudgetExceeded: recent scope directory overflow".to_string())?;
        if references.len() > 4096 || directory_bytes > 1024 * 1024 {
            return Err("HistoryBudgetExceeded: recent scope directory".into());
        }
        if max_events == 0 || max_bytes == 0 || through_cursor == 0 {
            return Ok(Vec::new());
        }
        let limit = max_events.min(50);
        let budget = max_bytes.min(1024 * 1024);
        #[allow(unused_mut)]
        let mut state = self
            .state
            .lock()
            .map_err(|_| "event lock poisoned".to_string())?;
        let mut selected = Vec::with_capacity(limit);
        let mut seen = BTreeSet::new();
        let mut total = 0usize;
        'commits: for (_, positions) in state.commit_positions.range(..=through_cursor).rev() {
            for position in positions.iter().rev() {
                let event = &state.events[*position];
                if event.stream_id != stream_id
                    && !event
                        .refs
                        .iter()
                        .any(|reference| references.contains(reference))
                {
                    continue;
                }
                if !seen.insert(event.event_id.as_str()) {
                    continue;
                }
                let scalar_bytes = event
                    .event_id
                    .len()
                    .saturating_add(event.stream_id.len())
                    .saturating_add(event.scope.as_str().len())
                    .saturating_add(event.kind.len())
                    .saturating_add(event.status.as_ref().map_or(0, String::len))
                    .saturating_add(event.actor.as_ref().map_or(0, String::len));
                let bytes = scalar_bytes.saturating_mul(6).saturating_add(384);
                if bytes > budget.saturating_sub(total) {
                    return Err("HistoryBudgetExceeded: recent event line scalars".into());
                }
                total += bytes;
                selected.push(event);
                if selected.len() == limit {
                    break 'commits;
                }
            }
        }
        let lines = selected
            .into_iter()
            .map(|event| harness_contract::mission::MissionControlEventLine {
                event_id: event.event_id.clone(),
                stream_id: event.stream_id.clone(),
                cursor: event.commit_cursor,
                transaction_index: event.transaction_index,
                scope: event.scope.as_str().into(),
                kind: event.kind.clone(),
                status: event.status.clone(),
                actor: event.actor.clone(),
                created_at_ms: event.created_at_ms,
            })
            .collect::<Vec<_>>();
        let actual = lines.iter().try_fold(0usize, |bytes, line| {
            let encoded = serde_json::to_vec(line)
                .map_err(|error| error.to_string())?
                .len();
            bytes
                .checked_add(encoded)
                .ok_or_else(|| "recent line bytes overflow".to_string())
        })?;
        if actual > budget || actual > total {
            return Err("recent line metadata size mismatch".into());
        }
        #[cfg(test)]
        {
            state.recent_clones += lines.len();
        }
        Ok(lines)
    }
    fn original_transaction_receipt(&self, transaction_id: &str)
        -> RuntimeEventStoreResult<Option<AppendTransactionReceipt>> {
        let state = self.state.lock()
            .map_err(|_| RuntimeEventStoreError::Corrupt("event lock poisoned".into()))?;
        Ok(state.transactions.get(transaction_id).cloned())
    }

    fn original_checked_transaction_receipt(&self, transaction_id: &str)
        -> RuntimeEventStoreResult<Option<CheckedAppendReceipt>> {
        let state = self.state.lock().map_err(|_| RuntimeEventStoreError::Corrupt("event lock poisoned".into()))?;
        match (state.transactions.get(transaction_id), state.checked_evidence.get(transaction_id)) {
            (None, None) => Ok(None),
            (Some(commit), Some(validation)) => Ok(Some(CheckedAppendReceipt { commit: commit.clone(), validation: validation.clone() })),
            (Some(_), None) => Err(RuntimeEventStoreError::InvalidTransaction("original transaction is not checked".into())),
            (None, Some(_)) => Err(RuntimeEventStoreError::Corrupt("orphan checked evidence".into())),
        }
    }

    fn transaction_stream_revision_exact(
        &self,
        transaction_id: &str,
        stream_id: &str,
    ) -> RuntimeEventStoreResult<Option<CommittedStreamRevision>> {
        let state = self
            .state
            .lock()
            .map_err(|_| RuntimeEventStoreError::Corrupt("event lock poisoned".into()))?;
        Ok(state
            .transactions
            .get(transaction_id)
            .and_then(|receipt| {
                receipt
                    .stream_revisions
                    .iter()
                    .find(|revision| revision.stream_id == stream_id)
            })
            .cloned())
    }

    fn prepared_content_root(
        &self,
        origin: &crate::PreparedArtifactOrigin,
        artifact: &harness_contract::context::ArtifactRef,
    ) -> RuntimeEventStoreResult<PreparedContentRoot> {
        origin
            .validate()
            .map_err(RuntimeEventStoreError::InvalidTransaction)?;
        let state = self
            .state
            .lock()
            .map_err(|_| RuntimeEventStoreError::Corrupt("event lock poisoned".into()))?;
        let event = |stream: &str, key: &str| {
            state
                .event_keys
                .get(&(stream.into(), key.into()))
                .map(|position| &state.events[*position])
        };
        let root = match origin {
            crate::PreparedArtifactOrigin::EventBody {
                stream_id,
                idempotency_key,
                kind,
            } => {
                if kind == "execution.host.transcript" {
                    let primary = event(stream_id, idempotency_key);
                    let mut node = None;
                    let mut graph = None;
                    if let Some(primary) = primary {
                        if let Ok((graph_id, _, _)) =
                            RuntimeEventStore::host_transcript_root_identity(primary)
                        {
                            let exact = |stream: &str, kind: Option<&str>| {
                                let mut matches = state
                                    .commit_positions
                                    .get(&primary.commit_cursor)
                                    .into_iter()
                                    .flat_map(|positions| positions.iter())
                                    .map(|position| &state.events[*position])
                                    .filter(|candidate| {
                                        candidate.stream_id == stream
                                            && candidate.transaction_id == primary.transaction_id
                                            && candidate.commit_cursor == primary.commit_cursor
                                            && kind.is_none_or(|kind| candidate.kind == kind)
                                    });
                                let first = matches.next();
                                if matches.next().is_some() {
                                    None
                                } else {
                                    first
                                }
                            };
                            node = exact(stream_id, Some("execution_node.transitioned"));
                            graph = exact(&graph_id, None);
                        }
                    }
                    return Ok(RuntimeEventStore::host_transcript_root(
                        primary, node, graph, artifact,
                    ));
                }
                match event(stream_id, idempotency_key) {
                    None => PreparedContentRoot::Absent,
                    Some(event) if &event.kind == kind && event.schema_version == 2 => {
                        let session = if kind == "goal.observation" {
                            event.payload["session_id"].as_str().map(str::to_string)
                        } else {
                            event.activity_binding().map(|binding| binding.session_id)
                        };
                        if session.is_none_or(|session| {
                            artifact.visibility_scope != format!("session:{session}")
                        }) {
                            return Ok(PreparedContentRoot::Blocked);
                        }
                        let slot = if kind == "goal.observation" {
                            if event.scope != RuntimeEventScope::Goal
                                || event.payload["observation_schema"] != 2
                            {
                                return Ok(PreparedContentRoot::Blocked);
                            }
                            &event.payload["observation_body"]
                        } else if event.scope == RuntimeEventScope::ExecutionGraph {
                            &event.payload
                        } else {
                            return Ok(PreparedContentRoot::Blocked);
                        };
                        RuntimeEventStore::prepared_slot_root(slot, artifact)
                    }
                    Some(_) => PreparedContentRoot::Blocked,
                }
            }
            crate::PreparedArtifactOrigin::ToolEffectClosure {
                stream_id,
                idempotency_key,
            } => {
                let primary = event(stream_id, idempotency_key);
                let index = primary
                    .and_then(|primary| {
                        RuntimeEventStore::process_closure_index_identity(
                            &primary.payload,
                            &primary.refs,
                        )
                        .ok()
                    })
                    .and_then(|(stream, key)| event(&stream, &key));
                RuntimeEventStore::process_closure_root(primary, index, artifact)
            }
            crate::PreparedArtifactOrigin::CheckpointBody {
                projection_id,
                source_cursor,
            } => match state.checkpoints.get(projection_id) {
                None => PreparedContentRoot::Absent,
                Some(checkpoint) => {
                    let payload = &checkpoint.payload;
                    let program_schema =
                        RuntimeEventStore::program_read_model_checkpoint_schema(projection_id);
                    let program = program_schema.is_some();
                    let schema = if program {
                        payload["schema_version"].as_u64()
                    } else {
                        payload["schema"].as_u64()
                    };
                    let valid_schema = if let Some(expected) = program_schema {
                        schema == Some(expected)
                    } else if projection_id.starts_with("projection-history:") {
                        matches!(schema, Some(3 | 4 | 5 | 6))
                            && (schema != Some(6) || payload["source_fence"].is_object())
                    } else {
                        schema == Some(2)
                    };
                    if !valid_schema {
                        return Ok(PreparedContentRoot::Blocked);
                    }
                    let scope = if program {
                        payload["session_id"]
                            .as_str()
                            .map(|session| format!("session:{session}"))
                    } else {
                        payload["scope"].as_str().map(str::to_string)
                    };
                    if scope.as_ref() != Some(&artifact.visibility_scope) {
                        return Ok(PreparedContentRoot::Blocked);
                    }
                    match RuntimeEventStore::prepared_slot_root(&payload["body"], artifact) {
                        PreparedContentRoot::Canonical
                            if checkpoint.source_cursor == *source_cursor =>
                        {
                            PreparedContentRoot::Checkpoint
                        }
                        PreparedContentRoot::Canonical => PreparedContentRoot::Blocked,
                        root => root,
                    }
                }
            },
            crate::PreparedArtifactOrigin::TerminalBody {
                terminal_id,
                session_id,
                execution_id,
                turn_id,
                event_stream,
                event_key,
            } => {
                let mut found = false;
                let mut outbox_reference = None;
                if let Some(terminal) = state.terminals.get(terminal_id) {
                    if &terminal.session_id != session_id
                        || terminal.execution_id.as_ref() != Some(execution_id)
                        || terminal.turn_id.as_ref() != Some(turn_id)
                    {
                        return Ok(PreparedContentRoot::Blocked);
                    }
                    match decode_session_terminal_artifact_ref(&terminal.payload_ref) {
                        Ok(reference) => {
                            if reference.visibility_scope != format!("session:{session_id}")
                                || reference.media_type
                                    != "application/vnd.cowd.session-terminal+json"
                                || !reference.is_durable()
                            {
                                return Ok(PreparedContentRoot::Blocked);
                            }
                            found |= &reference == artifact;
                            outbox_reference = Some(reference);
                        }
                        Err(_) => return Ok(PreparedContentRoot::Blocked),
                    }
                }
                if let Some(event) = event(event_stream, event_key) {
                    if event.scope != RuntimeEventScope::SessionInput
                        || event.schema_version != 1
                        || event.kind != "runtime.session.terminal_requested"
                        || event.payload["terminal_id"] != *terminal_id
                        || event.payload["session_id"] != *session_id
                        || event.payload["execution_id"] != *execution_id
                        || event.payload["turn_id"] != *turn_id
                    {
                        return Ok(PreparedContentRoot::Blocked);
                    }
                    match event.payload["payload_ref"]
                        .as_str()
                        .ok_or(())
                        .and_then(|value| {
                            decode_session_terminal_artifact_ref(value).map_err(|_| ())
                        }) {
                        Ok(reference) => {
                            if reference.visibility_scope != format!("session:{session_id}")
                                || reference.media_type
                                    != "application/vnd.cowd.session-terminal+json"
                                || !reference.is_durable()
                                || outbox_reference
                                    .as_ref()
                                    .is_some_and(|outbox| outbox != &reference)
                            {
                                return Ok(PreparedContentRoot::Blocked);
                            }
                            found |= &reference == artifact;
                        }
                        Err(_) => return Ok(PreparedContentRoot::Blocked),
                    }
                }
                if found {
                    PreparedContentRoot::Terminal
                } else {
                    PreparedContentRoot::Absent
                }
            }
        };
        if root == PreparedContentRoot::Absent && state.artifact_roots.contains(&artifact.selector)
        {
            return Ok(PreparedContentRoot::Canonical);
        }
        Ok(root)
    }
    fn position_page(
        &self,
        scope: RuntimeEventScope,
        identity: &str,
        kind: Option<&str>,
        after: Option<(u64, u32)>,
        through_cursor: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        i64::try_from(through_cursor).map_err(|_| "through_cursor overflow".to_string())?;
        self.position_page_classified(scope, identity, kind, after, through_cursor, max_events, max_bytes)
            .map_err(|error| error.to_string())
    }
    fn position_page_classified(
        &self,
        scope: RuntimeEventScope,
        identity: &str,
        kind: Option<&str>,
        after: Option<(u64, u32)>,
        through_cursor: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, PositionPageError> {
        i64::try_from(through_cursor).map_err(|_| "through_cursor overflow".to_string())?;
        let after = after.unwrap_or((0, 0));
        i64::try_from(after.0).map_err(|_| "after_cursor overflow".to_string())?;
        if max_events == 0 || max_bytes == 0 || after.0 > through_cursor {
            return Ok(Vec::new());
        }
        let prefix = match scope {
            RuntimeEventScope::Session => "session",
            RuntimeEventScope::ExecutionGraph => "root",
            RuntimeEventScope::ExecutionNode => "activity",
            _ => return Err(PositionPageError::Other("unsupported position query scope".into())),
        };
        let state = self
            .state
            .lock()
            .map_err(|_| "event store lock poisoned".to_string())?;
        let prefix = kind.map_or_else(|| prefix.to_string(), |kind| format!("{prefix}:{kind}"));
        let Some(index) = state.position_indexes.get(&(prefix, identity.into())) else {
            return Ok(Vec::new());
        };
        let mut selected = Vec::new();
        let mut total = 0usize;
        let budget = max_bytes.min(1024 * 1024);
        for (_, position) in index
            .range((
                std::ops::Bound::Excluded(after),
                std::ops::Bound::Included((through_cursor, u32::MAX)),
            ))
            .take(max_events.min(100))
        {
            let bytes = state.encoded_bytes[*position];
            if bytes > budget.saturating_sub(total) {
                if selected.is_empty() {
                    return Err(PositionPageError::FirstRowExceedsBudget(
                        state.events[*position].event_id.clone(),
                    ));
                }
                break;
            }
            total += bytes;
            selected.push(*position);
        }
        Ok(selected
            .into_iter()
            .map(|position| state.events[position].clone())
            .collect())
    }
    fn durable_commit_cursor(&self) -> RuntimeEventStoreResult<u64> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_cursor)
    }

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
    fn append_checked_transaction(
        &self,
        request: CheckedAppendRequest,
        validator: &dyn CheckedSourceValidator,
    ) -> RuntimeEventStoreResult<CheckedAppendReceipt> {
        let (commit, validation) = self.append_inner_checked(
            request.transaction.clone(),
            None,
            Some((&request, validator)),
        )?;
        Ok(CheckedAppendReceipt {
            commit,
            validation: validation.ok_or_else(|| {
                RuntimeEventStoreError::Corrupt("checked commit missing source evidence".into())
            })?,
        })
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
        i64::try_from(cursor)
            .map_err(|_| RuntimeEventStoreError::InvalidTransaction("cursor overflow".into()))?;
        if max_commits == 0 {
            return Ok(Vec::new());
        }
        let state = self.state.lock().unwrap();
        let mut selected = Vec::new();
        let mut total = 0usize;
        for (commit_cursor, positions) in state
            .commit_positions
            .range((
                std::ops::Bound::Excluded(cursor),
                std::ops::Bound::Unbounded,
            ))
            .take(max_commits.min(100))
        {
            let bytes = positions.iter().try_fold(0usize, |sum, position| {
                sum.checked_add(state.encoded_bytes[*position])
                    .ok_or_else(|| {
                        RuntimeEventStoreError::Corrupt(
                            "transaction metadata bytes overflow".into(),
                        )
                    })
            })?;
            if bytes > (1024usize * 1024).saturating_sub(total) {
                if selected.is_empty() {
                    return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
                        "committed transaction".into(),
                    ));
                }
                break;
            }
            total += bytes;
            selected.push((*commit_cursor, positions));
        }
        Ok(selected
            .into_iter()
            .map(|(commit_cursor, positions)| {
                let events = positions
                    .iter()
                    .map(|position| state.events[*position].clone())
                    .collect::<Vec<_>>();
                CommittedEventBatch {
                    transaction_id: events[0].transaction_id.clone(),
                    commit_cursor,
                    events,
                }
            })
            .collect())
    }
    fn projection_checkpoint(
        &self,
        id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeProjectionCheckpoint>> {
        Ok(self.state.lock().unwrap().checkpoints.get(id).cloned())
    }
    fn projection_scan_page(
        &self,
        cursor: u64,
        interest: &RuntimeProjectionInterest,
        max_commits: usize,
        max_events: usize,
        max_bytes: usize,
    ) -> RuntimeEventStoreResult<RuntimeProjectionScanPage> {
        i64::try_from(cursor)
            .map_err(|_| RuntimeEventStoreError::InvalidTransaction("cursor overflow".into()))?;
        let state = self.state.lock().unwrap();
        let mut page = RuntimeProjectionScanPage {
            scanned_through_cursor: cursor,
            ..RuntimeProjectionScanPage::default()
        };
        let mut bytes = 0usize;
        for (position, positions) in state
            .commit_positions
            .range((
                std::ops::Bound::Excluded(cursor),
                std::ops::Bound::Unbounded,
            ))
            .take(max_commits.min(100))
        {
            let selected = positions
                .iter()
                .copied()
                .filter(|index| interest.matches(&state.events[*index]))
                .collect::<Vec<_>>();
            let additional_bytes = selected.iter().try_fold(0usize, |sum, index| {
                sum.checked_add(state.encoded_bytes[*index]).ok_or_else(|| {
                    RuntimeEventStoreError::Corrupt("projection metadata bytes overflow".into())
                })
            })?;
            if selected.len() > max_events.min(100).saturating_sub(page.matched_events)
                || additional_bytes > max_bytes.min(1024 * 1024).saturating_sub(bytes)
            {
                if page.matched_events == 0 {
                    return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
                        "projection transaction".into(),
                    ));
                }
                break;
            }
            page.scanned_through_cursor = *position;
            page.scanned_commits += 1;
            page.matched_events += selected.len();
            bytes += additional_bytes;
            if !selected.is_empty() {
                let events = selected
                    .into_iter()
                    .map(|index| state.events[index].clone())
                    .collect::<Vec<_>>();
                page.batches.push(CommittedEventBatch {
                    commit_cursor: *position,
                    transaction_id: events[0].transaction_id.clone(),
                    events,
                });
            }
        }
        Ok(page)
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
    fn source_mutation_fence(&self) -> RuntimeEventStoreResult<RuntimeSourceMutationFence> {
        let mut state = self.state.lock().map_err(|_| RuntimeEventStoreError::Corrupt("event store lock poisoned".into()))?;
        Ok(state.source_mutation_fence())
    }

    fn compare_and_put_projection_checkpoint_fenced(
        &self,
        id: &str,
        cursor: u64,
        expected: u64,
        payload: &serde_json::Value,
        at: u64,
        source_fence: &RuntimeSourceMutationFence,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        let mut state = self.state.lock().map_err(|_| RuntimeEventStoreError::Corrupt("event store lock poisoned".into()))?;
        if state.source_mutation_fence() != *source_fence {
            return Err(RuntimeEventStoreError::Corrupt("source mutation fence changed".into()));
        }
        put_source_checkpoint(&mut state, id, cursor, expected, payload, at)
    }

    fn compare_and_put_projection_checkpoint(
        &self,
        id: &str,
        cursor: u64,
        expected: u64,
        payload: &serde_json::Value,
        at: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        let mut state = self.state.lock().map_err(|_| RuntimeEventStoreError::Corrupt("event store lock poisoned".into()))?;
        put_source_checkpoint(&mut state, id, cursor, expected, payload, at)
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
            .matching_limited(
                |event| event.stream_id == stream && event.idempotency_key.as_deref() == Some(key),
                1,
                true,
            )
            .into_iter()
            .next())
    }
    fn stream_revision(&self, stream: &str) -> RuntimeEventStoreResult<u64> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(state
            .stream_positions
            .get(stream)
            .and_then(|positions| positions.last_key_value())
            .map_or(0, |(sequence, _)| *sequence))
    }
    fn list_stream(&self, stream: &str) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self.matching(|e| e.stream_id == stream))
    }
    fn list_stream_after(
        &self,
        stream: &str,
        after_sequence: u64,
        through_sequence: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.sequence_page(
            stream,
            after_sequence,
            through_sequence,
            max_events,
            max_bytes,
            false,
        )
    }
    fn list_stream_before(
        &self,
        stream: &str,
        through_sequence: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.sequence_page(stream, 0, through_sequence, max_events, max_bytes, true)
    }
    fn count_stream_kinds(
        &self,
        stream: &str,
        kinds: &[&str],
        through_sequence: u64,
    ) -> Result<u64, String> {
        i64::try_from(through_sequence).map_err(|_| "through_sequence overflow".to_string())?;
        let state = self
            .state
            .lock()
            .map_err(|_| "event store lock poisoned".to_string())?;
        let Some(positions) = state.stream_positions.get(stream) else {
            return Ok(0);
        };
        let count = positions
            .range(..=through_sequence)
            .filter(|(_, position)| kinds.contains(&state.events[**position].kind.as_str()))
            .count();
        u64::try_from(count).map_err(|_| "event count overflow".to_string())
    }
    fn list_stream_page_desc(
        &self,
        stream_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let offset = u64::try_from(offset).map_err(|_| "history offset overflow".to_string())?;
        i64::try_from(offset).map_err(|_| "history offset exceeds current schema".to_string())?;
        let head = self
            .stream_revision(stream_id)
            .map_err(|error| error.to_string())?;
        let through = head.saturating_sub(offset);
        self.sequence_page(stream_id, 0, through, limit, 1024 * 1024, true)
    }
    fn stream_event_count(&self, stream: &str) -> Result<usize, String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "event store lock poisoned".to_string())?;
        Ok(state.stream_positions.get(stream).map_or(0, BTreeMap::len))
    }
    fn execution_events_for_session(
        &self,
        id: &str,
        pos: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self.matching_limited(
            |event| {
                after(event, pos)
                    && event
                        .refs
                        .iter()
                        .any(|reference| reference.kind == "session" && reference.id == id)
            },
            limit,
            false,
        ))
    }
    fn events_for_root_execution(
        &self,
        id: &str,
        pos: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self.matching_limited(
            |event| {
                after(event, pos)
                    && event
                        .activity_binding()
                        .is_some_and(|binding| binding.root_execution_id == id)
            },
            limit,
            false,
        ))
    }
    fn events_for_root_execution_kind(
        &self,
        id: &str,
        kind: &str,
        pos: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self.matching_limited(
            |event| {
                event.kind == kind
                    && after(event, pos)
                    && event
                        .activity_binding()
                        .is_some_and(|binding| binding.root_execution_id == id)
            },
            limit,
            false,
        ))
    }
    fn events_for_activity(
        &self,
        id: &str,
        pos: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self.matching_limited(
            |event| {
                after(event, pos)
                    && event
                        .activity_binding()
                        .is_some_and(|binding| binding.activity_id == id)
            },
            limit,
            false,
        ))
    }
    fn list_scope(
        &self,
        scope: RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self.matching_limited(|event| event.scope == scope, limit, true))
    }
    fn list_scope_page_asc(
        &self,
        scope: RuntimeEventScope,
        pos: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self.matching_limited(
            |event| event.scope == scope && after(event, pos),
            limit,
            false,
        ))
    }
    fn list_scope_stream_prefix_page_asc(
        &self,
        scope: RuntimeEventScope,
        prefix: &str,
        pos: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self.matching_limited(
            |event| {
                event.scope == scope && event.stream_id.starts_with(prefix) && after(event, pos)
            },
            limit,
            false,
        ))
    }
    fn list_scope_kind_page_asc(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        pos: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self.matching_limited(
            |event| event.scope == scope && event.kind == kind && after(event, pos),
            limit,
            false,
        ))
    }
    fn stream_ids_for_scope(
        &self,
        scope: RuntimeEventScope,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(state
            .events
            .iter()
            .filter(|event| event.scope == scope)
            .map(|event| &event.stream_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .cloned()
            .collect())
    }
    fn stream_ids_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(state
            .events
            .iter()
            .filter(|event| {
                event.scope == scope && event.kind == kind && event.sequence == sequence
            })
            .map(|event| event.stream_id.clone())
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
        let mut streams = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .events
            .iter()
            .filter(|event| {
                event.scope == scope && event.kind == kind && event.sequence == sequence
            })
            .map(|event| (event.stream_id.clone(), event.commit_cursor))
            .collect::<Vec<_>>();
        streams.sort_by(|left, right| (left.1, &left.0).cmp(&(right.1, &right.0)));
        Ok(streams
            .into_iter()
            .filter(|(identity, cursor)| {
                after_key
                    .as_ref()
                    .is_none_or(|(after_cursor, after_identity)| {
                        (*cursor, identity.as_str()) > (*after_cursor, after_identity.as_str())
                    })
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
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(ids
            .into_iter()
            .map(|id| {
                let status = state
                    .events
                    .iter()
                    .rev()
                    .find(|event| event.stream_id == id)
                    .and_then(|event| event.status.clone());
                (id, status)
            })
            .collect())
    }
    fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
        Ok(self.matching_limited(|_| true, limit, true))
    }
    fn latest_for_stream(&self, stream: &str) -> Result<Option<DurableRuntimeEvent>, String> {
        Ok(self
            .matching_limited(|event| event.stream_id == stream, 1, true)
            .pop())
    }
    fn latest_for_stream_kind(
        &self,
        stream: &str,
        kind: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        Ok(self
            .matching_limited(
                |event| event.stream_id == stream && event.kind == kind,
                1,
                true,
            )
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
        s.insert_terminal(v.clone());
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
    fn session_terminal_for_execution(
        &self,
        session_id: &str,
        execution_id: &str,
        turn_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
        RuntimeEventStore::validate_terminal_execution_selector(session_id, execution_id, turn_id)?;
        let state = self
            .state
            .lock()
            .map_err(|_| RuntimeEventStoreError::Corrupt("terminal store lock poisoned".into()))?;
        let key = (
            session_id.to_string(),
            execution_id.to_string(),
            turn_id.to_string(),
        );
        let Some(identities) = state.terminal_sources.get(&key) else {
            return Ok(None);
        };
        let mut matches = identities.iter().take(2);
        let identity = matches
            .next()
            .ok_or_else(|| RuntimeEventStoreError::Corrupt("empty terminal source index".into()))?;
        if matches.next().is_some() {
            return Err(RuntimeEventStoreError::Corrupt(
                "ambiguous Session terminal source tuple".into(),
            ));
        }
        let record = state.terminals.get(identity).ok_or_else(|| {
            RuntimeEventStoreError::Corrupt("missing indexed Session terminal".into())
        })?;
        if EphemeralState::terminal_source(record).as_ref() != Some(&key) {
            return Err(RuntimeEventStoreError::Corrupt(
                "Session terminal source index mismatch".into(),
            ));
        }
        Ok(Some(record.clone()))
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
            .get(id)
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
        let mut updated = v.clone();
        change(&mut updated);
        updated.revision += 1;
        s.insert_terminal(updated.clone());
        Ok(updated)
    }
}

#[cfg(test)]
mod bounded_query_463_tests {
    use super::*;
    use std::cell::Cell;

    fn backend_with_events() -> EphemeralRuntimeEventStore {
        let backend = EphemeralRuntimeEventStore::new();
        let mut events = Vec::new();
        let mut expected_streams = Vec::new();
        for stream in ["463:z", "unrelated:x", "463:a", "463:b"] {
            let foreign = stream == "unrelated:x";
            let session = if foreign {
                "foreign-session"
            } else {
                "session-463"
            };
            let graph = if foreign {
                "foreign-graph"
            } else {
                "graph-463"
            };
            let binding = serde_json::from_value(serde_json::json!({
                "root_execution_id": graph, "session_id": session, "turn_id": "turn-463",
                "root_task_id": "task-463", "task_id": "task-463", "node_id": "node",
                "activity_id": format!("activity:execution:{graph}:node:node"),
                "revision": 1, "fence": 1, "generation": 1,
            }))
            .unwrap();
            let event = RuntimeEventInput {
                stream_id: stream.into(),
                scope: RuntimeEventScope::ExecutionNode,
                kind: "bounded.read.test".into(),
                status: Some("first".into()),
                actor: Some("test".into()),
                refs: vec![RuntimeEventRef {
                    kind: "session".into(),
                    id: session.into(),
                }],
                payload: serde_json::json!({"padding": "x".repeat(65536)}),
            }
            .with_activity_binding(binding)
            .unwrap();
            let mut input: RuntimeTransactionEventInput = event.into();
            input.idempotency_key = Some(format!("read463:{stream}"));
            events.push(input);
            expected_streams.push(ExpectedStreamRevision {
                stream_id: stream.into(),
                expected_revision: 0,
            });
        }
        backend
            .append_transaction(AppendTransactionRequest {
                transaction_id: "bounded-463".into(),
                expected_streams,
                events,
            })
            .unwrap();
        backend
    }

    #[test]
    fn r1_463_limited_matching_stops_before_visiting_or_cloning_tail() {
        let backend = backend_with_events();
        let visits = Cell::new(0);
        let first = backend.matching_limited(
            |_| {
                visits.set(visits.get() + 1);
                true
            },
            1,
            false,
        );
        assert_eq!(visits.get(), 1);
        assert_eq!(first[0].stream_id, "463:z");
        visits.set(0);
        let last = backend.matching_limited(
            |_| {
                visits.set(visits.get() + 1);
                true
            },
            1,
            true,
        );
        assert_eq!(visits.get(), 1);
        assert_eq!(last[0].stream_id, "463:b");
        visits.set(0);
        assert!(backend
            .matching_limited(
                |_| {
                    visits.set(visits.get() + 1);
                    true
                },
                0,
                false
            )
            .is_empty());
        assert_eq!(visits.get(), 0);
    }

    #[test]
    fn r1_463_prefix_and_reference_pages_preserve_same_cursor_position_and_scope() {
        let backend = backend_with_events();
        let mut position = None;
        let mut streams = Vec::new();
        for _ in 0..4 {
            let page = backend
                .list_scope_stream_prefix_page_asc(
                    RuntimeEventScope::ExecutionNode,
                    "463:",
                    position,
                    1,
                )
                .unwrap();
            if page.is_empty() {
                break;
            }
            assert_eq!(page.len(), 1);
            let event = &page[0];
            assert!(position
                .is_none_or(|previous| (event.commit_cursor, event.transaction_index) > previous));
            position = Some((event.commit_cursor, event.transaction_index));
            streams.push(event.stream_id.clone());
        }
        assert_eq!(streams, ["463:z", "463:a", "463:b"]);
        assert!(backend
            .list_scope_stream_prefix_page_asc(RuntimeEventScope::Recovery, "463:", None, 1,)
            .unwrap()
            .is_empty());
        assert!(backend
            .list_scope_stream_prefix_page_asc(RuntimeEventScope::ExecutionNode, "463:", None, 0,)
            .unwrap()
            .is_empty());
        assert_eq!(
            backend
                .list_scope_page_asc(RuntimeEventScope::ExecutionNode, None, 1)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            backend
                .list_scope_kind_page_asc(
                    RuntimeEventScope::ExecutionNode,
                    "bounded.read.test",
                    None,
                    1,
                )
                .unwrap()
                .len(),
            1
        );
        let session = backend
            .execution_events_for_session("session-463", None, 20)
            .unwrap();
        let root = backend
            .events_for_root_execution("graph-463", None, 20)
            .unwrap();
        let kind = backend
            .events_for_root_execution_kind("graph-463", "bounded.read.test", None, 20)
            .unwrap();
        let activity = backend
            .events_for_activity("activity:execution:graph-463:node:node", None, 20)
            .unwrap();
        assert_eq!(session, root);
        assert_eq!(root, kind);
        assert_eq!(kind, activity);
        assert_eq!(activity.len(), 3);
    }

    #[test]
    fn r1_463_metadata_pages_keep_lexical_ties_and_latest_reads_keep_tail() {
        let backend = backend_with_events();
        let mut after_key = None;
        let mut streams = Vec::new();
        for _ in 0..5 {
            let page = backend
                .stream_ids_for_scope_kind_at_sequence_page(
                    RuntimeEventScope::ExecutionNode,
                    "bounded.read.test",
                    1,
                    after_key.clone(),
                    1,
                )
                .unwrap();
            if page.is_empty() {
                break;
            }
            assert_eq!(page.len(), 1);
            let (identity, cursor) = &page[0];
            after_key = Some((*cursor, identity.clone()));
            streams.push(identity.clone());
        }
        assert_eq!(streams, ["463:a", "463:b", "463:z", "unrelated:x"]);
        assert_eq!(
            backend
                .stream_ids_for_scope(RuntimeEventScope::ExecutionNode)
                .unwrap(),
            streams
        );
        backend
            .append_transaction(AppendTransactionRequest {
                transaction_id: "bounded-463-latest".into(),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: "463:z".into(),
                    expected_revision: 1,
                }],
                events: vec![RuntimeEventInput {
                    stream_id: "463:z".into(),
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "bounded.read.test".into(),
                    status: Some("latest".into()),
                    actor: Some("test".into()),
                    refs: Vec::new(),
                    payload: serde_json::json!({}),
                }
                .into()],
            })
            .unwrap();
        assert_eq!(
            backend
                .latest_for_stream("463:z")
                .unwrap()
                .unwrap()
                .status
                .as_deref(),
            Some("latest")
        );
        assert_eq!(
            backend
                .latest_for_stream_kind("463:z", "bounded.read.test")
                .unwrap()
                .unwrap()
                .sequence,
            2
        );
        assert_eq!(
            backend
                .list_scope(RuntimeEventScope::ExecutionNode, 1)
                .unwrap()[0]
                .sequence,
            2
        );
        assert_eq!(backend.all_events(1).unwrap()[0].sequence, 2);
        assert_eq!(
            backend
                .event_by_idempotency_key("463:z", "read463:463:z")
                .unwrap()
                .unwrap()
                .sequence,
            1
        );
        assert_eq!(
            backend
                .event_by_idempotency_key("463:z", "bounded-463:0")
                .unwrap(),
            None
        );
    }

    #[test]
    fn r1_464_latest_status_metadata_preserves_discovery_and_latest_none() {
        let backend = backend_with_events();
        let initial = backend
            .latest_stream_statuses_for_scope_kind_at_sequence(
                RuntimeEventScope::ExecutionNode,
                "bounded.read.test",
                1,
            )
            .unwrap();
        assert_eq!(
            initial,
            ["463:a", "463:b", "463:z", "unrelated:x"]
                .map(|stream| (stream.to_string(), Some("first".into())))
        );
        for (stream, status) in [("463:a", Some("updated")), ("463:z", None)] {
            backend
                .append_transaction(AppendTransactionRequest {
                    transaction_id: format!("status-464:{stream}"),
                    expected_streams: vec![ExpectedStreamRevision {
                        stream_id: stream.into(),
                        expected_revision: 1,
                    }],
                    events: vec![RuntimeEventInput {
                        stream_id: stream.into(),
                        scope: RuntimeEventScope::ExecutionNode,
                        kind: "other.status.kind".into(),
                        status: status.map(str::to_owned),
                        actor: Some("test".into()),
                        refs: Vec::new(),
                        payload: serde_json::json!({"padding": "x".repeat(65536)}),
                    }
                    .into()],
                })
                .unwrap();
        }
        assert_eq!(
            backend
                .latest_stream_statuses_for_scope_kind_at_sequence(
                    RuntimeEventScope::ExecutionNode,
                    "bounded.read.test",
                    1,
                )
                .unwrap(),
            vec![
                ("463:a".into(), Some("updated".into())),
                ("463:b".into(), Some("first".into())),
                ("463:z".into(), None),
                ("unrelated:x".into(), Some("first".into())),
            ]
        );
        for (scope, kind, sequence) in [
            (RuntimeEventScope::Recovery, "bounded.read.test", 1),
            (RuntimeEventScope::ExecutionNode, "missing.kind", 1),
            (RuntimeEventScope::ExecutionNode, "bounded.read.test", 2),
        ] {
            assert!(backend
                .latest_stream_statuses_for_scope_kind_at_sequence(scope, kind, sequence,)
                .unwrap()
                .is_empty());
        }
    }
}

fn put_source_checkpoint(
    state: &mut EphemeralState,
    id: &str,
    cursor: u64,
    expected: u64,
    payload: &serde_json::Value,
    at: u64,
) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        let actual = state.checkpoints.get(id).map_or(0, |v| v.revision);
        if actual != expected {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: id.into(),
                expected,
                actual,
            });
        }
        if let Some(current) = state.checkpoints.get(id) {
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
        state.checkpoints.insert(id.into(), value.clone());
        Ok(value)
    }

#[cfg(test)]
mod source_fence_tests {
    use super::*;

    #[test]
    fn own858_source_fence_survives_append_and_rejects_foreign_and_stale_cas() {
        let backend = EphemeralRuntimeEventStore::new();
        let source = backend.source_mutation_fence().unwrap();
        backend.append(RuntimeEventInput {
            stream_id: "858-unrelated".into(), scope: RuntimeEventScope::Session,
            kind: "858.append".into(), status: None, actor: None,
            refs: Vec::new(), payload: serde_json::json!({}),
        }).unwrap();
        assert_eq!(backend.source_mutation_fence().unwrap(), source);
        let payload = serde_json::json!({"verified":true});
        let saved = backend.compare_and_put_projection_checkpoint_fenced("858-checkpoint", 1, 0, &payload, 0, &source).unwrap();
        assert_eq!(saved.revision, 1);
        assert!(matches!(backend.compare_and_put_projection_checkpoint_fenced("858-checkpoint", 1, 0, &payload, 0, &source), Err(RuntimeEventStoreError::StaleRevision { .. })));
        let foreign = EphemeralRuntimeEventStore::new().source_mutation_fence().unwrap();
        assert_ne!(source.incarnation, foreign.incarnation);
        assert!(backend.compare_and_put_projection_checkpoint_fenced("858-foreign", 1, 0, &payload, 0, &foreign).is_err());
        {
            let mut state = backend.state.lock().unwrap();
            state.events[0].actor = Some("changed".into());
            state.source_fence.as_mut().unwrap().mutation_epoch += 1;
        }
        assert!(backend.compare_and_put_projection_checkpoint_fenced("858-checkpoint", 1, 1, &payload, 0, &source).is_err());
        assert_eq!(backend.projection_checkpoint("858-checkpoint").unwrap(), Some(saved));
        let current = backend.source_mutation_fence().unwrap();
        assert_eq!(current.mutation_epoch, source.mutation_epoch + 1);
        backend.compare_and_put_projection_checkpoint_fenced("858-checkpoint", 1, 1, &payload, 0, &current).unwrap();
    }
}

#[cfg(test)]
mod checked_source_tests {
    use super::*;

    struct Neutral;
    impl CheckedSourceValidator for Neutral {
        fn frame_digest(&self) -> &str {
            "independent-frame"
        }
        fn validate_tail(
            &self,
            source: &ExpectedStreamRevision,
            actual: u64,
            events: &[DurableRuntimeEvent],
        ) -> RuntimeEventStoreResult<()> {
            if events.iter().all(|event| event.kind == "neutral") {
                Ok(())
            } else {
                Err(RuntimeEventStoreError::StaleRevision {
                    stream_id: source.stream_id.clone(),
                    expected: source.expected_revision,
                    actual,
                })
            }
        }
    }

    fn command(id: &str, head: u64, kind: &str) -> AppendTransactionRequest {
        AppendTransactionRequest {
            transaction_id: id.into(),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: "source".into(),
                expected_revision: head,
            }],
            events: vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: "source".into(),
                    scope: RuntimeEventScope::Program,
                    kind: kind.into(),
                    status: None,
                    actor: None,
                    refs: vec![],
                    payload: serde_json::json!({}),
                },
                idempotency_key: Some(id.into()),
                schema_version: 1,
            }],
        }
    }

    #[test]
    fn n2_checked_append_keeps_original_identity_and_strict_cas() {
        let store = EphemeralRuntimeEventStore::new();
        store
            .append_transaction(command("neutral-1", 0, "neutral"))
            .unwrap();
        let original = CheckedAppendRequest {
            transaction: command("conclusion", 0, "conclusion"),
            frame_digest: "independent-frame".into(),
            budget: SourceCheckBudget::default(),
        };
        assert!(matches!(
            store.append_transaction(original.transaction.clone()),
            Err(RuntimeEventStoreError::StaleRevision { .. })
        ));
        let receipt = store
            .append_checked_transaction(original.clone(), &Neutral)
            .unwrap();
        assert_eq!(receipt.validation.sources[0].expected_revision, 0);
        assert_eq!(receipt.validation.sources[0].committed_revision, 1);
        assert_eq!(receipt.validation.events_read, 1);
        assert_eq!(receipt.commit.stream_revisions[0].committed_revision, 2);
        store
            .append_transaction(command("neutral-2", 2, "neutral"))
            .unwrap();
        let duplicate = store
            .append_checked_transaction(original.clone(), &Neutral)
            .unwrap();
        assert!(duplicate.commit.duplicate);
        assert_eq!(duplicate.commit.commit_cursor, receipt.commit.commit_cursor);
        assert_eq!(duplicate.validation, receipt.validation);
        let mut changed = original;
        changed.transaction.events[0].event.payload = serde_json::json!({"changed": true});
        assert!(matches!(
            store.append_checked_transaction(changed, &Neutral),
            Err(RuntimeEventStoreError::TransactionConflict { .. })
        ));
        assert_eq!(store.stream_revision("source").unwrap(), 3);
    }

    #[test]
    fn n2_checked_append_rejects_related_tail_and_budget_without_mutation() {
        let store = EphemeralRuntimeEventStore::new();
        store
            .append_transaction(command("related", 0, "related"))
            .unwrap();
        let request = CheckedAppendRequest {
            transaction: command("conclusion", 0, "conclusion"),
            frame_digest: "independent-frame".into(),
            budget: SourceCheckBudget::default(),
        };
        assert!(matches!(
            store.append_checked_transaction(request, &Neutral),
            Err(RuntimeEventStoreError::StaleRevision { .. })
        ));
        assert_eq!(store.stream_revision("source").unwrap(), 1);
        let store = EphemeralRuntimeEventStore::new();
        store
            .append_transaction(command("neutral-1", 0, "neutral"))
            .unwrap();
        store
            .append_transaction(command("neutral-2", 1, "neutral"))
            .unwrap();
        let request = CheckedAppendRequest {
            transaction: command("conclusion", 0, "conclusion"),
            frame_digest: "independent-frame".into(),
            budget: SourceCheckBudget {
                max_events: 1,
                ..Default::default()
            },
        };
        assert!(matches!(
            store.append_checked_transaction(request, &Neutral),
            Err(RuntimeEventStoreError::HistoryBudgetExceeded(_))
        ));
        assert_eq!(store.stream_revision("source").unwrap(), 2);
    }
}
