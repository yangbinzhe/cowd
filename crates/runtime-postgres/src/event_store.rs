//! PostgreSQL Runtime event-store adapter and migration pipeline.

use super::*;

pub(crate) const RECENT_EVENT_LINE_METADATA_SQL: &str =
    "SELECT commit_cursor,transaction_index,
        384::BIGINT + 6::BIGINT * (octet_length(event_id)::BIGINT + octet_length(stream_id)::BIGINT
        + octet_length(scope)::BIGINT + octet_length(kind)::BIGINT
        + COALESCE(octet_length(status),0)::BIGINT + COALESCE(octet_length(actor),0)::BIGINT)
     FROM runtime_events WHERE commit_cursor<=$3 AND
        (stream_id=$1 OR EXISTS (SELECT 1 FROM jsonb_array_elements($2::JSONB) AS selected(reference)
            WHERE refs @> jsonb_build_array(selected.reference)))
     ORDER BY commit_cursor DESC,transaction_index DESC LIMIT $4";

/// Complete PostgreSQL implementation of the Runtime event backend contract.
#[derive(Clone, Debug)]
pub struct PostgresRuntimeEventStore {
    executor: PostgresExecutor,
    #[cfg(test)]
    recent_read_counts: Arc<std::sync::Mutex<(usize, usize, usize)>>,
    #[cfg(test)]
    sequence_read_counts: Arc<std::sync::Mutex<(usize, usize)>>,
    #[cfg(test)]
    recent_interleave: Arc<std::sync::Mutex<Option<RuntimeEventInput>>>,
}

struct PgOwnedCommitGuard<'a>(&'a dyn runtime::RuntimeTransactionCommitControl, bool);
impl Drop for PgOwnedCommitGuard<'_> {
    fn drop(&mut self) {
        if !self.1 { self.0.finish(runtime::RuntimeTransactionOutcome::Unknown); }
    }
}

impl PostgresRuntimeEventStore {
    fn append_owned_cancellable_inner(
        &self, request: AppendTransactionRequest, control: &dyn runtime::RuntimeTransactionCommitControl,
    ) -> RuntimeEventStoreResult<Option<AppendTransactionReceipt>> {
        if !control.begin().map_err(RuntimeEventStoreError::InvalidTransaction)? { return Ok(None); }
        let mut guard = PgOwnedCommitGuard(control, false);
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let receipt = append_transaction_in_tx(&mut tx, &request, None)?;
        control.before_commit();
        if !control.commit_wins().map_err(RuntimeEventStoreError::InvalidTransaction)? {
            pg(tx.rollback())?;
            control.finish(runtime::RuntimeTransactionOutcome::RolledBack);
            guard.1 = true;
            return Ok(None);
        }
        pg(tx.commit())?;
        control.finish(runtime::RuntimeTransactionOutcome::Committed);
        guard.1 = true;
        Ok(Some(receipt))
    }
    fn put_source_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &Value,
        updated_at_ms: u64,
        source_fence: Option<&RuntimeSourceMutationFence>,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        validate_projection_id(projection_id)?;
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        pg(tx.batch_execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))?;
        if let Some(expected) = source_fence {
            pg(tx.query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || ':cowd-runtime-source-mutation', 0))",
                &[],
            ))?;
            let row = pg(tx.query_one("SELECT incarnation,mutation_epoch FROM runtime_event_source_fence WHERE singleton=TRUE", &[]))?;
            let current = RuntimeSourceMutationFence {
                incarnation: pg(row.try_get(0))?,
                mutation_epoch: from_i64(pg(row.try_get(1))?, "source mutation epoch")?,
            };
            if current != *expected {
                return Err(RuntimeEventStoreError::Corrupt("source mutation fence changed".into()));
            }
        }
        let lock_key = format!("cowd-runtime-projection:{projection_id}");
        pg(tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&lock_key],
        ))?;
        let current = pg(tx.query_opt(
            "SELECT projection_id, source_cursor, revision, payload, updated_at_ms
               FROM runtime_projection_checkpoints
              WHERE projection_id=$1 FOR UPDATE",
            &[&projection_id],
        ))?
        .map(|row| row_to_projection_checkpoint(&row))
        .transpose()?;
        let checkpoint = match current {
            Some(current) => {
                if current.revision != expected_revision {
                    return Err(RuntimeEventStoreError::StaleRevision {
                        stream_id: format!("projection:{projection_id}"),
                        expected: expected_revision,
                        actual: current.revision,
                    });
                }
                if source_cursor < current.source_cursor {
                    return Err(RuntimeEventStoreError::StaleRevision {
                        stream_id: format!("projection-source:{projection_id}"),
                        expected: source_cursor,
                        actual: current.source_cursor,
                    });
                }
                if source_cursor == current.source_cursor && current.payload == *payload {
                    return Ok(current);
                }
                let row = pg(tx.query_one(
                    "UPDATE runtime_projection_checkpoints
                        SET source_cursor=$1, revision=revision+1, payload=$2, updated_at_ms=$3
                      WHERE projection_id=$4 AND revision=$5
                  RETURNING projection_id, source_cursor, revision, payload, updated_at_ms",
                    &[
                        &to_i64(source_cursor, "source_cursor")?,
                        payload,
                        &to_i64(updated_at_ms, "updated_at_ms")?,
                        &projection_id,
                        &to_i64(expected_revision, "expected_revision")?,
                    ],
                ))?;
                row_to_projection_checkpoint(&row)?
            }
            None => {
                if expected_revision != 0 {
                    return Err(RuntimeEventStoreError::StaleRevision {
                        stream_id: format!("projection:{projection_id}"),
                        expected: expected_revision,
                        actual: 0,
                    });
                }
                let row = pg(tx.query_one(
                    "INSERT INTO runtime_projection_checkpoints
                        (projection_id, source_cursor, revision, payload, updated_at_ms)
                     VALUES ($1,$2,1,$3,$4)
                     RETURNING projection_id, source_cursor, revision, payload, updated_at_ms",
                    &[
                        &projection_id,
                        &to_i64(source_cursor, "source_cursor")?,
                        payload,
                        &to_i64(updated_at_ms, "updated_at_ms")?,
                    ],
                ))?;
                row_to_projection_checkpoint(&row)?
            }
        };
        pg(tx.commit())?;
        Ok(checkpoint)
    }

    #[cfg(test)]
    pub(crate) fn take_recent_read_counts(&self) -> (usize, usize, usize) {
        std::mem::take(&mut *self.recent_read_counts.lock().unwrap())
    }
    #[cfg(test)]
    pub(crate) fn take_sequence_read_counts(&self) -> (usize, usize) {
        std::mem::take(&mut *self.sequence_read_counts.lock().unwrap())
    }
    #[cfg(test)]
    pub(crate) fn interleave_recent_append(&self, input: RuntimeEventInput) {
        *self.recent_interleave.lock().unwrap() = Some(input);
    }
    #[cfg(test)]
    pub(crate) fn recent_interleave_pending(&self) -> bool {
        self.recent_interleave.lock().unwrap().is_some()
    }
    fn sequence_page(
        &self,
        stream_id: &str,
        after_sequence: u64,
        through_sequence: u64,
        max_events: usize,
        max_bytes: usize,
        descending: bool,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        // An unbounded head-only cursor is a legitimate request: clamp it to
        // the representable range instead of failing, so the read is not blind
        // exactly when a diagnostic needs it.
        let after_sequence =
            to_i64(after_sequence.min(i64::MAX as u64), "after_sequence")
                .map_err(|error| error.to_string())?;
        let through_sequence =
            to_i64(through_sequence.min(i64::MAX as u64), "through_sequence")
                .map_err(|error| error.to_string())?;
        if max_events == 0 || max_bytes == 0 || through_sequence <= after_sequence {
            return Ok(Vec::new());
        }
        let limit =
            i64::try_from(max_events.min(100)).map_err(|_| "page limit overflow".to_string())?;
        let order = if descending { "DESC" } else { "ASC" };
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let rows = connection.query(
            &format!("SELECT event_id, encoded_bytes FROM runtime_events WHERE stream_id=$1 AND sequence>$2 AND sequence<=$3 ORDER BY sequence {order} LIMIT $4"),
            &[&stream_id, &after_sequence, &through_sequence, &limit],
        ).map_err(|_| "runtime event metadata read failed".to_string())?;
        #[cfg(test)]
        {
            self.sequence_read_counts.lock().unwrap().0 += rows.len();
        }
        let mut selected: Vec<String> = Vec::new();
        let mut total = 0usize;
        let budget = max_bytes.min(1024 * 1024);
        for row in rows {
            let identity: String = row
                .try_get(0)
                .map_err(|_| "event identity is corrupt".to_string())?;
            let encoded: i64 = row
                .try_get(1)
                .map_err(|_| "event encoded bytes are missing".to_string())?;
            let bytes = usize::try_from(encoded)
                .ok()
                .filter(|bytes| *bytes > 0)
                .ok_or_else(|| "event encoded bytes are corrupt".to_string())?;
            if bytes > budget.saturating_sub(total) {
                if selected.is_empty() {
                    return Err(RuntimeEventStoreError::HistoryBudgetExceeded(identity).to_string());
                }
                break;
            }
            total += bytes;
            selected.push(identity);
        }
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        let events = pg(connection.query(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE event_id=ANY($1) ORDER BY sequence {order}"), &[&selected],
        )).and_then(rows_to_events).map_err(|error| error.to_string())?;
        #[cfg(test)]
        {
            self.sequence_read_counts.lock().unwrap().1 += events.len();
        }
        let actual_bytes = events.iter().try_fold(0usize, |total, event| {
            let bytes = serde_json::to_vec(event)
                .map_err(|error| error.to_string())?
                .len();
            total
                .checked_add(bytes)
                .ok_or_else(|| "event page bytes overflow".to_string())
        })?;
        if events.len() != selected.len() || actual_bytes != total {
            return Err("event metadata changed during hydration".to_string());
        }
        Ok(events)
    }
    pub fn new(executor: PostgresExecutor) -> RuntimeEventStoreResult<Self> {
        validate_clean_domain_catalog(&executor, RUNTIME_EVENT_DOMAIN, RUNTIME_EVENT_MIGRATIONS)?;
        executor.apply_migrations(RUNTIME_EVENT_DOMAIN, RUNTIME_EVENT_MIGRATIONS)?;
        Ok(Self {
            executor,
            #[cfg(test)]
            recent_read_counts: Arc::new(std::sync::Mutex::new((0, 0, 0))),
            #[cfg(test)]
            sequence_read_counts: Arc::new(std::sync::Mutex::new((0, 0))),
            #[cfg(test)]
            recent_interleave: Arc::new(std::sync::Mutex::new(None)),
        })
    }

    #[must_use]
    pub fn executor(&self) -> &PostgresExecutor {
        &self.executor
    }

    fn checkout_event_read(&self) -> Result<PostgresConnection, StorageError> {
        match RuntimeEventStore::current_projection_work_class() {
            Some(RuntimeProjectionWorkClass::Background) => self.executor.checkout_background(),
            Some(RuntimeProjectionWorkClass::Recovery) | None => {
                self.executor.checkout_online_read()
            }
        }
    }

    fn checkout_event_write(&self) -> Result<PostgresConnection, StorageError> {
        match RuntimeEventStore::current_projection_work_class() {
            Some(RuntimeProjectionWorkClass::Background) => self.executor.checkout_background(),
            Some(RuntimeProjectionWorkClass::Recovery) | None => self.executor.checkout_critical(),
        }
    }

    #[must_use]
    pub fn into_runtime_event_store(self) -> RuntimeEventStore {
        RuntimeEventStore::from_backend(Arc::new(self))
    }
}

impl PostgresRuntimeEventStore {
    fn append_checked_owned_tx(
        &self, request: CheckedAppendRequest, terminal: Option<&SessionTerminalInput>,
        validator: &dyn CheckedSourceValidator,
        control: &dyn runtime::RuntimeTransactionCommitControl,
        claim: &runtime::RuntimeOriginalSessionClaim<'_>,
    ) -> RuntimeEventStoreResult<Option<CheckedAppendReceipt>> {
        request.identity_hash()?;
        if validator.frame_digest() != request.frame_digest {
            return Err(RuntimeEventStoreError::InvalidTransaction("source frame mismatch".into()));
        }
        // Identify an already committed transaction under the original
        // transaction advisory lock before considering a new cancellation.
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        pg(tx.query_one(
            "SELECT set_config('statement_timeout', $1, true)",
            &[&format!("{}ms", request.budget.max_lock_millis)],
        ))?;
        // Do not allow search_path fallback to resolve Session and Runtime
        // tables from different schemas. The selected production topology
        // constructs both adapters from the same scoped executor.
        let same_schema: bool = pg(tx.query_one(
            "SELECT COALESCE(to_regclass('session_records') = to_regclass(format('%I.session_records', current_schema())), false) AND COALESCE(to_regclass('session_runtime_outbox') = to_regclass(format('%I.session_runtime_outbox', current_schema())), false) AND COALESCE(to_regclass('runtime_commits') = to_regclass(format('%I.runtime_commits', current_schema())), false) AND COALESCE(to_regclass('runtime_events') = to_regclass(format('%I.runtime_events', current_schema())), false)",
            &[],
        ))?.try_get(0).map_err(|error| RuntimeEventStoreError::Corrupt(error.to_string()))?;
        if !same_schema {
            return Err(RuntimeEventStoreError::InvalidTransaction("Session and Runtime transaction schemas differ".into()));
        }
        // Match the existing checked owner's transaction identity lock. This
        // makes the duplicate probe and subsequent append one decision.
        lock_original_member_decisions(&mut tx, &request.transaction)?;
        let transaction_lock = format!("cowd-runtime-event-transaction:{}", request.transaction.transaction_id);
        pg(tx.query_one("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", &[&transaction_lock]))?;
        let already_committed = pg(tx.query_opt(
            "SELECT 1 FROM runtime_commits WHERE transaction_id=$1",
            &[&request.transaction.transaction_id],
        ))?.is_some();
        if !already_committed {
            session_postgres::lock_runtime_input_claim_with_consumed_tx(
                &mut tx, claim.session_id, claim.ingress_message_id, claim.turn_id, claim.fence,
                claim.consumed_input_sequence,
            ).map_err(|error| RuntimeEventStoreError::InvalidTransaction(error.to_string()))?;
        }
        let commit = match append_transaction_in_tx_checked(
            &mut tx, &request.transaction, terminal, Some((&request, validator)),
        ) {
            Ok(commit) => commit,
            Err(error @ (RuntimeEventStoreError::StaleRevision { .. }
                | RuntimeEventStoreError::TransactionConflict { .. }
                | RuntimeEventStoreError::CurrentMemberAuthorityExpired)) => {
                // The backend, not a missing receipt, proves this attempt did
                // not commit. If rollback itself fails, preserve Unknown.
                pg(tx.rollback())?;
                control.finish(runtime::RuntimeTransactionOutcome::RolledBack);
                return Err(RuntimeEventStoreError::CheckedAttemptRolledBack(Box::new(error)));
            }
            Err(error) => return Err(error),
        };
        let row = pg(tx.query_one(
            "SELECT checked_source_evidence FROM runtime_commits WHERE transaction_id=$1",
            &[&request.transaction.transaction_id],
        ))?;
        let evidence: Option<Value> = pg(row.try_get(0))?;
        let validation = serde_json::from_value(evidence.ok_or_else(|| {
            RuntimeEventStoreError::Corrupt("checked commit missing source evidence".into())
        })?)?;
        if commit.duplicate {
            pg(tx.commit())?;
            return Ok(Some(CheckedAppendReceipt { commit, validation }));
        }
        if !control.begin().map_err(RuntimeEventStoreError::InvalidTransaction)? {
            pg(tx.rollback())?;
            return Ok(None);
        }
        let mut guard = PgOwnedCommitGuard(control, false);
        control.before_commit();
        if !control.commit_wins().map_err(RuntimeEventStoreError::InvalidTransaction)? {
            pg(tx.rollback())?;
            control.finish(runtime::RuntimeTransactionOutcome::RolledBack);
            guard.1 = true;
            return Ok(None);
        }
        pg(tx.commit())?;
        control.finish(runtime::RuntimeTransactionOutcome::Committed);
        guard.1 = true;
        Ok(Some(CheckedAppendReceipt { commit, validation }))
    }

}

impl RuntimeEventStoreBackend for PostgresRuntimeEventStore {
    /// Deadline-bounded reader for the fixture evidence oracle.  Without it a
    /// PostgreSQL-backed run can never reach a business verdict: the oracle
    /// reports "backend has no deadline-bounded fixture reader" and fails on
    /// a harness gap instead of the product behaviour.
    #[cfg(any(test, feature = "test-fixtures"))]
    fn fixture_evidence_page(
        &self,
        request: &FixtureEvidencePageRequest<'_>,
    ) -> Result<FixtureEvidencePage, &'static str> {
        use std::time::Instant;
        if Instant::now() >= request.deadline {
            return Err("Unavailable: deadline exhausted before read");
        }
        // PostgreSQL sequence values are signed BIGINT. An unbounded
        // head-only cursor lies beyond every representable stored sequence.
        let after_sequence = to_i64(request.after_sequence.min(i64::MAX as u64), "after_sequence")
            .map_err(|_| "Unavailable: sequence bound overflow")?;
        // The oracle asks for an open-ended range; clamp instead of failing so
        // an unbounded request still reads every event that exists.
        let through_sequence = to_i64(
            request.through_sequence.min(i64::MAX as u64),
            "through_sequence",
        )
        .map_err(|_| "Unavailable: sequence bound overflow")?;
        let limit = i64::try_from(request.max_events.min(100))
            .map_err(|_| "Unavailable: page limit overflow")?;
        let mut connection = self
            .checkout_event_read()
            .map_err(|_| "Unavailable: event read checkout failed")?;
        let head = connection
            .query(
                "SELECT COALESCE(MAX(sequence),0) FROM runtime_events WHERE stream_id=$1",
                &[&request.stream_id],
            )
            .map_err(|_| "Unavailable: stream head read failed")
            .and_then(|rows| {
                rows.first()
                    .map(|row| {
                        row.try_get::<_, i64>(0)
                            .map_err(|_| "Unavailable: stream head read failed")
                    })
                    .unwrap_or(Ok(0))
            })
            .map_err(|_| "Unavailable: stream head read failed")?;
        let head = from_i64(head, "head").map_err(|_| "Unavailable: stream head corrupt")?;
        let mut page = FixtureEvidencePage {
            head,
            next_sequence: request.after_sequence,
            encoded_bytes: 0,
            records: Vec::new(),
            stop: None,
        };
        let through = request.through_sequence.min(head);
        if through <= request.after_sequence {
            return Ok(page);
        }
        let rows = connection
            .query(
                &format!(
                    "SELECT event_id, sequence, encoded_bytes FROM runtime_events WHERE stream_id=$1 AND sequence>$2 AND sequence<=$3 ORDER BY sequence ASC LIMIT $4"
                ),
                &[&request.stream_id, &after_sequence, &through_sequence, &limit],
            )
            .map_err(|_| "Unavailable: event metadata read failed")?;
        let budget = request.max_bytes.min(1024 * 1024);
        let mut selected: Vec<String> = Vec::new();
        let mut total = 0usize;
        for row in rows {
            if Instant::now() >= request.deadline {
                page.stop = Some("deadline");
                break;
            }
            let identity: String = row
                .try_get(0)
                .map_err(|_| "Unavailable: event identity is corrupt")?;
            let sequence: i64 = row
                .try_get(1)
                .map_err(|_| "Unavailable: event sequence is corrupt")?;
            let encoded: i64 = row
                .try_get(2)
                .map_err(|_| "Unavailable: event encoded bytes are missing")?;
            let bytes = usize::try_from(encoded)
                .ok()
                .filter(|bytes| *bytes > 0)
                .ok_or("Unavailable: event encoded bytes are corrupt")?;
            if bytes > budget.saturating_sub(total) {
                page.stop = Some("bytes");
                break;
            }
            total += bytes;
            selected.push(identity);
            page.next_sequence =
                from_i64(sequence, "sequence").map_err(|_| "Unavailable: event sequence corrupt")?;
        }
        if !selected.is_empty() {
            let events = pg(
                connection.query(
                    &format!(
                        "SELECT {EVENT_COLUMNS} FROM runtime_events WHERE event_id=ANY($1) ORDER BY sequence ASC"
                    ),
                    &[&selected],
                ),
            )
            .and_then(rows_to_events)
            .map_err(|_| "Unavailable: event rows read failed")?;
            page.encoded_bytes = total;
            page.records = events;
        }
        if page.stop.is_none() && page.records.len() >= request.max_events.min(100) {
            page.stop = Some("events");
        }
        Ok(page)
    }

    fn bounded_event_by_idempotency_key(
        &self,
        stream: &str,
        key: &str,
        through: u64,
        max_bytes: usize,
    ) -> RuntimeEventStoreResult<runtime::RuntimeBoundedEventLookup> {
        let mut connection = self.checkout_event_read()?;
        let through = to_i64(through, "bounded source cut")?;
        let Some(metadata) = pg(connection.query_opt(
            "SELECT encoded_bytes FROM runtime_events WHERE stream_id=$1 AND idempotency_key=$2 AND commit_cursor<=$3",
            &[&stream, &key, &through],
        ))? else {
            return Ok(runtime::RuntimeBoundedEventLookup::Absent);
        };
        let bytes = usize::try_from(from_i64(pg(metadata.try_get(0))?, "bounded event extent")?)
            .map_err(|_| RuntimeEventStoreError::Corrupt("bounded event extent overflow".into()))?;
        if bytes == 0 {
            return Err(RuntimeEventStoreError::Corrupt("zero bounded event extent".into()));
        }
        if bytes > max_bytes {
            return Ok(runtime::RuntimeBoundedEventLookup::Deferred { required_bytes: bytes });
        }
        let row = pg(connection.query_opt(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 AND idempotency_key=$2 AND commit_cursor<=$3 AND encoded_bytes=$4"),
            &[&stream, &key, &through, &to_i64(bytes as u64, "bounded event extent")?],
        ))?.ok_or_else(|| RuntimeEventStoreError::Corrupt("bounded source changed during read".into()))?;
        let record = row_to_event(&row)?;
        if serde_json::to_vec(&record)?.len() != bytes {
            return Err(RuntimeEventStoreError::Corrupt("bounded event extent mismatch".into()));
        }
        Ok(runtime::RuntimeBoundedEventLookup::Record { record: Box::new(record), encoded_bytes: bytes })
    }

    fn recent_event_lines(
        &self,
        stream_id: &str,
        references: &[runtime::RuntimeEventRef],
        through_cursor: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<runtime::MissionControlEventLine>, String> {
        let through =
            to_i64(through_cursor, "through_cursor").map_err(|error| error.to_string())?;
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
        if max_events == 0 || max_bytes == 0 || through == 0 {
            return Ok(Vec::new());
        }
        let limit = max_events.min(50) as i64;
        let budget = max_bytes.min(1024 * 1024);
        let references = serde_json::to_value(references).map_err(|error| error.to_string())?;
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let rows = connection
            .query(
                RECENT_EVENT_LINE_METADATA_SQL,
                &[&stream_id, &references, &through, &limit],
            )
            .map_err(|_| "recent metadata query failed".to_string())?;
        #[cfg(test)]
        {
            let mut counts = self.recent_read_counts.lock().unwrap();
            counts.0 += rows.len();
            counts.2 += rows.len() * 24;
        }
        let mut cursors = Vec::with_capacity(rows.len());
        let mut indexes = Vec::with_capacity(rows.len());
        let mut total = 0usize;
        let mut previous = None;
        for row in rows {
            let cursor: i64 = row.try_get(0).map_err(|error| error.to_string())?;
            let index: i64 = row.try_get(1).map_err(|error| error.to_string())?;
            let encoded: i64 = row.try_get(2).map_err(|error| error.to_string())?;
            let size =
                usize::try_from(encoded).map_err(|_| "recent scalar size overflow".to_string())?;
            let position = (cursor, index);
            if cursor <= 0
                || cursor > through
                || index < 0
                || index > i64::from(u32::MAX)
                || previous.is_some_and(|previous| position >= previous)
            {
                return Err("recent metadata position mismatch".into());
            }
            if size > budget.saturating_sub(total) {
                return Err("HistoryBudgetExceeded: recent event line scalars".into());
            }
            previous = Some(position);
            total += size;
            cursors.push(cursor);
            indexes.push(index);
        }
        if cursors.is_empty() {
            return Ok(Vec::new());
        }
        #[cfg(test)]
        {
            let interleave = self.recent_interleave.lock().unwrap().take();
            if let Some(input) = interleave {
                self.append(input)?;
            }
        }
        let rows = connection.query(
            "SELECT event_id,stream_id,commit_cursor,transaction_index,scope,kind,status,actor,created_at_ms
             FROM runtime_events WHERE commit_cursor<=$3 AND (commit_cursor,transaction_index) IN
                (SELECT * FROM unnest($1::BIGINT[],$2::BIGINT[]))
             ORDER BY commit_cursor DESC,transaction_index DESC", &[&cursors,&indexes,&through],
        ).map_err(|_| "recent scalar query failed".to_string())?;
        if rows.len() != cursors.len() {
            return Err("recent metadata selected rows missing".into());
        }
        let mut lines = Vec::with_capacity(rows.len());
        let mut seen = BTreeSet::new();
        let mut actual = 0usize;
        for (offset, row) in rows.iter().enumerate() {
            let cursor: i64 = row.try_get(2).map_err(|error| error.to_string())?;
            let index: i64 = row.try_get(3).map_err(|error| error.to_string())?;
            if (cursor, index) != (cursors[offset], indexes[offset]) {
                return Err("recent metadata position changed".into());
            }
            let created: i64 = row.try_get(8).map_err(|error| error.to_string())?;
            let line = runtime::MissionControlEventLine {
                event_id: row.try_get(0).map_err(|error| error.to_string())?,
                stream_id: row.try_get(1).map_err(|error| error.to_string())?,
                cursor: from_i64(cursor, "recent cursor").map_err(|error| error.to_string())?,
                transaction_index: u32::try_from(index)
                    .map_err(|_| "recent index overflow".to_string())?,
                scope: row.try_get(4).map_err(|error| error.to_string())?,
                kind: row.try_get(5).map_err(|error| error.to_string())?,
                status: row.try_get(6).map_err(|error| error.to_string())?,
                actor: row.try_get(7).map_err(|error| error.to_string())?,
                created_at_ms: from_i64(created, "recent timestamp")
                    .map_err(|error| error.to_string())?,
            };
            #[cfg(test)]
            {
                let mut counts = self.recent_read_counts.lock().unwrap();
                counts.1 += 1;
                counts.2 += 24
                    + line.event_id.len()
                    + line.stream_id.len()
                    + line.scope.len()
                    + line.kind.len()
                    + line.status.as_ref().map_or(0, String::len)
                    + line.actor.as_ref().map_or(0, String::len);
            }
            if !seen.insert(line.event_id.clone()) {
                return Err("recent event identity duplicated".into());
            }
            actual = actual
                .checked_add(
                    serde_json::to_vec(&line)
                        .map_err(|error| error.to_string())?
                        .len(),
                )
                .ok_or_else(|| "recent line bytes overflow".to_string())?;
            if actual > budget || actual > total {
                return Err("recent metadata size mismatch".into());
            }
            lines.push(line);
        }
        Ok(lines)
    }
    fn original_transaction_receipt(&self, transaction_id: &str)
        -> RuntimeEventStoreResult<Option<AppendTransactionReceipt>> {
        if transaction_id.trim().is_empty() {
            return Err(RuntimeEventStoreError::InvalidTransaction("original transaction identity missing".into()));
        }
        let mut connection = self.executor.checkout_online_read()?;
        if pg(connection.query_opt("SELECT 1 FROM runtime_commits WHERE transaction_id=$1", &[&transaction_id]))?.is_none() {
            return Ok(None);
        }
        load_receipt(&mut connection, transaction_id, true).map(Some)
    }

    fn original_checked_transaction_receipt(&self, transaction_id: &str)
        -> RuntimeEventStoreResult<Option<CheckedAppendReceipt>> {
        if transaction_id.trim().is_empty() {
            return Err(RuntimeEventStoreError::InvalidTransaction("original transaction identity missing".into()));
        }
        let mut connection = self.executor.checkout_online_read()?;
        let mut tx = pg(connection.transaction())?;
        let Some(row) = pg(tx.query_opt("SELECT checked_source_evidence FROM runtime_commits WHERE transaction_id=$1", &[&transaction_id]))? else {
            return Ok(None);
        };
        let value: Option<Value> = pg(row.try_get(0))?;
        let validation = serde_json::from_value(value.ok_or_else(||
            RuntimeEventStoreError::InvalidTransaction("original transaction is not checked".into()))?)?;
        let commit = load_receipt(&mut tx, transaction_id, true)?;
        pg(tx.commit())?;
        Ok(Some(CheckedAppendReceipt { commit, validation }))
    }

    fn transaction_stream_revision_exact(
        &self,
        transaction_id: &str,
        stream_id: &str,
    ) -> RuntimeEventStoreResult<Option<CommittedStreamRevision>> {
        let row = pg(self.executor.checkout_online_read()?.query_opt("SELECT expected_revision,committed_revision FROM runtime_transaction_streams WHERE transaction_id=$1 AND stream_id=$2", &[&transaction_id,&stream_id]))?;
        row.map(|row| {
            Ok(CommittedStreamRevision {
                stream_id: stream_id.into(),
                expected_revision: from_i64(row.get(0), "expected revision")?,
                committed_revision: from_i64(row.get(1), "committed revision")?,
            })
        })
        .transpose()
    }

    fn prepared_content_root(
        &self,
        origin: &runtime::PreparedArtifactOrigin,
        artifact: &runtime::ArtifactRef,
    ) -> RuntimeEventStoreResult<runtime::PreparedContentRoot> {
        use runtime::{PreparedArtifactOrigin, PreparedContentRoot};
        origin
            .validate()
            .map_err(RuntimeEventStoreError::InvalidTransaction)?;
        let mut connection = self.executor.checkout_online_read()?;
        let root = match origin {
            PreparedArtifactOrigin::EventBody {
                stream_id,
                idempotency_key,
                kind,
            } => {
                if kind == "execution.host.transcript" {
                    let primary = Self::host_transcript_root_event(
                        &mut connection,
                        stream_id,
                        idempotency_key,
                    )?;
                    let mut node = None;
                    let mut graph = None;
                    if let Some(primary) = primary.as_ref() {
                        if let Ok((graph_id, _, _)) =
                            RuntimeEventStore::host_transcript_root_identity(primary)
                        {
                            node = Self::host_transcript_pair_metadata(
                                &mut connection,
                                primary,
                                stream_id,
                                true,
                            )?;
                            graph = Self::host_transcript_pair_metadata(
                                &mut connection,
                                primary,
                                &graph_id,
                                false,
                            )?;
                        }
                    }
                    return Ok(RuntimeEventStore::host_transcript_root(
                        primary.as_ref(),
                        node.as_ref(),
                        graph.as_ref(),
                        artifact,
                    ));
                }
                let row = connection.query_opt(
                    "SELECT CASE WHEN octet_length(kind)<=1024 THEN kind ELSE '' END,schema_version,CASE WHEN octet_length(scope)<=64 THEN scope ELSE '' END,
                     CASE WHEN octet_length((payload->'observation_schema')::TEXT)<=16 THEN payload->'observation_schema' END,
                     jsonb_build_object('content_schema',CASE WHEN octet_length((e->'content_schema')::TEXT)<=16 THEN e->'content_schema' END,
                        'sha256',CASE WHEN octet_length((e->'sha256')::TEXT)<=80 THEN e->'sha256' END,
                        'bytes',CASE WHEN octet_length((e->'bytes')::TEXT)<=20 THEN e->'bytes' END,
                        'body',CASE WHEN octet_length(((e->'body')-'value')::TEXT)<=8192 THEN (e->'body')-'value' ELSE NULL END),
                     CASE WHEN kind='goal.observation' AND octet_length(payload->>'session_id')<=1024 THEN payload->>'session_id'
                        WHEN kind<>'goal.observation' AND octet_length(payload#>>'{_runtime_activity_binding,session_id}')<=1024 THEN payload#>>'{_runtime_activity_binding,session_id}' END
                     FROM runtime_events CROSS JOIN LATERAL (SELECT CASE WHEN kind='goal.observation' THEN payload->'observation_body' ELSE payload END AS e) selected
                     WHERE stream_id=$1 AND idempotency_key=$2", &[&stream_id,&idempotency_key]).map_err(|error| RuntimeEventStoreError::Corrupt(error.to_string()))?;
                match row {
                    None => PreparedContentRoot::Absent,
                    Some(row) => {
                        let actual_kind: String = row.get(0);
                        let scope: String = row.get(2);
                        if row.get::<_, Option<String>>(5).is_none_or(|session| {
                            artifact.visibility_scope != format!("session:{session}")
                        }) {
                            return Ok(PreparedContentRoot::Blocked);
                        }
                        if actual_kind != *kind
                            || row.get::<_, i64>(1) != 2
                            || (kind == "goal.observation"
                                && (scope != "goal"
                                    || row.get::<_, Option<Value>>(3)
                                        != Some(serde_json::json!(2))))
                            || (kind != "goal.observation" && scope != "execution_graph")
                        {
                            return Ok(PreparedContentRoot::Blocked);
                        }
                        RuntimeEventStore::prepared_slot_root(&row.get::<_, Value>(4), artifact)
                    }
                }
            }
            PreparedArtifactOrigin::ToolEffectClosure {
                stream_id,
                idempotency_key,
            } => {
                let primary =
                    Self::process_closure_root_event(&mut connection, stream_id, idempotency_key)?;
                let identity = primary.as_ref().and_then(|primary| {
                    RuntimeEventStore::process_closure_index_identity(
                        &primary.payload,
                        &primary.refs,
                    )
                    .ok()
                });
                let index = match identity {
                    Some((stream, key)) => {
                        Self::process_closure_root_event(&mut connection, &stream, &key)?
                    }
                    None => None,
                };
                RuntimeEventStore::process_closure_root(primary.as_ref(), index.as_ref(), artifact)
            }
            PreparedArtifactOrigin::CheckpointBody {
                projection_id,
                source_cursor,
            } => {
                let row = connection.query_opt(
                    "SELECT source_cursor,
                     CASE WHEN octet_length((payload->'schema')::TEXT)<=16 THEN payload->'schema' END,
                     CASE WHEN octet_length((payload->'schema_version')::TEXT)<=16 THEN payload->'schema_version' END,
                     jsonb_build_object('content_schema',CASE WHEN octet_length((payload#>'{body,content_schema}')::TEXT)<=16 THEN payload#>'{body,content_schema}' END,
                        'sha256',CASE WHEN octet_length((payload#>'{body,sha256}')::TEXT)<=80 THEN payload#>'{body,sha256}' END,
                        'bytes',CASE WHEN octet_length((payload#>'{body,bytes}')::TEXT)<=20 THEN payload#>'{body,bytes}' END,
                        'body',CASE WHEN octet_length(((payload#>'{body,body}')-'value')::TEXT)<=8192 THEN (payload#>'{body,body}')-'value' ELSE NULL END),
                     CASE WHEN octet_length(payload->>'scope')<=1024 THEN payload->>'scope'
                        WHEN octet_length(payload->>'session_id')<=1024 THEN 'session:'||(payload->>'session_id') END,
                     COALESCE(jsonb_typeof(payload->'source_fence')='object',FALSE)
                     FROM runtime_projection_checkpoints WHERE projection_id=$1", &[&projection_id]).map_err(|error| RuntimeEventStoreError::Corrupt(error.to_string()))?;
                match row {
                    None => PreparedContentRoot::Absent,
                    Some(row) => {
                        let program_schema =
                            RuntimeEventStore::program_read_model_checkpoint_schema(projection_id);
                        let schema: Option<Value> =
                            row.get(if program_schema.is_some() { 2 } else { 1 });
                        let valid_schema = if let Some(expected) = program_schema {
                            schema.as_ref().and_then(Value::as_u64) == Some(expected)
                        } else if projection_id.starts_with("projection-history:") {
                            matches!(schema.as_ref().and_then(Value::as_u64), Some(3 | 4 | 5 | 6))
                                && (schema != Some(serde_json::json!(6)) || row.get::<_, bool>(5))
                        } else {
                            schema == Some(serde_json::json!(2))
                        };
                        if !valid_schema {
                            return Ok(PreparedContentRoot::Blocked);
                        }
                        if row.get::<_, Option<String>>(4).as_ref()
                            != Some(&artifact.visibility_scope)
                        {
                            return Ok(PreparedContentRoot::Blocked);
                        }
                        match RuntimeEventStore::prepared_slot_root(
                            &row.get::<_, Value>(3),
                            artifact,
                        ) {
                            PreparedContentRoot::Canonical
                                if from_i64(row.get(0), "checkpoint source")? == *source_cursor =>
                            {
                                PreparedContentRoot::Checkpoint
                            }
                            PreparedContentRoot::Canonical => PreparedContentRoot::Blocked,
                            root => root,
                        }
                    }
                }
            }
            PreparedArtifactOrigin::TerminalBody {
                terminal_id,
                session_id,
                execution_id,
                turn_id,
                event_stream,
                event_key,
            } => {
                let mut found = false;
                let mut outbox_reference = None;
                if let Some(row) = connection
                    .query_opt(
                        "SELECT CASE WHEN octet_length(session_id)<=1024 THEN session_id END,
                     CASE WHEN octet_length(execution_id)<=1024 THEN execution_id END,
                     CASE WHEN octet_length(turn_id)<=1024 THEN turn_id END,
                     CASE WHEN octet_length(payload_ref)<=8192 THEN payload_ref END
                     FROM runtime_session_outbox WHERE terminal_id=$1",
                        &[&terminal_id],
                    )
                    .map_err(|error| RuntimeEventStoreError::Corrupt(error.to_string()))?
                {
                    if row.get::<_, Option<String>>(0).as_ref() != Some(session_id)
                        || row.get::<_, Option<String>>(1).as_ref() != Some(execution_id)
                        || row.get::<_, Option<String>>(2).as_ref() != Some(turn_id)
                    {
                        return Ok(PreparedContentRoot::Blocked);
                    }
                    match row
                        .get::<_, Option<String>>(3)
                        .as_deref()
                        .ok_or(())
                        .and_then(|encoded| {
                            runtime::decode_session_terminal_artifact_ref(encoded).map_err(|_| ())
                        }) {
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
                if let Some(row) = connection.query_opt(
                    "SELECT kind,CASE WHEN octet_length((payload-'payload_ref')::TEXT)<=8192 THEN jsonb_build_object('terminal_id',payload->'terminal_id','session_id',payload->'session_id','execution_id',payload->'execution_id','turn_id',payload->'turn_id') END,
                     CASE WHEN octet_length(payload->>'payload_ref')<=8192 THEN payload->>'payload_ref' END,
                     CASE WHEN octet_length(scope)<=64 THEN scope ELSE '' END,schema_version
                     FROM runtime_events WHERE stream_id=$1 AND idempotency_key=$2", &[&event_stream,&event_key]).map_err(|error| RuntimeEventStoreError::Corrupt(error.to_string()))? {
                    let payload: Option<Value> = row.get(1);
                    let Some(payload) = payload else { return Ok(PreparedContentRoot::Blocked); };
                    if row.get::<_,String>(3) != "session_input" || row.get::<_,i64>(4) != 1 || row.get::<_,String>(0) != "runtime.session.terminal_requested" || payload["terminal_id"] != *terminal_id
                        || payload["session_id"] != *session_id || payload["execution_id"] != *execution_id || payload["turn_id"] != *turn_id { return Ok(PreparedContentRoot::Blocked); }
                    match row.get::<_,Option<String>>(2).as_deref().ok_or(()).and_then(|encoded| runtime::decode_session_terminal_artifact_ref(encoded).map_err(|_| ())) {
                        Ok(reference) => {
                            if reference.visibility_scope != format!("session:{session_id}") || reference.media_type != "application/vnd.cowd.session-terminal+json" || !reference.is_durable()
                                || outbox_reference.as_ref().is_some_and(|outbox| outbox != &reference) { return Ok(PreparedContentRoot::Blocked); }
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
        if root == PreparedContentRoot::Absent {
            let reference = serde_json::json!([{"kind":"artifact","id":artifact.selector}]);
            if pg(connection.query_one(
                "SELECT EXISTS(SELECT 1 FROM runtime_events WHERE refs @> $1::JSONB)",
                &[&reference],
            ))?
            .get::<_, bool>(0)
            {
                return Ok(PreparedContentRoot::Canonical);
            }
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
        let through =
            to_i64(through_cursor, "through_cursor").map_err(|error| error.to_string())?;
        let (cursor, index) = after.unwrap_or((0, 0));
        let cursor = to_i64(cursor, "after_cursor").map_err(|error| error.to_string())?;
        if max_events == 0 || max_bytes == 0 || identity.is_empty() {
            return Ok(Vec::new());
        }
        let predicate = match scope {
            RuntimeEventScope::Session => "(session_id=$1 OR stream_id=$1 OR stream_id='session:'||$1 OR refs @> jsonb_build_array(jsonb_build_object('kind','session','id',$1::text)))",
            RuntimeEventScope::ExecutionGraph => "(root_execution_id=$1 OR stream_id=$1 OR refs @> jsonb_build_array(jsonb_build_object('kind','execution_graph','id',$1::text)) OR refs @> jsonb_build_array(jsonb_build_object('kind','execution','id',$1::text)))",
            RuntimeEventScope::ExecutionNode => "activity_id=$1",
            _ => return Err(PositionPageError::Other("unsupported position query scope".into())),
        };
        let kind = kind.unwrap_or("");
        let limit =
            i64::try_from(max_events.min(100)).map_err(|_| "page limit overflow".to_string())?;
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let metadata = connection.query(&format!("SELECT event_id,encoded_bytes FROM runtime_events WHERE {predicate} AND ($2='' OR kind=$2) AND (commit_cursor,transaction_index)>($3,$4) AND commit_cursor<=$5 ORDER BY commit_cursor,transaction_index LIMIT $6"),
            &[&identity,&kind,&cursor,&i64::from(index),&through,&limit]).map_err(|_| "event position metadata read failed".to_string())?;
        let mut selected: Vec<String> = Vec::new();
        let mut total = 0usize;
        let budget = max_bytes.min(1024 * 1024);
        for row in metadata {
            let identity: String = row.get(0);
            let bytes = usize::try_from(row.get::<_, i64>(1))
                .ok()
                .filter(|bytes| *bytes > 0)
                .ok_or("invalid event size metadata")?;
            if bytes > budget.saturating_sub(total) {
                if selected.is_empty() {
                    return Err(PositionPageError::FirstRowExceedsBudget(identity));
                }
                break;
            }
            total += bytes;
            selected.push(identity);
        }
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        let events = pg(connection.query(&format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE event_id=ANY($1) ORDER BY commit_cursor,transaction_index"), &[&selected]))
            .and_then(rows_to_events).map_err(|error| error.to_string())?;
        let actual = events.iter().try_fold(0usize, |total, event| {
            total
                .checked_add(
                    serde_json::to_vec(event)
                        .map_err(|error| error.to_string())?
                        .len(),
                )
                .ok_or_else(|| "event page byte overflow".to_string())
        })?;
        if events.len() != selected.len() || actual != total {
            return Err(PositionPageError::Other("event position metadata changed during hydration".into()));
        }
        Ok(events)
    }
    fn durable_commit_cursor(&self) -> RuntimeEventStoreResult<u64> {
        let mut connection = self.checkout_event_read()?;
        let row = pg(connection.query_one(
            "SELECT COALESCE(MAX(commit_cursor), 0) FROM runtime_commits",
            &[],
        ))?;
        from_i64(pg(row.try_get(0))?, "commit_cursor")
    }

    fn background_projection_capacity_hint(&self) -> usize {
        self.executor
            .health()
            .lanes
            .iter()
            .find(|lane| lane.workload == storage::PostgresWorkloadClass::Background)
            .map_or(1, |lane| lane.max_connections as usize)
    }

    fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String> {
        validate_event(&input).map_err(|error| error.to_string())?;
        let mut connection = self
            .checkout_event_write()
            .map_err(|error| error.to_string())?;
        let mut tx = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let stream_lock = format!("cowd-runtime-event-stream:{}", input.stream_id);
        pg(tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&stream_lock],
        ))
        .map_err(|error| error.to_string())?;
        let expected_revision =
            stream_head(&mut tx, &input.stream_id).map_err(|error| error.to_string())?;
        let request = AppendTransactionRequest {
            transaction_id: format!("runtime-tx-{}", uuid::Uuid::new_v4()),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: input.stream_id.clone(),
                expected_revision,
            }],
            events: vec![input.into()],
        };
        let receipt =
            append_transaction_in_tx(&mut tx, &request, None).map_err(|error| error.to_string())?;
        let event_id = receipt
            .event_ids
            .first()
            .ok_or_else(|| "committed runtime transaction has no event".to_string())?;
        let event = pg(tx.query_one(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE event_id=$1"),
            &[&event_id],
        ))
        .and_then(|row| row_to_event(&row))
        .map_err(|error| error.to_string())?;
        tx.commit().map_err(|error| error.to_string())?;
        Ok(event)
    }

    fn append_transaction(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let receipt = append_transaction_in_tx(&mut tx, &request, None)?;
        pg(tx.commit())?;
        Ok(receipt)
    }

    fn append_transaction_cancellable(
        &self, request: AppendTransactionRequest,
        control: &dyn runtime::RuntimeTransactionCommitControl,
    ) -> RuntimeEventStoreResult<Option<AppendTransactionReceipt>> {
        self.append_owned_cancellable_inner(request, control)
    }

    fn append_checked_transaction(
        &self,
        request: CheckedAppendRequest,
        validator: &dyn CheckedSourceValidator,
    ) -> RuntimeEventStoreResult<CheckedAppendReceipt> {
        request.identity_hash()?;
        if validator.frame_digest() != request.frame_digest {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "source frame mismatch".into(),
            ));
        }
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        pg(tx.query_one(
            "SELECT set_config('statement_timeout', $1, true)",
            &[&format!("{}ms", request.budget.max_lock_millis)],
        ))?;
        let commit = append_transaction_in_tx_checked(
            &mut tx,
            &request.transaction,
            None,
            Some((&request, validator)),
        )?;
        let row = pg(tx.query_one(
            "SELECT checked_source_evidence FROM runtime_commits WHERE transaction_id=$1",
            &[&request.transaction.transaction_id],
        ))?;
        let evidence: Option<Value> = pg(row.try_get(0))?;
        let validation = serde_json::from_value(evidence.ok_or_else(|| {
            RuntimeEventStoreError::Corrupt("checked commit missing source evidence".into())
        })?)?;
        pg(tx.commit())?;
        Ok(CheckedAppendReceipt { commit, validation })
    }

    fn append_checked_transaction_cancellable(
        &self, request: CheckedAppendRequest, validator: &dyn CheckedSourceValidator,
        control: &dyn runtime::RuntimeTransactionCommitControl,
        claim: &runtime::RuntimeOriginalSessionClaim<'_>,
    ) -> RuntimeEventStoreResult<Option<CheckedAppendReceipt>> {
        self.append_checked_owned_tx(request, None, validator, control, claim)
    }

    fn append_original_member_decision(
        &self, request: CheckedAppendRequest, member: runtime::OriginalMemberDecisionInput,
        validator: &dyn CheckedSourceValidator, control: &dyn runtime::RuntimeTransactionCommitControl,
        claim: &runtime::RuntimeOriginalSessionClaim<'_>,
    ) -> RuntimeEventStoreResult<runtime::OriginalMemberDecision> {
        let invalid = || RuntimeEventStoreError::InvalidTransaction("original member decision identity unavailable".into());
        let checked_hash = request.identity_hash()?;
        if validator.frame_digest() != request.frame_digest || member.terminal_transaction_id != format!("member-noncommit:{checked_hash}")
            || request.transaction.transaction_id == member.terminal_transaction_id || member.preparation.events.len() != 1 {
            return Err(invalid());
        }
        let preparation = &member.preparation.events[0];
        let pp = &preparation.event.payload;
        let outer = pp["outer_key"].as_str().filter(|v| !v.is_empty()).ok_or_else(invalid)?;
        let index = pp["member_index"].as_u64().ok_or_else(invalid)?;
        let ticket = pp.get("ticket").filter(|v| !v.is_null()).ok_or_else(invalid)?;
        let stream = format!("execution-effect:{outer}");
        let member_key = format!("agentic-member:{index}");
        let prep_key = format!("agentic-member-preparation:{index}");
        let prep_hash = request_hash_with_terminal(&member.preparation, None)?;
        let expected_payload = serde_json::json!({"ticket":ticket,"outer_key":outer,"member_index":index,
            "preparation_transaction_id":member.preparation.transaction_id,
            "preparation_request_hash":prep_hash,"checked_transaction_id":request.transaction.transaction_id,
            "checked_request_hash":checked_hash});
        let terminal = &member.terminal_event;
        if preparation.event.stream_id != stream || preparation.event.scope != RuntimeEventScope::ExecutionNode
            || preparation.event.kind != "execution.effect.member_preparation" || preparation.event.status.as_deref() != Some("inflight")
            || preparation.event.actor.as_deref() != Some("execution_commit_service") || preparation.schema_version != 1
            || preparation.idempotency_key.as_deref() != Some(prep_key.as_str())
            || member.preparation_receipt.transaction_id != member.preparation.transaction_id
            || member.preparation_receipt.request_hash != prep_hash || member.preparation_receipt.commit_cursor == 0
            || member.preparation_receipt.event_ids.len() != 1
            || terminal.event.stream_id != stream || terminal.event.scope != RuntimeEventScope::ExecutionNode
            || terminal.event.kind != "execution.effect.member_noncommit" || terminal.event.status.as_deref() != Some("not_committed")
            || terminal.event.actor.as_deref() != Some("execution_commit_service") || terminal.event.refs != preparation.event.refs
            || terminal.schema_version != 1 || terminal.idempotency_key.as_deref() != Some(member_key.as_str())
            || terminal.event.payload != expected_payload
            || !request.transaction.expected_streams.iter().any(|source| source.stream_id == stream) {
            return Err(invalid());
        }
        let mut markers = request.transaction.events.iter().filter(|event| event.event.kind == "agentic.batch_member.index");
        let marker = markers.next().ok_or_else(invalid)?;
        if markers.next().is_some() || marker.event.stream_id != stream || marker.event.scope != RuntimeEventScope::ExecutionNode
            || marker.event.status.as_deref() != Some("committed") || marker.event.actor.as_deref() != Some("runtime.agentic_action")
            || marker.schema_version != 1 || marker.idempotency_key.as_deref() != Some(member_key.as_str())
            || marker.event.payload["outer_key"] != outer || marker.event.payload["member_index"] != index
            || marker.event.payload.get("ticket") != Some(ticket) {
            return Err(invalid());
        }
        let position = marker.event.payload["program_event_index"].as_u64().and_then(|v| usize::try_from(v).ok()).ok_or_else(invalid)?;
        let action = request.transaction.events.get(position).ok_or_else(invalid)?;
        if action.event.payload.pointer("/envelope/actor/session_id").and_then(Value::as_str) != Some(claim.session_id)
            || action.event.payload.pointer("/envelope/actor/turn_id").and_then(Value::as_str) != Some(claim.turn_id)
            || marker.event.payload["program_stream"] != action.event.stream_id
            || marker.event.payload["program_event_kind"] != action.event.kind
            || marker.event.payload["program_event_key"].as_str() != action.idempotency_key.as_deref()
            || !matches!(action.event.kind.as_str(), "agentic.action_applied" | "agentic.completion_rejected") {
            return Err(invalid());
        }
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        pg(tx.query_one("SELECT set_config('statement_timeout', $1, true)", &[&format!("{}ms", request.budget.max_lock_millis)]))?;
        let same_schema: bool = pg(tx.query_one("SELECT COALESCE(to_regclass('session_records') = to_regclass(format('%I.session_records', current_schema())), false) AND COALESCE(to_regclass('session_runtime_outbox') = to_regclass(format('%I.session_runtime_outbox', current_schema())), false) AND COALESCE(to_regclass('runtime_commits') = to_regclass(format('%I.runtime_commits', current_schema())), false) AND COALESCE(to_regclass('runtime_events') = to_regclass(format('%I.runtime_events', current_schema())), false)", &[]))?.try_get(0).map_err(|error| RuntimeEventStoreError::Corrupt(error.to_string()))?;
        if !same_schema { return Err(invalid()); }
        lock_original_member_decisions(&mut tx, &request.transaction)?;
        let action_lock = format!("cowd-runtime-event-transaction:{}", request.transaction.transaction_id);
        let terminal_lock = format!("cowd-runtime-event-transaction:{}", member.terminal_transaction_id);
        let row = pg(tx.query_one("SELECT hashtextextended($1, 0), hashtextextended($2, 0)", &[&action_lock, &terminal_lock]))?;
        let mut transaction_locks = vec![pg(row.try_get::<_, i64>(0))?, pg(row.try_get::<_, i64>(1))?];
        transaction_locks.sort_unstable(); transaction_locks.dedup();
        for lock in transaction_locks { pg(tx.query_one("SELECT pg_advisory_xact_lock($1)", &[&lock]))?; }
        let winner_exists = pg(tx.query_opt("SELECT 1 FROM runtime_commits WHERE transaction_id=$1", &[&request.transaction.transaction_id]))?.is_some();
        let terminal_exists = pg(tx.query_opt("SELECT 1 FROM runtime_commits WHERE transaction_id=$1", &[&member.terminal_transaction_id]))?.is_some();
        if !winner_exists && !terminal_exists {
            session_postgres::lock_runtime_input_claim_with_consumed_tx(&mut tx, claim.session_id, claim.ingress_message_id,
                claim.turn_id, claim.fence, claim.consumed_input_sequence).map_err(|e| RuntimeEventStoreError::InvalidTransaction(e.to_string()))?;
        }
        let mut sources = request.transaction.expected_streams.iter().map(|source| source.stream_id.as_str()).collect::<Vec<_>>();
        sources.sort_unstable(); sources.dedup();
        for source in sources { let lock = format!("cowd-runtime-event-stream:{source}"); pg(tx.query_one("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", &[&lock]))?; }
        let stored_preparation = pg(tx.query_opt(&format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 AND idempotency_key=$2"), &[&stream, &prep_key]))?
            .map(|row| row_to_event(&row)).transpose()?.ok_or_else(invalid)?;
        let stored_receipt = load_receipt(&mut tx, &member.preparation.transaction_id, true)?;
        let mut stored_revisions = stored_receipt.stream_revisions.clone();
        let mut provided_revisions = member.preparation_receipt.stream_revisions.clone();
        stored_revisions.sort_by(|a, b| a.stream_id.cmp(&b.stream_id));
        provided_revisions.sort_by(|a, b| a.stream_id.cmp(&b.stream_id));
        if stored_preparation.event_id != member.preparation_receipt.event_ids[0]
            || stored_preparation.transaction_id != member.preparation.transaction_id || stored_preparation.commit_cursor != member.preparation_receipt.commit_cursor
            || stored_preparation.transaction_index != 0 || stored_preparation.schema_version != preparation.schema_version
            || stored_preparation.kind != preparation.event.kind || stored_preparation.scope != preparation.event.scope
            || stored_preparation.actor != preparation.event.actor || stored_preparation.status != preparation.event.status
            || stored_preparation.refs != preparation.event.refs || stored_preparation.payload != preparation.event.payload
            || stored_receipt.request_hash != prep_hash || stored_receipt.commit_cursor != member.preparation_receipt.commit_cursor
            || stored_receipt.event_ids != member.preparation_receipt.event_ids || stored_revisions != provided_revisions {
            return Err(invalid());
        }
        if winner_exists {
            let commit = append_transaction_in_tx_checked(&mut tx, &request.transaction, None, Some((&request, validator)))?;
            if !commit.duplicate || commit.request_hash != checked_hash { return Err(invalid()); }
            let rows = pg(tx.query(&format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE transaction_id=$1 ORDER BY transaction_index ASC"), &[&request.transaction.transaction_id]))?;
            if rows.len() != request.transaction.events.len() || commit.event_ids.len() != rows.len() { return Err(invalid()); }
            for (position, (row, expected)) in rows.iter().zip(&request.transaction.events).enumerate() {
                let actual = row_to_event(row)?;
                if actual.event_id != commit.event_ids[position] || actual.transaction_id != request.transaction.transaction_id
                    || actual.transaction_index as usize != position || actual.commit_cursor != commit.commit_cursor
                    || actual.stream_id != expected.event.stream_id || actual.scope != expected.event.scope
                    || actual.kind != expected.event.kind || actual.status != expected.event.status || actual.actor != expected.event.actor
                    || actual.refs != expected.event.refs || actual.payload != expected.event.payload
                    || actual.schema_version != expected.schema_version || actual.idempotency_key != expected.idempotency_key { return Err(invalid()); }
            }
            let row = pg(tx.query_one("SELECT checked_source_evidence FROM runtime_commits WHERE transaction_id=$1", &[&request.transaction.transaction_id]))?;
            let evidence: Option<Value> = pg(row.try_get(0))?;
            let validation = serde_json::from_value(evidence.ok_or_else(invalid)?)?;
            pg(tx.commit())?;
            return Ok(runtime::OriginalMemberDecision::Committed(CheckedAppendReceipt { commit, validation }));
        }
        let existing = pg(tx.query_opt(&format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 AND idempotency_key=$2"), &[&stream, &member_key]))?
            .map(|row| row_to_event(&row)).transpose()?;
        if terminal_exists {
            let event = existing.ok_or_else(invalid)?;
            let reason = event.payload["noncommit_reason"].as_str().filter(|reason| matches!(*reason, "cancelled" | "rolled_back" | "abandoned")).ok_or_else(invalid)?;
            let error_kind = event.payload["noncommit_error_kind"].as_str().ok_or_else(invalid)?;
            if !matches!((reason, error_kind), ("cancelled", "cancelled") | ("rolled_back", "stale_revision" | "current_member_authority_expired") | ("abandoned", "original_owner_stopped")) { return Err(invalid()); }
            let mut expected_event = member.terminal_event.clone();
            expected_event.event.payload.as_object_mut().ok_or_else(invalid)?.insert("noncommit_reason".into(), Value::String(reason.into()));
            expected_event.event.payload.as_object_mut().ok_or_else(invalid)?.insert("noncommit_error_kind".into(), Value::String(error_kind.into()));
            let receipt = load_receipt(&mut tx, &member.terminal_transaction_id, true)?;
            let source = receipt.stream_revisions.first().filter(|_| receipt.stream_revisions.len() == 1).ok_or_else(invalid)?;
            let terminal_request = AppendTransactionRequest { transaction_id: member.terminal_transaction_id,
                expected_streams: vec![ExpectedStreamRevision { stream_id: stream.clone(), expected_revision: source.expected_revision }],
                events: vec![expected_event.clone()] };
            if event.transaction_id != terminal_request.transaction_id || event.event_id != *receipt.event_ids.first().filter(|_| receipt.event_ids.len() == 1).ok_or_else(invalid)?
                || event.commit_cursor == 0 || event.commit_cursor != receipt.commit_cursor || event.transaction_index != 0
                || event.stream_id != stream || event.sequence != source.committed_revision || source.stream_id != stream
                || source.expected_revision.checked_add(1) != Some(source.committed_revision)
                || event.scope != expected_event.event.scope || event.kind != expected_event.event.kind || event.status != expected_event.event.status
                || event.actor != expected_event.event.actor || event.refs != expected_event.event.refs || event.payload != expected_event.event.payload
                || event.schema_version != expected_event.schema_version || event.idempotency_key != expected_event.idempotency_key
                || receipt.request_hash != request_hash_with_terminal(&terminal_request, None)? { return Err(invalid()); }
            pg(tx.commit())?;
            return Ok(runtime::OriginalMemberDecision::ConfirmedNoncommit { receipt, transaction: terminal_request });
        }
        if existing.is_some() { return Err(invalid()); }
        pg(tx.batch_execute("SAVEPOINT original_member_action"))?;
        let attempted = append_transaction_in_tx_checked(&mut tx, &request.transaction, None, Some((&request, validator)));
        let (reason, error_kind, mut guard, finish_rollback) = match attempted {
            Ok(commit) => {
                if commit.duplicate { return Err(invalid()); }
                let row = pg(tx.query_one("SELECT checked_source_evidence FROM runtime_commits WHERE transaction_id=$1", &[&request.transaction.transaction_id]))?;
                let evidence: Option<Value> = pg(row.try_get(0))?;
                let validation = serde_json::from_value(evidence.ok_or_else(invalid)?)?;
                if !control.begin().map_err(RuntimeEventStoreError::InvalidTransaction)? {
                    let guard = PgOwnedCommitGuard(control, false);
                    pg(tx.batch_execute("ROLLBACK TO SAVEPOINT original_member_action; RELEASE SAVEPOINT original_member_action"))?;
                    ("cancelled", "cancelled", Some(guard), false)
                } else {
                    let mut guard = PgOwnedCommitGuard(control, false);
                    control.before_commit();
                    if control.commit_wins().map_err(RuntimeEventStoreError::InvalidTransaction)? {
                        pg(tx.batch_execute("RELEASE SAVEPOINT original_member_action"))?;
                        pg(tx.commit())?;
                        control.finish(runtime::RuntimeTransactionOutcome::Committed);
                        guard.1 = true;
                        return Ok(runtime::OriginalMemberDecision::Committed(CheckedAppendReceipt { commit, validation }));
                    }
                    pg(tx.batch_execute("ROLLBACK TO SAVEPOINT original_member_action; RELEASE SAVEPOINT original_member_action"))?;
                    ("cancelled", "cancelled", Some(guard), true)
                }
            }
            Err(error @ (RuntimeEventStoreError::StaleRevision { .. } | RuntimeEventStoreError::CurrentMemberAuthorityExpired)) => {
                let kind = match error { RuntimeEventStoreError::StaleRevision { .. } => "stale_revision", RuntimeEventStoreError::CurrentMemberAuthorityExpired => "current_member_authority_expired", _ => unreachable!() };
                let guard = PgOwnedCommitGuard(control, false);
                pg(tx.batch_execute("ROLLBACK TO SAVEPOINT original_member_action; RELEASE SAVEPOINT original_member_action"))?;
                ("rolled_back", kind, Some(guard), true)
            }
            Err(error) => { pg(tx.batch_execute("ROLLBACK TO SAVEPOINT original_member_action; RELEASE SAVEPOINT original_member_action"))?; return Err(error); }
        };
        let mut event = member.terminal_event.clone();
        event.event.payload.as_object_mut().ok_or_else(invalid)?.insert("noncommit_reason".into(), Value::String(reason.into()));
        event.event.payload.as_object_mut().ok_or_else(invalid)?.insert("noncommit_error_kind".into(), Value::String(error_kind.into()));
        let terminal_request = AppendTransactionRequest { transaction_id: member.terminal_transaction_id,
            expected_streams: vec![ExpectedStreamRevision { stream_id: stream.clone(), expected_revision: stream_head(&mut tx, &stream)? }],
            events: vec![event] };
        let receipt = append_transaction_in_tx(&mut tx, &terminal_request, None)?;
        if receipt.duplicate || receipt.request_hash != request_hash_with_terminal(&terminal_request, None)? { return Err(invalid()); }
        pg(tx.commit())?;
        if finish_rollback { control.finish(runtime::RuntimeTransactionOutcome::RolledBack); }
        if let Some(guard) = &mut guard { guard.1 = true; }
        Ok(runtime::OriginalMemberDecision::ConfirmedNoncommit { receipt, transaction: terminal_request })
    }

    fn settle_original_member(
        &self, checked: Option<&CheckedAppendRequest>, member: runtime::OriginalMemberDecisionInput,
        claim: &runtime::RuntimeOriginalSessionClaim<'_>,
    ) -> RuntimeEventStoreResult<runtime::OriginalMemberDecision> {
        let invalid = || RuntimeEventStoreError::InvalidTransaction("original member settlement identity unavailable".into());
        let [preparation] = member.preparation.events.as_slice() else { return Err(invalid()); };
        let pp = &preparation.event.payload;
        let outer = pp["outer_key"].as_str().filter(|v| !v.is_empty()).ok_or_else(invalid)?;
        let index = pp["member_index"].as_u64().ok_or_else(invalid)?;
        let ticket = pp.get("ticket").filter(|v| !v.is_null()).ok_or_else(invalid)?;
        let graph = ticket["graph_id"].as_str().filter(|v| !v.is_empty()).ok_or_else(invalid)?;
        let stream = format!("execution-effect:{outer}");
        let member_key = format!("agentic-member:{index}");
        let prep_key = format!("agentic-member-preparation:{index}");
        let prep_hash = request_hash_with_terminal(&member.preparation, None)?;
        let mut expected_payload = serde_json::json!({"ticket":ticket,"outer_key":outer,"member_index":index,
            "preparation_transaction_id":member.preparation.transaction_id,"preparation_request_hash":prep_hash});
        let terminal_id = if let Some(request) = checked {
            let hash = request.identity_hash()?;
            expected_payload["checked_transaction_id"] = serde_json::json!(request.transaction.transaction_id);
            expected_payload["checked_request_hash"] = serde_json::json!(hash);
            let mut markers = request.transaction.events.iter().filter(|e| e.event.kind == "agentic.batch_member.index");
            let marker = markers.next().ok_or_else(invalid)?;
            if markers.next().is_some() || marker.event.stream_id != stream || marker.idempotency_key.as_deref() != Some(member_key.as_str())
                || marker.event.payload["outer_key"] != outer || marker.event.payload["member_index"] != index
                || marker.event.payload.get("ticket") != Some(ticket)
                || !request.transaction.expected_streams.iter().any(|s| s.stream_id == stream) { return Err(invalid()); }
            let position = marker.event.payload["program_event_index"].as_u64().and_then(|n| usize::try_from(n).ok()).ok_or_else(invalid)?;
            let action = request.transaction.events.get(position).ok_or_else(invalid)?;
            if action.event.payload.pointer("/envelope/actor/session_id").and_then(Value::as_str) != Some(claim.session_id)
                || action.event.payload.pointer("/envelope/actor/turn_id").and_then(Value::as_str) != Some(claim.turn_id) { return Err(invalid()); }
            format!("member-noncommit:{hash}")
        } else {
            expected_payload["decision_identity"] = serde_json::json!("preparation");
            format!("member-preparation-noncommit:{prep_hash}")
        };
        let terminal = &member.terminal_event;
        if member.terminal_transaction_id != terminal_id
            || preparation.event.stream_id != stream || preparation.event.scope != RuntimeEventScope::ExecutionNode
            || preparation.event.kind != "execution.effect.member_preparation" || preparation.event.status.as_deref() != Some("inflight")
            || preparation.event.actor.as_deref() != Some("execution_commit_service") || preparation.schema_version != 1
            || preparation.idempotency_key.as_deref() != Some(prep_key.as_str())
            || member.preparation_receipt.transaction_id != member.preparation.transaction_id
            || member.preparation_receipt.request_hash != prep_hash || member.preparation_receipt.commit_cursor == 0
            || member.preparation_receipt.event_ids.len() != 1
            || terminal.event.stream_id != stream || terminal.event.scope != RuntimeEventScope::ExecutionNode
            || terminal.event.kind != "execution.effect.member_noncommit" || terminal.event.status.as_deref() != Some("not_committed")
            || terminal.event.actor.as_deref() != Some("execution_commit_service") || terminal.event.refs != preparation.event.refs
            || terminal.schema_version != 1 || terminal.idempotency_key.as_deref() != Some(member_key.as_str())
            || pp["session_id"].as_str() != Some(claim.session_id) || pp["turn_id"].as_str() != Some(claim.turn_id)
            || terminal.event.payload != expected_payload { return Err(invalid()); }
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let timeout = checked.map_or(runtime::SourceCheckBudget::default().max_lock_millis, |r| r.budget.max_lock_millis);
        pg(tx.query_one("SELECT set_config('statement_timeout', $1, true)", &[&format!("{timeout}ms")]))?;
        let same_schema: bool = pg(tx.query_one("SELECT COALESCE(to_regclass('session_records') = to_regclass(format('%I.session_records', current_schema())), false) AND COALESCE(to_regclass('session_runtime_outbox') = to_regclass(format('%I.session_runtime_outbox', current_schema())), false) AND COALESCE(to_regclass('runtime_commits') = to_regclass(format('%I.runtime_commits', current_schema())), false) AND COALESCE(to_regclass('runtime_events') = to_regclass(format('%I.runtime_events', current_schema())), false)", &[]))?.try_get(0).map_err(|e| RuntimeEventStoreError::Corrupt(e.to_string()))?;
        if !same_schema { return Err(invalid()); }
        lock_original_member_decisions(&mut tx, &member.preparation)?;
        // Same lock order as checked append: transaction identities, original claim, sources.
        let mut ids = vec![member.terminal_transaction_id.as_str()];
        if let Some(request) = checked { ids.push(&request.transaction.transaction_id); }
        let mut locks = Vec::new();
        for id in ids {
            let name = format!("cowd-runtime-event-transaction:{id}");
            locks.push(pg(tx.query_one("SELECT hashtextextended($1, 0)", &[&name]))?.get::<_, i64>(0));
        }
        locks.sort_unstable(); locks.dedup();
        for lock in locks { pg(tx.query_one("SELECT pg_advisory_xact_lock($1)", &[&lock]))?; }
        // An immutable existing result is read without renewing current admission.
        let prior = pg(tx.query_opt(&format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 AND idempotency_key=$2"), &[&stream, &member_key]))?
            .map(|row| row_to_event(&row)).transpose()?;
        if prior.is_none() {
            session_postgres::lock_runtime_input_claim_with_consumed_tx(&mut tx, claim.session_id, claim.ingress_message_id,
                claim.turn_id, claim.fence, claim.consumed_input_sequence).map_err(|e| RuntimeEventStoreError::InvalidTransaction(e.to_string()))?;
        }
        let mut sources = member.preparation.expected_streams.iter().map(|s| s.stream_id.as_str()).collect::<Vec<_>>();
        sources.push(&stream); sources.push(graph);
        if let Some(request) = checked { sources.extend(request.transaction.expected_streams.iter().map(|s| s.stream_id.as_str())); }
        sources.sort_unstable(); sources.dedup();
        for source in sources {
            let name = format!("cowd-runtime-event-stream:{source}");
            pg(tx.query_one("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", &[&name]))?;
        }
        let stored = pg(tx.query_opt(&format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 AND idempotency_key=$2"), &[&stream, &prep_key]))?
            .map(|row| row_to_event(&row)).transpose()?.ok_or_else(invalid)?;
        let receipt = load_receipt(&mut tx, &member.preparation.transaction_id, true)?;
        let mut actual_sources = receipt.stream_revisions.clone();
        let mut expected_sources = member.preparation_receipt.stream_revisions.clone();
        actual_sources.sort_by(|a,b| a.stream_id.cmp(&b.stream_id)); expected_sources.sort_by(|a,b| a.stream_id.cmp(&b.stream_id));
        if stored.transaction_id != member.preparation.transaction_id || stored.transaction_index != 0
            || stored.commit_cursor != receipt.commit_cursor || receipt.commit_cursor != member.preparation_receipt.commit_cursor
            || receipt.request_hash != prep_hash || receipt.event_ids != member.preparation_receipt.event_ids
            || actual_sources != expected_sources || receipt.event_ids.as_slice() != [stored.event_id.clone()]
            || stored.scope != preparation.event.scope || stored.kind != preparation.event.kind || stored.status != preparation.event.status
            || stored.actor != preparation.event.actor || stored.refs != preparation.event.refs || stored.payload != preparation.event.payload
            || stored.schema_version != preparation.schema_version || stored.idempotency_key != preparation.idempotency_key { return Err(invalid()); }
        let existing = pg(tx.query_opt(&format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 AND idempotency_key=$2"), &[&stream, &member_key]))?
            .map(|row| row_to_event(&row)).transpose()?;
        if let Some(event) = existing {
            if event.kind == "agentic.batch_member.index" {
                // A locator alone cannot authenticate another action. Without the actual
                // retained checked command leave the winner unresolved for its original reader.
                let request = checked.ok_or_else(invalid)?;
                let commit = load_receipt(&mut tx, &event.transaction_id, true)?;
                if event.transaction_id != request.transaction.transaction_id || commit.request_hash != request.identity_hash()?
                    || commit.commit_cursor == 0 || commit.event_ids.len() != request.transaction.events.len() { return Err(invalid()); }
                let rows = pg(tx.query(&format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE transaction_id=$1 ORDER BY transaction_index ASC"), &[&event.transaction_id]))?;
                if rows.len() != request.transaction.events.len() { return Err(invalid()); }
                for (i, (row, expected)) in rows.iter().zip(&request.transaction.events).enumerate() {
                    let actual = row_to_event(row)?;
                    if actual.event_id != commit.event_ids[i] || actual.transaction_index as usize != i
                        || actual.transaction_id != commit.transaction_id || actual.commit_cursor != commit.commit_cursor
                        || actual.stream_id != expected.event.stream_id || actual.scope != expected.event.scope
                        || actual.kind != expected.event.kind || actual.status != expected.event.status || actual.actor != expected.event.actor
                        || actual.refs != expected.event.refs || actual.payload != expected.event.payload
                        || actual.schema_version != expected.schema_version || actual.idempotency_key != expected.idempotency_key { return Err(invalid()); }
                }
                let row = pg(tx.query_one("SELECT checked_source_evidence FROM runtime_commits WHERE transaction_id=$1", &[&event.transaction_id]))?;
                let evidence: Option<Value> = pg(row.try_get(0))?;
                let validation: CheckedSourceEvidence = serde_json::from_value(evidence.ok_or_else(invalid)?)?;
                if validation.frame_digest != request.frame_digest { return Err(invalid()); }
                pg(tx.commit())?;
                return Ok(runtime::OriginalMemberDecision::Committed(CheckedAppendReceipt { commit, validation }));
            }
            let reason = event.payload["noncommit_reason"].as_str().ok_or_else(invalid)?;
            let kind = event.payload["noncommit_error_kind"].as_str().ok_or_else(invalid)?;
            if !matches!((reason, kind), ("cancelled", "cancelled") | ("rolled_back", "stale_revision" | "current_member_authority_expired")
                | ("abandoned", "original_owner_stopped")) { return Err(invalid()); }
            let mut expected = terminal.clone();
            expected.event.payload["noncommit_reason"] = serde_json::json!(reason);
            expected.event.payload["noncommit_error_kind"] = serde_json::json!(kind);
            let receipt = load_receipt(&mut tx, &member.terminal_transaction_id, true)?;
            let [source] = receipt.stream_revisions.as_slice() else { return Err(invalid()); };
            let transaction = AppendTransactionRequest { transaction_id: member.terminal_transaction_id,
                expected_streams: vec![ExpectedStreamRevision { stream_id: stream.clone(), expected_revision: source.expected_revision }], events: vec![expected.clone()] };
            if event.transaction_id != transaction.transaction_id || receipt.transaction_id != transaction.transaction_id
                || receipt.request_hash != request_hash_with_terminal(&transaction, None)? || receipt.commit_cursor == 0
                || event.commit_cursor != receipt.commit_cursor || event.transaction_index != 0
                || receipt.event_ids.as_slice() != [event.event_id.clone()] || source.stream_id != stream
                || event.sequence != source.committed_revision || source.expected_revision.checked_add(1) != Some(source.committed_revision)
                || event.scope != expected.event.scope || event.kind != expected.event.kind || event.status != expected.event.status
                || event.actor != expected.event.actor || event.refs != expected.event.refs || event.payload != expected.event.payload
                || event.schema_version != expected.schema_version || event.idempotency_key != expected.idempotency_key { return Err(invalid()); }
            pg(tx.commit())?;
            return Ok(runtime::OriginalMemberDecision::ConfirmedNoncommit { receipt, transaction });
        }
        // The real source locks plus the unique member key arbitrate with every
        // action/rejection writer. This is a terminal decision, not inferred rollback.
        let mut event = member.terminal_event;
        event.event.payload["noncommit_reason"] = serde_json::json!("abandoned");
        event.event.payload["noncommit_error_kind"] = serde_json::json!("original_owner_stopped");
        let transaction = AppendTransactionRequest { transaction_id: member.terminal_transaction_id,
            expected_streams: vec![ExpectedStreamRevision { stream_id: stream.clone(), expected_revision: stream_head(&mut tx, &stream)? }], events: vec![event] };
        let receipt = append_transaction_in_tx(&mut tx, &transaction, None)?;
        if receipt.duplicate { return Err(invalid()); }
        pg(tx.commit())?;
        Ok(runtime::OriginalMemberDecision::ConfirmedNoncommit { receipt, transaction })
    }

    fn original_checked_terminal_winner(
        &self, request: &CheckedAppendRequest, terminal: &SessionTerminalInput,
        validator: &dyn CheckedSourceValidator,
    ) -> RuntimeEventStoreResult<Option<CheckedAppendReceipt>> {
        if validator.frame_digest() != request.frame_digest { return Err(RuntimeEventStoreError::InvalidTransaction("source frame mismatch".into())); }
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        // Keep the common writer order even if a terminal transaction carries a member.
        lock_original_member_decisions(&mut tx, &request.transaction)?;
        let lock = format!("cowd-runtime-event-transaction:{}", request.transaction.transaction_id);
        pg(tx.query_one("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", &[&lock]))?;
        if pg(tx.query_opt("SELECT 1 FROM runtime_commits WHERE transaction_id=$1", &[&request.transaction.transaction_id]))?.is_none() {
            pg(tx.rollback())?;
            return Ok(None);
        }
        // The original helper checks checked identity AND the terminal outbox
        // content on duplicate; the terminal is not in checked identity_hash.
        let commit = append_transaction_in_tx_checked(&mut tx, &request.transaction, Some(terminal), Some((request, validator)))?;
        if !commit.duplicate { return Err(RuntimeEventStoreError::Corrupt("terminal winner was not duplicate".into())); }
        let row = pg(tx.query_one("SELECT checked_source_evidence FROM runtime_commits WHERE transaction_id=$1", &[&request.transaction.transaction_id]))?;
        let evidence: Option<Value> = pg(row.try_get(0))?;
        let validation = serde_json::from_value(evidence.ok_or_else(|| RuntimeEventStoreError::Corrupt("checked terminal evidence missing".into()))?)?;
        pg(tx.commit())?;
        Ok(Some(CheckedAppendReceipt { commit, validation }))
    }

    fn append_checked_terminal_cancellable(
        &self, request: CheckedAppendRequest, terminal: SessionTerminalInput,
        validator: &dyn CheckedSourceValidator, control: &dyn runtime::RuntimeTransactionCommitControl,
        claim: &runtime::RuntimeOriginalSessionClaim<'_>,
    ) -> RuntimeEventStoreResult<Option<CheckedAppendReceipt>> {
        self.append_checked_owned_tx(request, Some(&terminal), validator, control, claim)
    }

    fn append_transaction_with_terminal(
        &self,
        request: AppendTransactionRequest,
        terminal: SessionTerminalInput,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let receipt = append_transaction_in_tx(&mut tx, &request, Some(&terminal))?;
        pg(tx.commit())?;
        Ok(receipt)
    }

    fn consume_verified_decision_lease(
        &self,
        lease_id: &str,
        principal_id: &str,
        review_id: &str,
        action: &str,
        scope: &str,
        evidence_digest: &str,
        credential_epoch: u64,
        consumed_at_ms: u64,
    ) -> RuntimeEventStoreResult<()> {
        validate_decision_lease_claims(
            lease_id,
            principal_id,
            review_id,
            action,
            scope,
            evidence_digest,
        )?;
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let inserted = pg(tx.execute(
            "INSERT INTO runtime_consumed_decision_leases
             (lease_id, principal_id, review_id, action, scope, evidence_digest, credential_epoch, consumed_at_ms)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(lease_id) DO NOTHING",
            &[
                &lease_id,
                &principal_id,
                &review_id,
                &action,
                &scope,
                &evidence_digest,
                &to_i64(credential_epoch, "credential_epoch")?,
                &to_i64(consumed_at_ms, "consumed_at_ms")?,
            ],
        ))?;
        if inserted == 0 {
            return Err(RuntimeEventStoreError::DecisionLeaseAlreadyConsumed {
                lease_id: lease_id.to_string(),
            });
        }
        pg(tx.commit())?;
        Ok(())
    }

    fn append_transaction_with_verified_decision_lease(
        &self,
        request: AppendTransactionRequest,
        lease: &VerifiedDecisionLease,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        validate_decision_lease_claims(
            lease.lease_id(),
            lease.principal_id(),
            lease.review_id(),
            lease.action(),
            lease.scope(),
            lease.evidence_digest(),
        )?;
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let receipt = append_transaction_in_tx(&mut tx, &request, None)?;
        let inserted = pg(tx.execute(
            "INSERT INTO runtime_consumed_decision_leases
             (lease_id, principal_id, review_id, action, scope, evidence_digest, credential_epoch, consumed_at_ms)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(lease_id) DO NOTHING",
            &[
                &lease.lease_id(),
                &lease.principal_id(),
                &lease.review_id(),
                &lease.action(),
                &lease.scope(),
                &lease.evidence_digest(),
                &to_i64(lease.credential_epoch(), "credential_epoch")?,
                &to_i64(now_ms(), "consumed_at_ms")?,
            ],
        ))?;
        if inserted == 0 {
            let existing = pg(tx.query_one(
                "SELECT principal_id, review_id, action, scope, evidence_digest, credential_epoch
                   FROM runtime_consumed_decision_leases WHERE lease_id=$1",
                &[&lease.lease_id()],
            ))?;
            let existing_epoch: i64 = pg(existing.try_get(5))?;
            let matches = pg(existing.try_get::<_, String>(0))? == lease.principal_id()
                && pg(existing.try_get::<_, String>(1))? == lease.review_id()
                && pg(existing.try_get::<_, String>(2))? == lease.action()
                && pg(existing.try_get::<_, String>(3))? == lease.scope()
                && pg(existing.try_get::<_, String>(4))? == lease.evidence_digest()
                && existing_epoch == to_i64(lease.credential_epoch(), "credential_epoch")?;
            if !receipt.duplicate || !matches {
                return Err(RuntimeEventStoreError::DecisionLeaseAlreadyConsumed {
                    lease_id: lease.lease_id().to_string(),
                });
            }
        }
        pg(tx.commit())?;
        Ok(receipt)
    }

    fn append_batch_if_revision(
        &self,
        stream_id: String,
        expected_revision: u64,
        transaction_id: String,
        events: Vec<RuntimeTransactionEventInput>,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        if events
            .iter()
            .any(|event| event.event.stream_id != stream_id)
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "single-stream batch contains an event for another stream".to_string(),
            ));
        }
        self.append_transaction(AppendTransactionRequest {
            transaction_id,
            expected_streams: vec![ExpectedStreamRevision {
                stream_id,
                expected_revision,
            }],
            events,
        })
    }

    fn events_after_cursor(
        &self,
        cursor: u64,
        max_commits: usize,
    ) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>> {
        if max_commits == 0 {
            return Ok(Vec::new());
        }
        let cursor = to_i64(cursor, "cursor")?;
        let limit = to_i64(max_commits.min(100) as u64, "max_commits")?;
        let mut connection = self.checkout_event_read()?;
        let metadata = pg(connection.query(
            "SELECT selected.commit_cursor, COALESCE(SUM(event.encoded_bytes),0)::BIGINT
               FROM (SELECT commit_cursor FROM runtime_commits WHERE commit_cursor>$1 ORDER BY commit_cursor LIMIT $2) selected
               LEFT JOIN runtime_events event ON event.commit_cursor=selected.commit_cursor
              GROUP BY selected.commit_cursor ORDER BY selected.commit_cursor",
            &[&cursor, &limit],
        ))?;
        let mut selected = Vec::<i64>::new();
        let mut total = 0usize;
        for row in metadata {
            let position: i64 = pg(row.try_get(0))?;
            let bytes = usize::try_from(pg(row.try_get::<_, i64>(1))?).map_err(|_| {
                RuntimeEventStoreError::Corrupt("invalid transaction metadata bytes".into())
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
            selected.push(position);
        }
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        let events = rows_to_events(pg(connection.query(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE commit_cursor=ANY($1) ORDER BY commit_cursor,transaction_index"),
            &[&selected],
        ))?)?;
        let actual = events.iter().try_fold(0usize, |total, event| {
            let bytes = serde_json::to_vec(event)?.len();
            total
                .checked_add(bytes)
                .ok_or_else(|| RuntimeEventStoreError::Corrupt("transaction bytes overflow".into()))
        })?;
        if actual != total {
            return Err(RuntimeEventStoreError::Corrupt(
                "transaction metadata bytes mismatch".into(),
            ));
        }
        Ok(group_committed_events(events))
    }

    fn projection_scan_page(
        &self,
        cursor: u64,
        interest: &RuntimeProjectionInterest,
        max_commits: usize,
        max_events: usize,
        max_bytes: usize,
    ) -> RuntimeEventStoreResult<RuntimeProjectionScanPage> {
        let cursor_sql = to_i64(cursor, "cursor")?;
        if max_commits == 0 {
            return Ok(RuntimeProjectionScanPage {
                scanned_through_cursor: cursor,
                ..RuntimeProjectionScanPage::default()
            });
        }
        let mut connection = self.checkout_event_read()?;
        let selected_rows = pg(connection.query(
            "SELECT commit_cursor, transaction_id FROM runtime_commits WHERE commit_cursor>$1 ORDER BY commit_cursor LIMIT $2",
            &[&cursor_sql, &to_i64(max_commits.min(100) as u64, "max_commits")?],
        ))?;
        let mut selected = selected_rows
            .iter()
            .map(|row| {
                Ok((
                    from_i64(pg(row.try_get(0))?, "commit_cursor")?,
                    pg(row.try_get(1))?,
                ))
            })
            .collect::<RuntimeEventStoreResult<Vec<(u64, String)>>>()?;
        let Some((highwater, _)) = selected.last() else {
            return Ok(RuntimeProjectionScanPage {
                scanned_through_cursor: cursor,
                ..RuntimeProjectionScanPage::default()
            });
        };
        let interest_json = Value::Array(
            interest
                .events
                .iter()
                .map(|event| serde_json::json!({"scope":event.scope.as_str(),"kind":event.kind}))
                .collect(),
        );
        let predicate = "commit_cursor>$1 AND commit_cursor<=$2 AND EXISTS(SELECT 1 FROM jsonb_array_elements($3::jsonb) wanted WHERE event.scope=wanted->>'scope' AND event.kind=wanted->>'kind')";
        let metadata = pg(connection.query(
            &format!("SELECT commit_cursor,COUNT(*)::BIGINT,SUM(encoded_bytes)::BIGINT FROM runtime_events event WHERE {predicate} GROUP BY commit_cursor ORDER BY commit_cursor"),
            &[&cursor_sql,&to_i64(*highwater,"highwater")?,&interest_json],
        ))?;
        let mut sizes = BTreeMap::new();
        for row in metadata {
            let position = from_i64(pg(row.try_get(0))?, "commit_cursor")?;
            let count = usize::try_from(pg(row.try_get::<_, i64>(1))?).map_err(|_| {
                RuntimeEventStoreError::Corrupt("invalid matched event count".into())
            })?;
            let bytes = usize::try_from(pg(row.try_get::<_, i64>(2))?).map_err(|_| {
                RuntimeEventStoreError::Corrupt("invalid matched event bytes".into())
            })?;
            sizes.insert(position, (count, bytes));
        }
        let mut count = 0usize;
        let mut bytes = 0usize;
        let mut accepted = 0usize;
        for (position, _) in &selected {
            let (additional_count, additional_bytes) =
                sizes.get(position).copied().unwrap_or_default();
            if additional_count > max_events.min(100).saturating_sub(count)
                || additional_bytes > max_bytes.min(1024 * 1024).saturating_sub(bytes)
            {
                if count == 0 {
                    return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
                        "projection transaction".into(),
                    ));
                }
                break;
            }
            count += additional_count;
            bytes += additional_bytes;
            accepted += 1;
        }
        selected.truncate(accepted);
        let through = selected.last().map_or(cursor, |(position, _)| *position);
        let events = if count == 0 {
            Vec::new()
        } else {
            rows_to_events(pg(connection.query(
                &format!("SELECT {EVENT_COLUMNS} FROM runtime_events event WHERE {predicate} ORDER BY commit_cursor,transaction_index"),
                &[&cursor_sql,&to_i64(through,"through")?,&interest_json],
            ))?)?
        };
        let actual = events.iter().try_fold(0usize, |sum, event| {
            sum.checked_add(serde_json::to_vec(event)?.len())
                .ok_or_else(|| RuntimeEventStoreError::Corrupt("projection bytes overflow".into()))
        })?;
        if actual != bytes || events.len() != count {
            return Err(RuntimeEventStoreError::Corrupt(
                "projection metadata bytes mismatch".into(),
            ));
        }
        Ok(build_projection_scan_page(cursor, selected, events))
    }

    fn projection_checkpoint(
        &self,
        projection_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeProjectionCheckpoint>> {
        validate_projection_id(projection_id)?;
        let mut connection = self.checkout_event_read()?;
        pg(connection.query_opt(
            "SELECT projection_id, source_cursor, revision, payload, updated_at_ms
               FROM runtime_projection_checkpoints WHERE projection_id=$1",
            &[&projection_id],
        ))?
        .map(|row| row_to_projection_checkpoint(&row))
        .transpose()
    }

    fn projection_checkpoints_with_prefix(
        &self,
        prefix: &str,
    ) -> RuntimeEventStoreResult<Vec<RuntimeProjectionCheckpoint>> {
        if prefix.trim().is_empty() {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "projection checkpoint prefix must not be empty".to_string(),
            ));
        }
        let escaped = prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("{escaped}%");
        let mut connection = self.checkout_event_read()?;
        pg(connection.query(
            "SELECT projection_id, source_cursor, revision, payload, updated_at_ms
               FROM runtime_projection_checkpoints
              WHERE projection_id LIKE $1 ESCAPE '\\'
              ORDER BY projection_id ASC",
            &[&pattern],
        ))?
        .iter()
        .map(row_to_projection_checkpoint)
        .collect()
    }

    fn put_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        payload: &Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        validate_projection_id(projection_id)?;
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let lock_key = format!("cowd-runtime-projection:{projection_id}");
        pg(tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&lock_key],
        ))?;
        let current = pg(tx.query_opt(
            "SELECT projection_id, source_cursor, revision, payload, updated_at_ms
               FROM runtime_projection_checkpoints
              WHERE projection_id=$1 FOR UPDATE",
            &[&projection_id],
        ))?
        .map(|row| row_to_projection_checkpoint(&row))
        .transpose()?;
        if let Some(current) = current {
            if source_cursor < current.source_cursor {
                return Err(RuntimeEventStoreError::StaleRevision {
                    stream_id: format!("projection:{projection_id}"),
                    expected: source_cursor,
                    actual: current.source_cursor,
                });
            }
            if source_cursor == current.source_cursor {
                if current.payload == *payload {
                    return Ok(current);
                }
                return Err(RuntimeEventStoreError::TransactionConflict {
                    transaction_id: format!("projection:{projection_id}:{source_cursor}"),
                });
            }
            let row = pg(tx.query_one(
                "UPDATE runtime_projection_checkpoints
                    SET source_cursor=$1, revision=revision+1, payload=$2, updated_at_ms=$3
                  WHERE projection_id=$4
              RETURNING projection_id, source_cursor, revision, payload, updated_at_ms",
                &[
                    &to_i64(source_cursor, "source_cursor")?,
                    payload,
                    &to_i64(updated_at_ms, "updated_at_ms")?,
                    &projection_id,
                ],
            ))?;
            let checkpoint = row_to_projection_checkpoint(&row)?;
            pg(tx.commit())?;
            return Ok(checkpoint);
        }
        let row = pg(tx.query_one(
            "INSERT INTO runtime_projection_checkpoints
                (projection_id, source_cursor, revision, payload, updated_at_ms)
             VALUES ($1,$2,1,$3,$4)
             RETURNING projection_id, source_cursor, revision, payload, updated_at_ms",
            &[
                &projection_id,
                &to_i64(source_cursor, "source_cursor")?,
                payload,
                &to_i64(updated_at_ms, "updated_at_ms")?,
            ],
        ))?;
        let checkpoint = row_to_projection_checkpoint(&row)?;
        pg(tx.commit())?;
        Ok(checkpoint)
    }

    fn source_mutation_fence(&self) -> RuntimeEventStoreResult<RuntimeSourceMutationFence> {
        let mut connection = self.checkout_event_read()?;
        let row = pg(connection.query_one("SELECT incarnation,mutation_epoch FROM runtime_event_source_fence WHERE singleton=TRUE", &[]))?;
        Ok(RuntimeSourceMutationFence {
            incarnation: pg(row.try_get(0))?,
            mutation_epoch: from_i64(pg(row.try_get(1))?, "source mutation epoch")?,
        })
    }

    fn compare_and_put_projection_checkpoint_fenced(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &Value,
        updated_at_ms: u64,
        source_fence: &RuntimeSourceMutationFence,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        self.put_source_checkpoint(projection_id, source_cursor, expected_revision, payload, updated_at_ms, Some(source_fence))
    }

    fn compare_and_put_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        self.put_source_checkpoint(projection_id, source_cursor, expected_revision, payload, updated_at_ms, None)
    }
    fn compare_and_repair_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        validate_projection_id(projection_id)?;
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let lock_key = format!("cowd-runtime-projection:{projection_id}");
        pg(tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&lock_key],
        ))?;
        let row = pg(tx.query_opt(
            "UPDATE runtime_projection_checkpoints
                SET source_cursor=$1, revision=revision+1, payload=$2, updated_at_ms=$3
              WHERE projection_id=$4 AND revision=$5
          RETURNING projection_id, source_cursor, revision, payload, updated_at_ms",
            &[
                &to_i64(source_cursor, "source_cursor")?,
                payload,
                &to_i64(updated_at_ms, "updated_at_ms")?,
                &projection_id,
                &to_i64(expected_revision, "expected_revision")?,
            ],
        ))?;
        let Some(row) = row else {
            let actual = pg(tx.query_opt(
                "SELECT revision FROM runtime_projection_checkpoints WHERE projection_id=$1",
                &[&projection_id],
            ))?
            .map_or(0, |row| row.get::<_, i64>(0) as u64);
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: format!("projection:{projection_id}"),
                expected: expected_revision,
                actual,
            });
        };
        let checkpoint = row_to_projection_checkpoint(&row)?;
        pg(tx.commit())?;
        Ok(checkpoint)
    }

    fn delete_projection_checkpoint(&self, projection_id: &str) -> RuntimeEventStoreResult<bool> {
        validate_projection_id(projection_id)?;
        let mut connection = self.checkout_event_write()?;
        Ok(pg(connection.execute(
            "DELETE FROM runtime_projection_checkpoints WHERE projection_id=$1",
            &[&projection_id],
        ))? > 0)
    }

    fn event_by_idempotency_key(
        &self,
        stream_id: &str,
        idempotency_key: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>> {
        let mut connection = self.checkout_event_read()?;
        pg(connection.query_opt(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 AND idempotency_key=$2"),
            &[&stream_id, &idempotency_key],
        ))?
        .map(|row| row_to_event(&row))
        .transpose()
    }

    fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64> {
        let mut connection = self.checkout_event_read()?;
        stream_head(&mut connection, stream_id)
    }

    fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        pg(connection.query(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 ORDER BY sequence ASC"),
            &[&stream_id],
        ))
        .and_then(rows_to_events)
        .map_err(|error| error.to_string())
    }

    fn list_stream_after(
        &self,
        stream_id: &str,
        after_sequence: u64,
        through_sequence: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.sequence_page(
            stream_id,
            after_sequence,
            through_sequence,
            max_events,
            max_bytes,
            false,
        )
    }

    fn list_stream_before(
        &self,
        stream_id: &str,
        through_sequence: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.sequence_page(stream_id, 0, through_sequence, max_events, max_bytes, true)
    }

    fn count_stream_kinds(
        &self,
        stream_id: &str,
        kinds: &[&str],
        through_sequence: u64,
    ) -> Result<u64, String> {
        let through_sequence =
            to_i64(through_sequence, "through_sequence").map_err(|error| error.to_string())?;
        let kinds = kinds.iter().copied().collect::<BTreeSet<_>>();
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let mut total = 0u64;
        for kind in kinds {
            let row = connection.query_one("SELECT COUNT(*) FROM runtime_events WHERE stream_id=$1 AND kind=$2 AND sequence<=$3", &[&stream_id, &kind, &through_sequence])
                .map_err(|_| "runtime event count failed".to_string())?;
            let count: i64 = row
                .try_get(0)
                .map_err(|_| "runtime event count is corrupt".to_string())?;
            let count =
                u64::try_from(count).map_err(|_| "runtime event count overflow".to_string())?;
            total = total
                .checked_add(count)
                .ok_or_else(|| "runtime event count overflow".to_string())?;
        }
        Ok(total)
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
    fn stream_event_count(&self, stream_id: &str) -> Result<usize, String> {
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let count: i64 = pg(connection.query_one(
            "SELECT COUNT(*) FROM runtime_events WHERE stream_id=$1",
            &[&stream_id],
        ))
        .and_then(|row| pg(row.try_get(0)))
        .map_err(|error| error.to_string())?;
        usize::try_from(count).map_err(|_| "runtime stream event count overflow".to_string())
    }

    fn execution_events_for_session(
        &self,
        session_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if session_id.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let ref_filter = serde_json::json!([{"kind": "session", "id": session_id}]);
        let direct_refs = {
            let mut connection = self
                .checkout_event_read()
                .map_err(|error| error.to_string())?;
            let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
            let rows = match after_position {
                Some((cursor, transaction_index)) => pg(connection.query(
                    &format!(
                        "SELECT {EVENT_COLUMNS} FROM runtime_events
                         WHERE refs @> $1
                           AND (commit_cursor > $2
                                OR (commit_cursor = $2 AND transaction_index > $3))
                         ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $4"
                    ),
                    &[
                        &ref_filter,
                        &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                        &i64::from(transaction_index),
                        &limit,
                    ],
                )),
                None => pg(connection.query(
                    &format!(
                        "SELECT {EVENT_COLUMNS} FROM runtime_events
                         WHERE refs @> $1
                         ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $2"
                    ),
                    &[&ref_filter, &limit],
                )),
            }
            .and_then(rows_to_events)
            .map_err(|error| error.to_string())?;
            rows
        };
        let terminal_requests = {
            let mut connection = self
                .checkout_event_read()
                .map_err(|error| error.to_string())?;
            let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
            let event_kind = "runtime.session.terminal_requested";
            let rows = match after_position {
                Some((cursor, transaction_index)) => pg(connection.query(
                    &format!(
                        "SELECT {EVENT_COLUMNS} FROM runtime_events
                         WHERE refs @> $1 AND kind = $2
                           AND (commit_cursor > $3
                                OR (commit_cursor = $3 AND transaction_index > $4))
                         ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $5"
                    ),
                    &[
                        &ref_filter,
                        &event_kind,
                        &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                        &i64::from(transaction_index),
                        &limit,
                    ],
                )),
                None => pg(connection.query(
                    &format!(
                        "SELECT {EVENT_COLUMNS} FROM runtime_events
                         WHERE refs @> $1 AND kind = $2
                         ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $3"
                    ),
                    &[&ref_filter, &event_kind, &limit],
                )),
            };
            rows.and_then(rows_to_events)
                .map_err(|error| error.to_string())?
        };
        let graph_ids = terminal_requests
            .iter()
            .flat_map(|event| event.refs.iter())
            .filter(|reference| reference.kind == "execution_graph")
            .map(|reference| reference.id.clone())
            .collect::<BTreeSet<_>>();
        let mut related = direct_refs;
        related.extend(terminal_requests);
        let mut pending = graph_ids.into_iter().collect::<VecDeque<_>>();
        let mut visited = BTreeSet::new();
        while let Some(graph_id) = pending.pop_front() {
            if visited.len() >= limit || !visited.insert(graph_id.clone()) {
                continue;
            }
            related.extend(RuntimeEventStoreBackend::list_stream(self, &graph_id)?);
            let lineage_stream = format!("execution-lineage:{graph_id}");
            let lineage_events = RuntimeEventStoreBackend::list_stream(self, &lineage_stream)?;
            for event in &lineage_events {
                if event.kind != "execution.lineage.child_registered.v1" {
                    continue;
                }
                if let Some(child_id) = event
                    .payload
                    .get("child_execution_id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                {
                    pending.push_back(child_id.to_string());
                }
            }
            related.extend(lineage_events);
        }
        related.sort_by_key(|event| (event.commit_cursor, event.transaction_index));
        related.dedup_by(|left, right| left.event_id == right.event_id);
        Ok(related
            .into_iter()
            .filter(|event| {
                after_position.is_none_or(|position| {
                    (event.commit_cursor, event.transaction_index) > position
                })
            })
            .take(limit)
            .collect())
    }

    fn events_for_root_execution(
        &self,
        root_execution_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if root_execution_id.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
        let rows = match after_position {
            Some((cursor, transaction_index)) => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE root_execution_id=$1
                       AND (commit_cursor > $2
                            OR (commit_cursor = $2 AND transaction_index > $3))
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $4"
                ),
                &[
                    &root_execution_id,
                    &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                    &i64::from(transaction_index),
                    &limit,
                ],
            )),
            None => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE root_execution_id=$1
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $2"
                ),
                &[&root_execution_id, &limit],
            )),
        };
        rows.and_then(rows_to_events)
            .map_err(|error| error.to_string())
    }

    fn events_for_root_execution_kind(
        &self,
        root_execution_id: &str,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if root_execution_id.trim().is_empty() || kind.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
        let rows = match after_position {
            Some((cursor, transaction_index)) => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE root_execution_id=$1 AND kind=$2
                       AND (commit_cursor > $3
                            OR (commit_cursor = $3 AND transaction_index > $4))
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $5"
                ),
                &[
                    &root_execution_id,
                    &kind,
                    &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                    &i64::from(transaction_index),
                    &limit,
                ],
            )),
            None => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE root_execution_id=$1 AND kind=$2
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $3"
                ),
                &[&root_execution_id, &kind, &limit],
            )),
        };
        rows.and_then(rows_to_events)
            .map_err(|error| error.to_string())
    }

    fn events_for_activity(
        &self,
        activity_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if activity_id.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
        let rows = match after_position {
            Some((cursor, transaction_index)) => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE activity_id=$1
                       AND (commit_cursor > $2
                            OR (commit_cursor = $2 AND transaction_index > $3))
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $4"
                ),
                &[
                    &activity_id,
                    &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                    &i64::from(transaction_index),
                    &limit,
                ],
            )),
            None => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE activity_id=$1
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $2"
                ),
                &[&activity_id, &limit],
            )),
        };
        rows.and_then(rows_to_events)
            .map_err(|error| error.to_string())
    }

    fn list_scope(
        &self,
        scope: RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        pg(connection.query(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE scope=$1 ORDER BY commit_cursor DESC, transaction_index DESC LIMIT $2"),
            &[&scope.as_str(), &to_i64(limit as u64, "limit").map_err(|error| error.to_string())?],
        ))
        .and_then(rows_to_events)
        .map_err(|error| error.to_string())
    }

    fn list_scope_page_asc(
        &self,
        scope: RuntimeEventScope,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
        let rows = match after_position {
            Some((cursor, transaction_index)) => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE scope=$1
                       AND (commit_cursor > $2
                            OR (commit_cursor = $2 AND transaction_index > $3))
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $4"
                ),
                &[
                    &scope.as_str(),
                    &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                    &i64::from(transaction_index),
                    &limit,
                ],
            )),
            None => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events WHERE scope=$1
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $2"
                ),
                &[&scope.as_str(), &limit],
            )),
        };
        rows.and_then(rows_to_events)
            .map_err(|error| error.to_string())
    }

    fn list_scope_stream_prefix_page_asc(
        &self,
        scope: RuntimeEventScope,
        stream_prefix: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if stream_prefix.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
        let rows = match after_position {
            Some((cursor, transaction_index)) => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE scope=$1 AND starts_with(stream_id, $2)
                       AND (commit_cursor > $3
                            OR (commit_cursor = $3 AND transaction_index > $4))
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $5"
                ),
                &[
                    &scope.as_str(),
                    &stream_prefix,
                    &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                    &i64::from(transaction_index),
                    &limit,
                ],
            )),
            None => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE scope=$1 AND starts_with(stream_id, $2)
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $3"
                ),
                &[&scope.as_str(), &stream_prefix, &limit],
            )),
        };
        rows.and_then(rows_to_events)
            .map_err(|error| error.to_string())
    }

    fn list_scope_kind_page_asc(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
        let rows = match after_position {
            Some((cursor, transaction_index)) => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE scope=$1 AND kind=$2
                       AND (commit_cursor > $3
                            OR (commit_cursor = $3 AND transaction_index > $4))
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $5"
                ),
                &[
                    &scope.as_str(),
                    &kind,
                    &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                    &i64::from(transaction_index),
                    &limit,
                ],
            )),
            None => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE scope=$1 AND kind=$2
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $3"
                ),
                &[&scope.as_str(), &kind, &limit],
            )),
        };
        rows.and_then(rows_to_events)
            .map_err(|error| error.to_string())
    }

    fn stream_ids_for_scope(
        &self,
        scope: RuntimeEventScope,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        let mut connection = self.checkout_event_read()?;
        let rows = pg(connection.query(
            "SELECT stream_id FROM runtime_events WHERE scope=$1
             GROUP BY stream_id ORDER BY MAX(commit_cursor) ASC, stream_id ASC",
            &[&scope.as_str()],
        ))?;
        rows.into_iter().map(|row| pg(row.try_get(0))).collect()
    }

    fn stream_ids_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        let sequence = to_i64(sequence, "runtime event sequence")?;
        let mut connection = self.checkout_event_read()?;
        let rows = pg(connection.query(
            "SELECT stream_id FROM runtime_events
             WHERE scope=$1 AND kind=$2 AND sequence=$3
             ORDER BY commit_cursor ASC, stream_id ASC",
            &[&scope.as_str(), &kind, &sequence],
        ))?;
        rows.into_iter().map(|row| pg(row.try_get(0))).collect()
    }

    fn stream_ids_for_scope_kind_at_sequence_page(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
        after: Option<(u64, String)>,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<(String, u64)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let sequence = to_i64(sequence, "runtime event sequence")?;
        let limit = to_i64(limit as u64, "stream page limit")?;
        let mut connection = self.checkout_event_read()?;
        let rows = match after {
            Some((after_cursor, after_stream_id)) => pg(connection.query(
                "SELECT stream_id, commit_cursor FROM runtime_events
                 WHERE scope=$1 AND kind=$2 AND sequence=$3
                   AND (commit_cursor > $4 OR (commit_cursor = $4 AND stream_id > $5))
                 ORDER BY commit_cursor ASC, stream_id ASC LIMIT $6",
                &[
                    &scope.as_str(),
                    &kind,
                    &sequence,
                    &to_i64(after_cursor, "stream page cursor")?,
                    &after_stream_id,
                    &limit,
                ],
            )),
            None => pg(connection.query(
                "SELECT stream_id, commit_cursor FROM runtime_events
                 WHERE scope=$1 AND kind=$2 AND sequence=$3
                 ORDER BY commit_cursor ASC, stream_id ASC LIMIT $4",
                &[&scope.as_str(), &kind, &sequence, &limit],
            )),
        }?;
        rows.into_iter()
            .map(|row| {
                let stream_id = pg(row.try_get::<_, String>(0))?;
                let commit_cursor = pg(row.try_get::<_, i64>(1))?;
                Ok((stream_id, commit_cursor.max(0) as u64))
            })
            .collect()
    }

    fn latest_stream_statuses_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<(String, Option<String>)>> {
        let sequence = to_i64(sequence, "runtime event sequence")?;
        let mut connection = self.checkout_event_read()?;
        let rows = pg(connection.query(
            "WITH candidates AS (
                 SELECT stream_id FROM runtime_events
                  WHERE scope=$1 AND kind=$2 AND sequence=$3
             )
             SELECT stream_id, status
               FROM (
                   SELECT DISTINCT ON (event.stream_id)
                          event.stream_id, event.status, event.sequence
                     FROM runtime_events AS event
                     JOIN candidates USING(stream_id)
                    ORDER BY event.stream_id, event.sequence DESC
               ) AS latest
              ORDER BY stream_id ASC",
            &[&scope.as_str(), &kind, &sequence],
        ))?;
        rows.into_iter()
            .map(|row| Ok((pg(row.try_get(0))?, pg(row.try_get(1))?)))
            .collect()
    }

    fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        pg(connection.query(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events ORDER BY commit_cursor DESC, transaction_index DESC LIMIT $1"),
            &[&to_i64(limit as u64, "limit").map_err(|error| error.to_string())?],
        ))
        .and_then(rows_to_events)
        .map_err(|error| error.to_string())
    }

    fn latest_for_stream(&self, stream_id: &str) -> Result<Option<DurableRuntimeEvent>, String> {
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        pg(connection.query_opt(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 ORDER BY sequence DESC LIMIT 1"),
            &[&stream_id],
        ))
        .and_then(|row| row.map(|row| row_to_event(&row)).transpose())
        .map_err(|error| error.to_string())
    }

    fn latest_for_stream_kind(
        &self,
        stream_id: &str,
        kind: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        pg(connection.query_opt(
            &format!(
                "SELECT {EVENT_COLUMNS} FROM runtime_events
                  WHERE stream_id=$1 AND kind=$2
                  ORDER BY sequence DESC LIMIT 1"
            ),
            &[&stream_id, &kind],
        ))
        .and_then(|row| row.map(|row| row_to_event(&row)).transpose())
        .map_err(|error| error.to_string())
    }

    fn enqueue_session_terminal(
        &self,
        terminal_id: &str,
        message_id: &str,
        session_id: &str,
        commit_cursor: u64,
        payload_ref: &str,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        let _ = (
            terminal_id,
            message_id,
            session_id,
            commit_cursor,
            payload_ref,
        );
        Err(RuntimeEventStoreError::InvalidTransaction(
            "unfenced terminal enqueue is test-only; use append_transaction_with_terminal"
                .to_string(),
        ))
    }

    fn claim_session_terminals(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let expires = now_ms.saturating_add(lease_ms);
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let rows = pg(tx.query(
            "WITH candidates AS (
                SELECT terminal_id FROM runtime_session_outbox
                 WHERE ((status IN ('pending','retry_scheduled') AND COALESCE(next_attempt_at, 0) <= $1)
                    OR (status='claimed' AND claim_expires_at <= $1))
                 ORDER BY commit_cursor, terminal_id
                 FOR UPDATE SKIP LOCKED LIMIT $2
             ), claimed AS (
                UPDATE runtime_session_outbox outbox
                   SET status='claimed', attempts=outbox.attempts+1,
                       claim_owner=$3, claim_expires_at=$4, revision=outbox.revision+1
                  FROM candidates
                 WHERE outbox.terminal_id=candidates.terminal_id
                RETURNING outbox.terminal_id, outbox.message_id, outbox.session_id,
                    outbox.commit_cursor, outbox.payload_ref, outbox.execution_id, outbox.turn_id,
                    outbox.request_id, outbox.session_generation, outbox.input_sequence, outbox.input_claim_owner,
                    outbox.input_claim_token, outbox.input_claim_revision,
                    outbox.status, outbox.attempts, outbox.next_attempt_at, outbox.claim_owner,
                    outbox.claim_expires_at, outbox.failure_class, outbox.last_error,
                    outbox.materialized_at, outbox.revision
             ) SELECT * FROM claimed ORDER BY commit_cursor, terminal_id",
            &[
                &to_i64(now_ms, "now_ms")?,
                &to_i64(limit as u64, "limit")?,
                &worker_id,
                &to_i64(expires, "claim_expires_at")?,
            ],
        ))?;
        let records = rows
            .iter()
            .map(row_to_runtime_session_outbox)
            .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
        pg(tx.commit())?;
        Ok(records)
    }

    fn session_terminal(
        &self,
        terminal_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
        let mut connection = self.checkout_event_read()?;
        query_runtime_session_outbox(&mut connection, terminal_id)
    }

    fn has_unsettled_session_terminals(&self, session_id: &str) -> RuntimeEventStoreResult<bool> {
        let mut connection = self.checkout_event_read()?;
        let row = pg(connection.query_one(
            "SELECT EXISTS(
                 SELECT 1 FROM runtime_session_outbox
                  WHERE session_id=$1 AND status NOT IN ('materialized','suppressed')
             )",
            &[&session_id],
        ))?;
        pg(row.try_get(0))
    }

    fn session_terminal_for_execution(
        &self,
        session_id: &str,
        execution_id: &str,
        turn_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
        RuntimeEventStore::validate_terminal_execution_selector(session_id, execution_id, turn_id)?;
        let mut connection = self.checkout_event_read()?;
        let rows = pg(connection.query(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                    request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                    input_claim_revision, status, attempts, next_attempt_at, claim_owner, claim_expires_at,
                    failure_class, last_error, materialized_at, revision
               FROM runtime_session_outbox
              WHERE session_id=$1 AND execution_id=$2 AND turn_id=$3 LIMIT 2",
            &[&session_id, &execution_id, &turn_id],
        ))?;
        if rows.len() > 1 {
            return Err(RuntimeEventStoreError::Corrupt(
                "ambiguous Session terminal source tuple".into(),
            ));
        }
        rows.first().map(row_to_runtime_session_outbox).transpose()
    }

    fn materialized_session_terminals_after(
        &self,
        session_id: &str,
        after_commit_cursor: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        let mut connection = self.checkout_event_read()?;
        let rows = pg(connection.query(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                    request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                    input_claim_revision, status,
                    attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                    last_error, materialized_at, revision
               FROM runtime_session_outbox WHERE session_id=$1 AND status='materialized'
                 AND commit_cursor>$2 ORDER BY commit_cursor, terminal_id LIMIT $3",
            &[
                &session_id,
                &to_i64(after_commit_cursor, "after_commit_cursor")?,
                &to_i64(limit.clamp(1, 500) as u64, "limit")?,
            ],
        ))?;
        rows.iter().map(row_to_runtime_session_outbox).collect()
    }

    fn session_terminal_health(&self) -> RuntimeEventStoreResult<RuntimeSessionOutboxHealth> {
        let mut connection = self.checkout_event_read()?;
        let rows = pg(connection.query(
            "SELECT status, COUNT(*) FROM runtime_session_outbox GROUP BY status",
            &[],
        ))?;
        let mut health = RuntimeSessionOutboxHealth::default();
        for row in rows {
            let status: String = pg(row.try_get(0))?;
            let count = from_i64(pg(row.try_get(1))?, "outbox count")?;
            match status.as_str() {
                "pending" => health.pending = count,
                "claimed" => health.claimed = count,
                "retry_scheduled" => health.retry_scheduled = count,
                "materialized" => health.materialized = count,
                "blocked" => health.blocked = count,
                "suppressed" => health.suppressed = count,
                _ => {}
            }
        }
        Ok(health)
    }

    fn blocked_session_terminals(
        &self,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        let mut connection = self.checkout_event_read()?;
        let rows = pg(connection.query(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                    request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                    input_claim_revision, status,
                    attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                    last_error, materialized_at, revision
               FROM runtime_session_outbox WHERE status='blocked'
               ORDER BY COALESCE(next_attempt_at, 0), commit_cursor, terminal_id LIMIT $1",
            &[&to_i64(limit.clamp(1, 500) as u64, "limit")?],
        ))?;
        rows.iter().map(row_to_runtime_session_outbox).collect()
    }

    fn retry_session_terminal(
        &self,
        terminal_id: &str,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "manual terminal retry requires actor and reason".to_string(),
            ));
        }
        let mut connection = self.checkout_event_write()?;
        let changed = pg(connection.execute(
            "UPDATE runtime_session_outbox SET status='retry_scheduled', next_attempt_at=$1,
             claim_owner=NULL, claim_expires_at=NULL, failure_class=NULL, last_error=$2,
             revision=revision+1 WHERE terminal_id=$3 AND status='blocked'",
            &[
                &to_i64(now_ms, "now_ms")?,
                &format!("manual retry by {actor}: {reason}"),
                &terminal_id,
            ],
        ))?;
        if changed != 1 {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{terminal_id}` is not blocked"
            )));
        }
        query_runtime_session_outbox(&mut connection, terminal_id)?.ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!("terminal `{terminal_id}` vanished"))
        })
    }

    fn ack_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.transition_session_terminal(
            terminal_id,
            worker_id,
            expected_revision,
            "materialized",
            None,
            None,
            now_ms,
        )
    }

    fn suppress_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        reason: &str,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.transition_session_terminal(
            terminal_id,
            worker_id,
            expected_revision,
            "suppressed",
            Some(("terminal_fence_conflict", reason)),
            None,
            now_ms,
        )
    }

    fn adopt_session_terminal_fence(
        &self,
        request: &RuntimeSessionTerminalFenceAdoption,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        if request.terminal_id.trim().is_empty()
            || request.request_id.trim().is_empty()
            || request.session_id.trim().is_empty()
            || request.turn_id.trim().is_empty()
            || request.claim_owner.trim().is_empty()
            || request.claim_token.trim().is_empty()
            || request.session_generation == 0
            || request.claim_revision == 0
            || request.claim_expires_at_ms <= request.adopted_at_ms
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "terminal fence adoption requires live terminal, request, session, turn and claim identities"
                    .to_string(),
            ));
        }
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let current = pg(tx.query_opt(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                    request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                    input_claim_revision, status, attempts, next_attempt_at, claim_owner,
                    claim_expires_at, failure_class, last_error, materialized_at, revision
               FROM runtime_session_outbox WHERE terminal_id=$1 FOR UPDATE",
            &[&request.terminal_id],
        ))?
        .map(|row| row_to_runtime_session_outbox(&row))
        .transpose()?
        .ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!(
                "terminal `{}` is missing",
                request.terminal_id
            ))
        })?;
        if current.request_id.as_deref() != Some(request.request_id.as_str())
            || current.session_id != request.session_id
            || current.turn_id.as_deref() != Some(request.turn_id.as_str())
            || current.session_generation != Some(request.session_generation)
            || current.input_sequence != Some(request.input_sequence)
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{}` identity does not match the current Session claim",
                request.terminal_id
            )));
        }
        let already_adopted = current.input_claim_owner.as_deref()
            == Some(request.claim_owner.as_str())
            && current.input_claim_token.as_deref() == Some(request.claim_token.as_str())
            && current.input_claim_revision == Some(request.claim_revision)
            && current.input_sequence == Some(request.input_sequence);
        if already_adopted {
            return Ok(current);
        }
        if current.revision != request.expected_terminal_revision {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: format!("session-terminal:{}", request.terminal_id),
                expected: request.expected_terminal_revision,
                actual: current.revision,
            });
        }
        if current.status == "materialized" {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "materialized terminal `{}` cannot adopt a different Session claim",
                request.terminal_id
            )));
        }
        if !matches!(
            current.status.as_str(),
            "pending" | "retry_scheduled" | "blocked" | "claimed"
        ) {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{}` in state `{}` cannot adopt a Session claim",
                request.terminal_id, current.status
            )));
        }
        if current
            .input_claim_revision
            .is_some_and(|revision| request.claim_revision <= revision)
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{}` cannot regress Session claim revision",
                request.terminal_id
            )));
        }
        if current.status == "claimed"
            && !current
                .claim_expires_at_ms
                .is_some_and(|expires| expires <= request.adopted_at_ms)
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{}` has an active delivery claim",
                request.terminal_id
            )));
        }
        let changed = pg(tx.execute(
            "UPDATE runtime_session_outbox
                SET input_sequence=$1, input_claim_owner=$2, input_claim_token=$3, input_claim_revision=$4,
                    status='pending', next_attempt_at=0, claim_owner=NULL,
                    claim_expires_at=NULL, failure_class=NULL, last_error=NULL,
                    materialized_at=NULL, revision=revision+1
              WHERE terminal_id=$5 AND revision=$6",
            &[
                &to_i64(request.input_sequence, "input_sequence")?,
                &request.claim_owner,
                &request.claim_token,
                &to_i64(request.claim_revision, "claim_revision")?,
                &request.terminal_id,
                &to_i64(
                    request.expected_terminal_revision,
                    "expected_terminal_revision",
                )?,
            ],
        ))?;
        if changed != 1 {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: format!("session-terminal:{}", request.terminal_id),
                expected: request.expected_terminal_revision,
                actual: current.revision,
            });
        }
        let adopted =
            query_runtime_session_outbox(&mut tx, &request.terminal_id)?.ok_or_else(|| {
                RuntimeEventStoreError::Corrupt(format!(
                    "terminal `{}` vanished after fence adoption",
                    request.terminal_id
                ))
            })?;
        pg(tx.commit())?;
        Ok(adopted)
    }

    fn fail_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        class: RuntimeSessionOutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        let current = self.session_terminal(terminal_id)?.ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!("terminal `{terminal_id}` missing"))
        })?;
        let retry = class == RuntimeSessionOutboxFailureClass::Retryable
            && current.attempts < max_attempts.max(1);
        self.transition_session_terminal(
            terminal_id,
            worker_id,
            expected_revision,
            if retry { "retry_scheduled" } else { "blocked" },
            Some((failure_class_as_str(class), error)),
            retry.then_some(retry_at_ms),
            now_ms,
        )
    }
}

impl PostgresRuntimeEventStore {
    #[allow(clippy::too_many_arguments)]
    fn transition_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        status: &str,
        failure: Option<(&str, &str)>,
        retry_at_ms: Option<u64>,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        let mut connection = self.executor.checkout_critical()?;
        let (failure_class, last_error) = failure.unzip();
        let row = pg(connection.query_opt(
            "UPDATE runtime_session_outbox SET status=$1, next_attempt_at=$2,
             claim_owner=NULL, claim_expires_at=NULL, failure_class=$3, last_error=$4,
             materialized_at=CASE WHEN $1='materialized' THEN $5 ELSE materialized_at END,
             revision=revision+1 WHERE terminal_id=$6 AND status='claimed'
             AND claim_owner=$7 AND revision=$8
              RETURNING terminal_id, message_id, session_id, commit_cursor, payload_ref,
                 execution_id, turn_id, request_id, session_generation, input_sequence, input_claim_owner,
                 input_claim_token, input_claim_revision, status, attempts, next_attempt_at, claim_owner,
                 claim_expires_at, failure_class, last_error, materialized_at, revision",
            &[
                &status,
                &retry_at_ms
                    .map(|value| to_i64(value, "retry_at_ms"))
                    .transpose()?,
                &failure_class,
                &last_error,
                &to_i64(now_ms, "now_ms")?,
                &terminal_id,
                &worker_id,
                &to_i64(expected_revision, "expected_revision")?,
            ],
        ))?;
        if let Some(row) = row {
            return row_to_runtime_session_outbox(&row);
        }
        let actual_record = query_runtime_session_outbox(&mut connection, terminal_id)?;
        // P4 idempotent terminal acknowledgement: the message write is
        // exactly-once and delivery is at-least-once. A retried ack after a
        // successful materialization must succeed instead of failing with a
        // stale-revision error (`expected == actual` observed in production).
        if let Some(record) = actual_record.as_ref() {
            if status == "materialized"
                && record.status == "materialized"
                && record.revision >= expected_revision
            {
                return Ok(record.clone());
            }
        }
        let actual = actual_record.map_or(0, |record| record.revision);
        Err(RuntimeEventStoreError::StaleRevision {
            stream_id: format!("terminal:{terminal_id}"),
            expected: expected_revision,
            actual,
        })
    }
}

/// Copy a quiesced RuntimeEvent ledger exactly once, prove canonical digest
/// equality, then atomically write a backend-neutral cutover manifest.
fn append_transaction_in_tx(
    tx: &mut PostgresTransaction<'_>,
    request: &AppendTransactionRequest,
    terminal: Option<&SessionTerminalInput>,
) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
    append_transaction_in_tx_checked(tx, request, terminal, None)
}

// A member's action, rejection and terminal share one ordering point before
// transaction/claim/source locks, including the ordinary append/replay path.
fn original_member_decision_locks(request: &AppendTransactionRequest) -> RuntimeEventStoreResult<Vec<String>> {
    let invalid = || RuntimeEventStoreError::InvalidTransaction("member writer lacks its original index/source".into());
    let mut keys = Vec::new();
    for (position, input) in request.events.iter().enumerate() {
        let event = &input.event;
        if matches!(event.kind.as_str(), "agentic.action_applied" | "agentic.completion_rejected") && event.payload.get("member_admission").is_some() {
            let facts = &event.payload["member_admission"];
            let outer = facts["prepared"]["outer_key"].as_str().ok_or_else(invalid)?;
            let index = facts["prepared"]["index"].as_u64().ok_or_else(invalid)?;
            let markers = request.events.iter().filter(|marker| marker.event.kind == "agentic.batch_member.index"
                && marker.event.payload["outer_key"] == outer && marker.event.payload["member_index"] == index
                && marker.event.payload["program_event_index"].as_u64() == Some(position as u64)
                && marker.event.payload["program_stream"] == event.stream_id
                && marker.event.payload["program_event_kind"] == event.kind
                && marker.event.payload["program_event_key"].as_str() == input.idempotency_key.as_deref()
                && marker.event.payload["ticket"] == facts["ticket"]).count();
            if markers != 1 { return Err(invalid()); }
        }
        if !matches!(event.kind.as_str(), "agentic.batch_member.index" | "execution.effect.member_noncommit" | "execution.effect.member_preparation") { continue; }
        let outer = event.payload["outer_key"].as_str().filter(|s| !s.is_empty()).ok_or_else(invalid)?;
        let index = event.payload["member_index"].as_u64().ok_or_else(invalid)?;
        let stream = format!("execution-effect:{outer}");
        let key = if event.kind == "execution.effect.member_preparation" { format!("agentic-member-preparation:{index}") }
            else { format!("agentic-member:{index}") };
        if event.stream_id != stream || input.idempotency_key.as_deref() != Some(key.as_str())
            || !request.expected_streams.iter().any(|s| s.stream_id == stream) { return Err(invalid()); }
        if event.kind == "agentic.batch_member.index" {
            let position = event.payload["program_event_index"].as_u64().and_then(|n| usize::try_from(n).ok()).ok_or_else(invalid)?;
            let action = request.events.get(position).ok_or_else(invalid)?;
            if !matches!(action.event.kind.as_str(), "agentic.action_applied" | "agentic.completion_rejected")
                || action.event.payload.pointer("/member_admission/prepared/outer_key").and_then(Value::as_str) != Some(outer)
                || action.event.payload.pointer("/member_admission/prepared/index").and_then(Value::as_u64) != Some(index)
                || action.event.payload.pointer("/member_admission/ticket") != event.payload.get("ticket")
                || event.payload["program_stream"] != action.event.stream_id
                || event.payload["program_event_kind"] != action.event.kind
                || event.payload["program_event_key"].as_str() != action.idempotency_key.as_deref() { return Err(invalid()); }
        }
        keys.push(format!("cowd-runtime-member-decision:{}:{outer}:{index}", outer.len()));
    }
    keys.sort(); keys.dedup();
    Ok(keys)
}
fn lock_original_member_decisions(tx: &mut PostgresTransaction<'_>, request: &AppendTransactionRequest) -> RuntimeEventStoreResult<()> {
    for key in original_member_decision_locks(request)? {
        pg(tx.query_one("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", &[&key]))?;
    }
    Ok(())
}

fn append_transaction_in_tx_checked(
    tx: &mut PostgresTransaction<'_>,
    request: &AppendTransactionRequest,
    terminal: Option<&SessionTerminalInput>,
    checked: Option<(&CheckedAppendRequest, &dyn CheckedSourceValidator)>,
) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
    validate_transaction(request)?;
    RuntimeEventStore::validate_current_content_inputs(request)?;
    if let Some(terminal) = terminal {
        validate_fenced_terminal(terminal)?;
        if serde_json::to_vec(&(request, terminal))?.len() > MAX_TRANSACTION_BYTES {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "serialized terminal transaction exceeds hard limit {MAX_TRANSACTION_BYTES} bytes"
            )));
        }
    }
    let request_hash = match checked {
        Some((request, _)) => request.identity_hash()?,
        None => request_hash_with_terminal(request, terminal)?,
    };
    let held_since = std::time::Instant::now();
    lock_original_member_decisions(tx, request)?;
    let transaction_lock = format!("cowd-runtime-event-transaction:{}", request.transaction_id);
    pg(tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        &[&transaction_lock],
    ))?;
    if let Some(row) = pg(tx.query_opt(
        "SELECT request_hash FROM runtime_commits WHERE transaction_id=$1",
        &[&request.transaction_id],
    ))? {
        let committed_hash: String = pg(row.try_get(0))?;
        if committed_hash != request_hash {
            return Err(RuntimeEventStoreError::TransactionConflict {
                transaction_id: request.transaction_id.clone(),
            });
        }
        let receipt = load_receipt(tx, &request.transaction_id, true)?;
        if let Some(terminal) = terminal {
            verify_terminal_for_commit(tx, terminal, receipt.commit_cursor)?;
        }
        return Ok(receipt);
    }

    let mut streams = request.expected_streams.iter().collect::<Vec<_>>();
    streams.sort_by(|left, right| left.stream_id.cmp(&right.stream_id));
    for stream in &streams {
        let stream_lock = format!("cowd-runtime-event-stream:{}", stream.stream_id);
        pg(tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&stream_lock],
        ))?;
    }
    let mut expected = request
        .expected_streams
        .iter()
        .map(|stream| (stream.stream_id.as_str(), stream.expected_revision))
        .collect::<BTreeMap<_, _>>();
    let mut evidence = checked.map(|(request, _)| CheckedSourceEvidence {
        frame_digest: request.frame_digest.clone(),
        sources: Vec::new(),
        events_read: 0,
        bytes_read: 0,
        pages_read: 0,
    });
    for stream in &request.expected_streams {
        let actual = stream_head(tx, &stream.stream_id)?;
        if let Some((checked, validator)) = checked {
            let evidence = evidence.as_mut().expect("checked evidence initialized");
            let count = actual
                .checked_sub(stream.expected_revision)
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
            let metadata = pg(tx.query(
                "SELECT sequence,encoded_bytes FROM runtime_events WHERE stream_id=$1 AND sequence>$2 AND sequence<=$3 ORDER BY sequence LIMIT $4",
                &[&stream.stream_id, &to_i64(stream.expected_revision, "source cut")?,
                  &to_i64(actual, "source head")?, &to_i64(count as u64, "source count")?]))?;
            if metadata.len() != count {
                return Err(RuntimeEventStoreError::Corrupt(
                    "incomplete source tail".into(),
                ));
            }
            for (offset, row) in metadata.iter().enumerate() {
                let sequence = from_i64(pg(row.try_get(0))?, "source sequence")?;
                if sequence != stream.expected_revision + offset as u64 + 1 {
                    return Err(RuntimeEventStoreError::Corrupt(
                        "discontinuous source tail".into(),
                    ));
                }
                let bytes = from_i64(pg(row.try_get(1))?, "source bytes")?;
                evidence.bytes_read = evidence
                    .bytes_read
                    .saturating_add(16)
                    .saturating_add(usize::try_from(bytes).unwrap_or(usize::MAX));
            }
            if evidence.bytes_read > checked.budget.max_bytes
                || held_since.elapsed().as_millis() > u128::from(checked.budget.max_lock_millis)
            {
                return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
                    "cumulative source bytes or held time".into(),
                ));
            }
            let rows = pg(tx.query(&format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 AND sequence>$2 AND sequence<=$3 ORDER BY sequence LIMIT $4"),
                &[&stream.stream_id, &to_i64(stream.expected_revision, "source cut")?,
                  &to_i64(actual, "source head")?, &to_i64(count as u64, "source count")?]))?;
            let events = rows
                .iter()
                .map(row_to_event)
                .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
            if events.len() != count {
                return Err(RuntimeEventStoreError::Corrupt(
                    "source tail changed under guard".into(),
                ));
            }
            validator.validate_tail(stream, actual, &events)?;
            if held_since.elapsed().as_millis() > u128::from(checked.budget.max_lock_millis) {
                return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
                    "source validator held time".into(),
                ));
            }
            evidence.sources.push(CommittedStreamRevision {
                stream_id: stream.stream_id.clone(),
                expected_revision: stream.expected_revision,
                committed_revision: actual,
            });
            expected.insert(stream.stream_id.as_str(), actual);
        } else if actual != stream.expected_revision {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: stream.stream_id.clone(),
                expected: stream.expected_revision,
                actual,
            });
        }
    }

    pg(tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || ':cowd-runtime-publication', 0))",
        &[],
    ))?;
    let created_at_ms = now_ms();
    let row = pg(tx.query_one(
        "INSERT INTO runtime_commits(transaction_id, request_hash, created_at_ms)
         VALUES ($1,$2,$3) RETURNING commit_cursor",
        &[
            &request.transaction_id,
            &request_hash,
            &to_i64(created_at_ms, "created_at_ms")?,
        ],
    ))?;
    let commit_cursor = from_i64(pg(row.try_get(0))?, "commit_cursor")?;
    if let Some(evidence) = evidence {
        pg(tx.execute(
            "UPDATE runtime_commits SET checked_source_evidence=$1 WHERE commit_cursor=$2",
            &[
                &serde_json::to_value(evidence)?,
                &to_i64(commit_cursor, "commit cursor")?,
            ],
        ))?;
    }
    if let Some(terminal) = terminal {
        insert_terminal_in_tx(tx, terminal, commit_cursor)?;
    }
    let mut increments = BTreeMap::<&str, u64>::new();
    let mut event_ids = Vec::with_capacity(request.events.len());
    for (transaction_index, input) in request.events.iter().enumerate() {
        let stream_id = input.event.stream_id.as_str();
        let offset = increments.entry(stream_id).or_default();
        *offset += 1;
        let sequence = expected[stream_id] + *offset;
        let event_id = format!("runtime-event-{}", uuid::Uuid::new_v4());
        let encoded_bytes = serde_json::to_vec(&DurableRuntimeEvent {
            event_id: event_id.clone(),
            stream_id: input.event.stream_id.clone(),
            sequence,
            scope: input.event.scope,
            kind: input.event.kind.clone(),
            status: input.event.status.clone(),
            actor: input.event.actor.clone(),
            payload: input.event.payload.clone(),
            refs: input.event.refs.clone(),
            created_at_ms,
            commit_cursor,
            transaction_id: request.transaction_id.clone(),
            transaction_index: u32::try_from(transaction_index).map_err(|_| {
                RuntimeEventStoreError::InvalidTransaction("transaction index overflow".into())
            })?,
            schema_version: input.schema_version,
            idempotency_key: input.idempotency_key.clone(),
        })?
        .len();
        let encoded_bytes = i64::try_from(encoded_bytes).map_err(|_| {
            RuntimeEventStoreError::InvalidTransaction("event bytes overflow".into())
        })?;
        let refs = serde_json::to_value(&input.event.refs)?;
        let activity_binding = input.event.activity_binding();
        let root_execution_id = activity_binding
            .as_ref()
            .map(|binding| binding.root_execution_id.as_str());
        let activity_id = activity_binding
            .as_ref()
            .map(|binding| binding.activity_id.as_str());
        let session_id = activity_binding
            .as_ref()
            .map(|binding| binding.session_id.as_str())
            .or_else(|| {
                input
                    .event
                    .refs
                    .iter()
                    .find(|reference| reference.kind == "session")
                    .map(|reference| reference.id.as_str())
            });
        pg(tx.execute(
            "INSERT INTO runtime_events (event_id, stream_id, sequence, scope, kind, status, actor,
                payload, refs, created_at_ms, commit_cursor, transaction_id, transaction_index,
                schema_version, idempotency_key, root_execution_id, activity_id, encoded_bytes, session_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)",
            &[
                &event_id,
                &input.event.stream_id,
                &to_i64(sequence, "sequence")?,
                &input.event.scope.as_str(),
                &input.event.kind,
                &input.event.status,
                &input.event.actor,
                &input.event.payload,
                &refs,
                &to_i64(created_at_ms, "created_at_ms")?,
                &to_i64(commit_cursor, "commit_cursor")?,
                &request.transaction_id,
                &to_i64(transaction_index as u64, "transaction_index")?,
                &i64::from(input.schema_version),
                &input.idempotency_key,
                &root_execution_id,
                &activity_id,
                &encoded_bytes,
                &session_id,
            ],
        ))?;
        event_ids.push(event_id);
    }
    let mut stream_revisions = Vec::with_capacity(request.expected_streams.len());
    for stream in &request.expected_streams {
        let committed_revision = expected[stream.stream_id.as_str()]
            + increments
                .get(stream.stream_id.as_str())
                .copied()
                .unwrap_or_default();
        pg(tx.execute(
            "INSERT INTO runtime_stream_heads(stream_id, revision) VALUES ($1,$2)
             ON CONFLICT(stream_id) DO UPDATE SET revision=EXCLUDED.revision",
            &[
                &stream.stream_id,
                &to_i64(committed_revision, "committed_revision")?,
            ],
        ))?;
        pg(tx.execute(
            "INSERT INTO runtime_transaction_streams
             (transaction_id, stream_id, expected_revision, committed_revision)
             VALUES ($1,$2,$3,$4)",
            &[
                &request.transaction_id,
                &stream.stream_id,
                &to_i64(stream.expected_revision, "expected_revision")?,
                &to_i64(committed_revision, "committed_revision")?,
            ],
        ))?;
        stream_revisions.push(CommittedStreamRevision {
            stream_id: stream.stream_id.clone(),
            expected_revision: stream.expected_revision,
            committed_revision,
        });
    }
    if checked.is_some_and(|(request, _)| {
        held_since.elapsed().as_millis() > u128::from(request.budget.max_lock_millis)
    }) {
        return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
            "source transaction held time".into(),
        ));
    }
    Ok(AppendTransactionReceipt {
        commit_cursor,
        transaction_id: request.transaction_id.clone(),
        request_hash,
        stream_revisions,
        event_ids,
        duplicate: false,
    })
}

fn insert_terminal_in_tx(
    tx: &mut PostgresTransaction<'_>,
    terminal: &SessionTerminalInput,
    commit_cursor: u64,
) -> RuntimeEventStoreResult<()> {
    pg(tx.execute(
        "INSERT INTO runtime_session_outbox
         (terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
          request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
          input_claim_revision, status, revision)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'pending',0)
         ON CONFLICT(terminal_id) DO NOTHING",
        &[
            &terminal.terminal_id,
            &terminal.message_id,
            &terminal.session_id,
            &to_i64(commit_cursor, "commit_cursor")?,
            &terminal.payload_ref,
            &terminal.execution_id,
            &terminal.turn_id,
            &terminal.request_id,
            &terminal
                .session_generation
                .map(|value| to_i64(value, "session_generation"))
                .transpose()?,
            &terminal
                .input_sequence
                .map(|value| to_i64(value, "input_sequence"))
                .transpose()?,
            &terminal.input_claim_owner,
            &terminal.input_claim_token,
            &terminal
                .input_claim_revision
                .map(|value| to_i64(value, "input_claim_revision"))
                .transpose()?,
        ],
    ))?;
    let stored = query_runtime_session_outbox(tx, &terminal.terminal_id)?.ok_or_else(|| {
        RuntimeEventStoreError::Corrupt(format!(
            "terminal outbox `{}` disappeared during commit",
            terminal.terminal_id
        ))
    })?;
    if stored.message_id != terminal.message_id
        || stored.session_id != terminal.session_id
        || stored.commit_cursor != commit_cursor
        || stored.payload_ref != terminal.payload_ref
        || stored.execution_id != terminal.execution_id
        || stored.turn_id != terminal.turn_id
        || stored.request_id != terminal.request_id
        || stored.session_generation != terminal.session_generation
        || stored.input_sequence != terminal.input_sequence
        || stored.input_claim_owner != terminal.input_claim_owner
        || stored.input_claim_token != terminal.input_claim_token
        || stored.input_claim_revision != terminal.input_claim_revision
    {
        return Err(RuntimeEventStoreError::TransactionConflict {
            transaction_id: terminal.terminal_id.clone(),
        });
    }
    Ok(())
}

fn verify_terminal_for_commit(
    client: &mut impl PostgresClient,
    terminal: &SessionTerminalInput,
    commit_cursor: u64,
) -> RuntimeEventStoreResult<()> {
    let rows = pg(client.query(
        "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                input_claim_revision, status, attempts, next_attempt_at, claim_owner,
                claim_expires_at, failure_class, last_error, materialized_at, revision
           FROM runtime_session_outbox WHERE commit_cursor=$1
           ORDER BY terminal_id LIMIT 2",
        &[&to_i64(commit_cursor, "commit_cursor")?],
    ))?;
    if rows.len() != 1 {
        return Err(RuntimeEventStoreError::TransactionConflict {
            transaction_id: terminal.terminal_id.clone(),
        });
    }
    let stored = row_to_runtime_session_outbox(&rows[0])?;
    if stored.terminal_id != terminal.terminal_id
        || stored.message_id != terminal.message_id
        || stored.session_id != terminal.session_id
        || stored.payload_ref != terminal.payload_ref
        || stored.execution_id != terminal.execution_id
        || stored.turn_id != terminal.turn_id
        || stored.request_id != terminal.request_id
        || stored.session_generation != terminal.session_generation
        || stored.input_sequence != terminal.input_sequence
    {
        return Err(RuntimeEventStoreError::TransactionConflict {
            transaction_id: terminal.terminal_id.clone(),
        });
    }
    Ok(())
}

fn load_receipt(
    client: &mut impl PostgresClient,
    transaction_id: &str,
    duplicate: bool,
) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
    let row = pg(client.query_one(
        "SELECT commit_cursor, request_hash FROM runtime_commits WHERE transaction_id=$1",
        &[&transaction_id],
    ))?;
    let commit_cursor = from_i64(pg(row.try_get(0))?, "commit_cursor")?;
    let request_hash: String = pg(row.try_get(1))?;
    let stream_revisions = pg(client.query(
        "SELECT stream_id, expected_revision, committed_revision FROM runtime_transaction_streams
         WHERE transaction_id=$1 ORDER BY stream_id ASC",
        &[&transaction_id],
    ))?
    .iter()
    .map(|row| {
        Ok(CommittedStreamRevision {
            stream_id: pg(row.try_get(0))?,
            expected_revision: from_i64(pg(row.try_get(1))?, "expected_revision")?,
            committed_revision: from_i64(pg(row.try_get(2))?, "committed_revision")?,
        })
    })
    .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
    let event_ids = pg(client.query(
        "SELECT event_id FROM runtime_events WHERE transaction_id=$1 ORDER BY transaction_index ASC",
        &[&transaction_id],
    ))?
    .iter()
    .map(|row| pg(row.try_get(0)))
    .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
    Ok(AppendTransactionReceipt {
        commit_cursor,
        transaction_id: transaction_id.to_string(),
        request_hash,
        stream_revisions,
        event_ids,
        duplicate,
    })
}

fn stream_head(client: &mut impl PostgresClient, stream_id: &str) -> RuntimeEventStoreResult<u64> {
    pg(client.query_opt(
        "SELECT revision FROM runtime_stream_heads WHERE stream_id=$1",
        &[&stream_id],
    ))?
    .map(|row| from_i64(pg(row.try_get(0))?, "revision"))
    .transpose()
    .map(|value| value.unwrap_or_default())
}

fn query_runtime_session_outbox(
    client: &mut impl PostgresClient,
    terminal_id: &str,
) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
    pg(client.query_opt(
        "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                input_claim_revision, status,
                attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                last_error, materialized_at, revision
           FROM runtime_session_outbox WHERE terminal_id=$1",
        &[&terminal_id],
    ))?
    .map(|row| row_to_runtime_session_outbox(&row))
    .transpose()
}

fn rows_to_events(rows: Vec<Row>) -> RuntimeEventStoreResult<Vec<DurableRuntimeEvent>> {
    rows.iter().map(row_to_event).collect()
}

fn row_to_event(row: &Row) -> RuntimeEventStoreResult<DurableRuntimeEvent> {
    let scope: String = pg(row.try_get(3))?;
    Ok(DurableRuntimeEvent {
        event_id: pg(row.try_get(0))?,
        stream_id: pg(row.try_get(1))?,
        sequence: from_i64(pg(row.try_get(2))?, "sequence")?,
        scope: RuntimeEventScope::parse(&scope)?,
        kind: pg(row.try_get(4))?,
        status: pg(row.try_get(5))?,
        actor: pg(row.try_get(6))?,
        payload: pg(row.try_get(7))?,
        refs: serde_json::from_value(pg(row.try_get::<_, Value>(8))?)?,
        created_at_ms: from_i64(pg(row.try_get(9))?, "created_at_ms")?,
        commit_cursor: from_i64(pg(row.try_get(10))?, "commit_cursor")?,
        transaction_id: pg(row.try_get(11))?,
        transaction_index: u32::try_from(from_i64(pg(row.try_get(12))?, "transaction_index")?)
            .map_err(|_| {
                RuntimeEventStoreError::Corrupt("transaction_index exceeds u32".to_string())
            })?,
        schema_version: u32::try_from(from_i64(pg(row.try_get(13))?, "schema_version")?).map_err(
            |_| RuntimeEventStoreError::Corrupt("schema_version exceeds u32".to_string()),
        )?,
        idempotency_key: pg(row.try_get(14))?,
    })
}

fn row_to_projection_checkpoint(row: &Row) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
    Ok(RuntimeProjectionCheckpoint {
        projection_id: pg(row.try_get(0))?,
        source_cursor: from_i64(pg(row.try_get(1))?, "source_cursor")?,
        revision: from_i64(pg(row.try_get(2))?, "revision")?,
        payload: pg(row.try_get(3))?,
        updated_at_ms: from_i64(pg(row.try_get(4))?, "updated_at_ms")?,
    })
}

fn validate_projection_id(projection_id: &str) -> RuntimeEventStoreResult<()> {
    if projection_id.trim().is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "projection id must not be empty".to_string(),
        ));
    }
    if projection_id.len() > 512 {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "projection id exceeds 512 bytes".to_string(),
        ));
    }
    Ok(())
}

fn group_committed_events(events: Vec<RuntimeEventRecord>) -> Vec<CommittedEventBatch> {
    let mut batches = Vec::<CommittedEventBatch>::new();
    for event in events {
        if let Some(batch) = batches.last_mut() {
            if batch.commit_cursor == event.commit_cursor {
                batch.events.push(event);
                continue;
            }
        }
        batches.push(CommittedEventBatch {
            commit_cursor: event.commit_cursor,
            transaction_id: event.transaction_id.clone(),
            events: vec![event],
        });
    }
    batches
}

fn build_projection_scan_page(
    cursor: u64,
    selected: Vec<(u64, String)>,
    events: Vec<RuntimeEventRecord>,
) -> RuntimeProjectionScanPage {
    let mut events_by_commit = group_committed_events(events)
        .into_iter()
        .map(|batch| (batch.commit_cursor, batch.events))
        .collect::<BTreeMap<_, _>>();
    let mut page = RuntimeProjectionScanPage {
        scanned_through_cursor: cursor,
        ..RuntimeProjectionScanPage::default()
    };
    for (commit_cursor, transaction_id) in selected {
        let events = events_by_commit.remove(&commit_cursor).unwrap_or_default();
        page.scanned_through_cursor = commit_cursor;
        page.scanned_commits = page.scanned_commits.saturating_add(1);
        if events.is_empty() {
            continue;
        }
        page.matched_events = page.matched_events.saturating_add(events.len());
        page.batches.push(CommittedEventBatch {
            commit_cursor,
            transaction_id,
            events,
        });
    }
    page
}

fn row_to_runtime_session_outbox(row: &Row) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
    Ok(RuntimeSessionOutboxRecord {
        terminal_id: pg(row.try_get(0))?,
        message_id: pg(row.try_get(1))?,
        session_id: pg(row.try_get(2))?,
        commit_cursor: from_i64(pg(row.try_get(3))?, "commit_cursor")?,
        payload_ref: pg(row.try_get(4))?,
        execution_id: pg(row.try_get(5))?,
        turn_id: pg(row.try_get(6))?,
        request_id: pg(row.try_get(7))?,
        session_generation: pg(row.try_get::<_, Option<i64>>(8))?
            .map(|value| from_i64(value, "session_generation"))
            .transpose()?,
        input_sequence: pg(row.try_get::<_, Option<i64>>(9))?
            .map(|value| from_i64(value, "input_sequence"))
            .transpose()?,
        input_claim_owner: pg(row.try_get(10))?,
        input_claim_token: pg(row.try_get(11))?,
        input_claim_revision: pg(row.try_get::<_, Option<i64>>(12))?
            .map(|value| from_i64(value, "input_claim_revision"))
            .transpose()?,
        status: pg(row.try_get(13))?,
        attempts: u32::try_from(from_i64(pg(row.try_get(14))?, "attempts")?)
            .map_err(|_| RuntimeEventStoreError::Corrupt("attempts exceeds u32".to_string()))?,
        next_attempt_at_ms: pg(row.try_get::<_, Option<i64>>(15))?
            .map(|value| from_i64(value, "next_attempt_at"))
            .transpose()?,
        claim_owner: pg(row.try_get(16))?,
        claim_expires_at_ms: pg(row.try_get::<_, Option<i64>>(17))?
            .map(|value| from_i64(value, "claim_expires_at"))
            .transpose()?,
        failure_class: pg(row.try_get(18))?,
        last_error: pg(row.try_get(19))?,
        materialized_at_ms: pg(row.try_get::<_, Option<i64>>(20))?
            .map(|value| from_i64(value, "materialized_at"))
            .transpose()?,
        revision: from_i64(pg(row.try_get(21))?, "revision")?,
    })
}

const fn failure_class_as_str(class: RuntimeSessionOutboxFailureClass) -> &'static str {
    match class {
        RuntimeSessionOutboxFailureClass::Retryable => "retryable",
        RuntimeSessionOutboxFailureClass::Permanent => "permanent",
        RuntimeSessionOutboxFailureClass::AuthorizationBlocked => "authorization_blocked",
        RuntimeSessionOutboxFailureClass::CorruptPayload => "corrupt_payload",
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn pg<T>(result: Result<T, postgres::Error>) -> RuntimeEventStoreResult<T> {
    result.map_err(|error| RuntimeEventStoreError::Storage(StorageError::Other(error.to_string())))
}

fn from_i64(value: i64, field: &str) -> RuntimeEventStoreResult<u64> {
    u64::try_from(value).map_err(|_| {
        RuntimeEventStoreError::Corrupt(format!("runtime event `{field}` is negative"))
    })
}

fn to_i64(value: u64, field: &str) -> RuntimeEventStoreResult<i64> {
    i64::try_from(value).map_err(|_| {
        RuntimeEventStoreError::InvalidTransaction(format!("runtime event `{field}` exceeds i64"))
    })
}

#[cfg(test)]
mod current_history_tests {
    use super::*;

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn r1_bounded_history_reads_checkpoint_and_limited_tail() {
        let (store, fixture) = crate::tests::open_real_store();
        for sequence in 0..512 {
            store
                .append(RuntimeEventInput {
                    stream_id: "bounded-checkpoint".into(),
                    scope: RuntimeEventScope::Session,
                    kind: "current.fact".into(),
                    status: None,
                    actor: None,
                    refs: Vec::new(),
                    payload: serde_json::json!({"sequence":sequence}),
                })
                .unwrap();
        }
        let checkpoint = store
            .compare_and_put_projection_checkpoint(
                "bounded-checkpoint-reader",
                480,
                0,
                &serde_json::json!({"count":480}),
                0,
            )
            .unwrap();
        fixture.reconnect().checkout_critical().unwrap().execute("UPDATE runtime_events SET refs='{}'::JSONB WHERE stream_id='bounded-checkpoint' AND sequence<=480",&[]).unwrap();
        drop(store);
        let reopened = PostgresRuntimeEventStore::new(fixture.reconnect())
            .unwrap()
            .into_runtime_event_store();
        assert_eq!(
            reopened
                .projection_checkpoint("bounded-checkpoint-reader")
                .unwrap(),
            Some(checkpoint.clone())
        );
        let fixed_head = reopened.stream_revision("bounded-checkpoint").unwrap();
        reopened
            .append(RuntimeEventInput {
                stream_id: "bounded-checkpoint".into(),
                scope: RuntimeEventScope::Session,
                kind: "future.fact".into(),
                status: None,
                actor: None,
                refs: Vec::new(),
                payload: serde_json::Value::Null,
            })
            .unwrap();
        let tail = reopened
            .list_stream_after(
                "bounded-checkpoint",
                checkpoint.source_cursor,
                fixed_head,
                100,
                1024 * 1024,
            )
            .unwrap();
        assert_eq!(tail.len(), 32);
        assert_eq!(tail.first().unwrap().sequence, 481);
        assert_eq!(tail.last().unwrap().sequence, 512);
        assert!(tail.iter().all(|event| event.kind == "current.fact"));
        let descending = reopened
            .list_stream_before("bounded-checkpoint", fixed_head, 16, 1024 * 1024)
            .unwrap();
        assert_eq!(descending.first().unwrap().sequence, 512);
        assert_eq!(descending.last().unwrap().sequence, 497);
        assert_eq!(
            reopened
                .count_stream_kinds("bounded-checkpoint", &["current.fact"], fixed_head)
                .unwrap(),
            512
        );
        assert!(reopened.list_stream("bounded-checkpoint").is_err());
    }
}

impl PostgresRuntimeEventStore {
    fn host_transcript_pair_metadata(
        connection: &mut PostgresConnection,
        primary: &DurableRuntimeEvent,
        stream: &str,
        node: bool,
    ) -> RuntimeEventStoreResult<Option<DurableRuntimeEvent>> {
        let projection = if node {
            "jsonb_build_object('_runtime_activity_binding',payload->'_runtime_activity_binding',
                'graph_id',payload->'graph_id','node_id',payload->'node_id',
                'from',payload->'from','to',payload->'to','graph_revision',payload->'graph_revision',
                'result',jsonb_build_object('status',payload#>'{result,status}',
                    'evidence_refs',(SELECT jsonb_agg(reference) FROM jsonb_array_elements(
                        CASE WHEN jsonb_typeof(payload#>'{result,evidence_refs}')='array'
                        THEN payload#>'{result,evidence_refs}' ELSE '[]'::JSONB END) AS selected(reference)
                        WHERE reference->>'retrieval_selector'=$4)))"
        } else {
            "jsonb_build_object('_runtime_activity_binding',payload->'_runtime_activity_binding')"
        };
        let predicate = if node {
            "kind='execution_node.transitioned'"
        } else {
            "scope='execution_graph'"
        };
        let rows = pg(connection.query(
            &format!("SELECT CASE WHEN octet_length(event_id)<=1024 THEN event_id ELSE '' END,
                stream_id,sequence,scope,CASE WHEN octet_length(kind)<=1024 THEN kind ELSE '' END,
                CASE WHEN octet_length(status)<=64 THEN status ELSE '' END,
                CASE WHEN octet_length(actor)<=128 THEN actor ELSE '' END,
                CASE WHEN octet_length(proof::TEXT)<=1048576 THEN proof END AS payload,
                '[]'::JSONB AS refs,created_at_ms,commit_cursor,transaction_id,transaction_index,schema_version,
                CASE WHEN octet_length(idempotency_key)<=4096 THEN idempotency_key ELSE '' END
                FROM runtime_events CROSS JOIN LATERAL (SELECT {projection} AS proof) selected
                WHERE transaction_id=$1 AND commit_cursor=$2 AND stream_id=$3 AND {predicate}
                    AND $4::TEXT IS NOT NULL
                ORDER BY transaction_index LIMIT 2"),
            &[&primary.transaction_id, &to_i64(primary.commit_cursor, "Host transcript cursor")?,
                &stream, &primary.idempotency_key.as_deref().unwrap_or("")],
        ))?;
        if rows.len() != 1 {
            return Ok(None);
        }
        if rows[0].get::<_, Option<Value>>(7).is_none() {
            return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
                "Host transcript pair root metadata".into(),
            ));
        }
        row_to_event(&rows[0]).map(Some)
    }

    fn host_transcript_root_event(
        connection: &mut PostgresConnection,
        stream: &str,
        key: &str,
    ) -> RuntimeEventStoreResult<Option<DurableRuntimeEvent>> {
        Self::process_closure_root_event(connection, stream, key).map_err(|error| match error {
            RuntimeEventStoreError::HistoryBudgetExceeded(_) => {
                RuntimeEventStoreError::HistoryBudgetExceeded("Host transcript root event".into())
            }
            error => error,
        })
    }

    fn process_closure_root_event(
        connection: &mut PostgresConnection,
        stream: &str,
        key: &str,
    ) -> RuntimeEventStoreResult<Option<DurableRuntimeEvent>> {
        let Some(metadata) = pg(connection.query_opt(
            "SELECT encoded_bytes FROM runtime_events WHERE stream_id=$1 AND idempotency_key=$2",
            &[&stream, &key],
        ))?
        else {
            return Ok(None);
        };
        let bytes: i64 = metadata.get(0);
        if bytes <= 0 || bytes > 1024 * 1024 {
            return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
                "process closure root event".into(),
            ));
        }
        pg(connection.query_opt(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 AND idempotency_key=$2"),
            &[&stream, &key],
        ))?.map(|row| row_to_event(&row)).transpose()
    }
}

#[cfg(test)]
mod source_fence_tests {
    use super::*;

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn own858_pg_source_fence_persists_and_direct_old_mutations_reject_cas() {
        let (store, fixture) = crate::tests::open_real_store();
        let initial = store.source_mutation_fence().unwrap();
        store.append(RuntimeEventInput {
            stream_id: "858-event".into(), scope: RuntimeEventScope::Session,
            kind: "858.append".into(), status: None, actor: None,
            refs: Vec::new(), payload: serde_json::json!({}),
        }).unwrap();
        assert_eq!(initial, store.source_mutation_fence().unwrap());
        let reopened = PostgresRuntimeEventStore::new(fixture.reconnect()).unwrap().into_runtime_event_store();
        assert_eq!(initial, reopened.source_mutation_fence().unwrap());
        let payload = serde_json::json!({"verified":true});
        let cursor = store.durable_commit_cursor().unwrap();
        let saved = store.compare_and_put_projection_checkpoint_fenced("858-reader", cursor, 0, &payload, 0, &initial).unwrap();
        fixture.reconnect().checkout_critical().unwrap().execute("UPDATE runtime_events SET actor='changed' WHERE stream_id='858-event'", &[]).unwrap();
        let updated = store.source_mutation_fence().unwrap();
        assert_eq!(updated.incarnation, initial.incarnation);
        assert_eq!(updated.mutation_epoch, initial.mutation_epoch + 1);
        assert!(store.compare_and_put_projection_checkpoint_fenced("858-reader", cursor, saved.revision, &payload, 0, &initial).is_err());
        assert_eq!(store.projection_checkpoint("858-reader").unwrap(), Some(saved.clone()));
        fixture.reconnect().checkout_critical().unwrap().execute("DELETE FROM runtime_events WHERE stream_id='858-event'", &[]).unwrap();
        let deleted = store.source_mutation_fence().unwrap();
        assert_eq!(deleted.mutation_epoch, updated.mutation_epoch + 1);
        assert!(store.compare_and_put_projection_checkpoint_fenced("858-reader", cursor, saved.revision, &payload, 0, &updated).is_err());
        store.compare_and_put_projection_checkpoint_fenced("858-reader", cursor, saved.revision, &payload, 0, &deleted).unwrap();
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn own858_pg_source_fenced_cas_serializes_with_uncommitted_old_mutation() {
        let (store, fixture) = crate::tests::open_real_store();
        store.append(RuntimeEventInput {
            stream_id: "858-race".into(), scope: RuntimeEventScope::Session,
            kind: "858.append".into(), status: None, actor: None,
            refs: Vec::new(), payload: serde_json::json!({}),
        }).unwrap();
        let initial = store.source_mutation_fence().unwrap();
        let executor = fixture.reconnect();
        let mut connection = executor.checkout_critical().unwrap();
        let mut transaction = connection.transaction().unwrap();
        transaction.execute("UPDATE runtime_events SET actor='uncommitted' WHERE stream_id='858-race'", &[]).unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = store.compare_and_put_projection_checkpoint_fenced("858-race-reader", 1, 0, &serde_json::json!({}), 0, &initial);
            sender.send(result).unwrap();
        });
        assert!(matches!(receiver.recv_timeout(std::time::Duration::from_millis(100)), Err(std::sync::mpsc::RecvTimeoutError::Timeout)));
        transaction.commit().unwrap();
        let result = receiver.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
        assert!(matches!(result, Err(RuntimeEventStoreError::Corrupt(message)) if message == "source mutation fence changed"));
        worker.join().unwrap();
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn own858_pg_foreign_search_path_mutates_only_target_schema_fence_and_blocks_cas() {
        let (store_a, fixture_a) = crate::tests::open_real_store();
        let (store_b, fixture_b) = crate::tests::open_real_store();
        let store_a = std::sync::Arc::new(store_a);
        let executor_a = fixture_a.reconnect();
        let executor_b = fixture_b.reconnect();
        let schema_a: String = executor_a.checkout_critical().unwrap().query_one("SELECT current_schema()", &[]).unwrap().get(0);
        let schema_b: String = executor_b.checkout_critical().unwrap().query_one("SELECT current_schema()", &[]).unwrap().get(0);
        assert_ne!(schema_a, schema_b);
        let source_b = store_b.source_mutation_fence().unwrap();
        let mut revision_b = 0;
        for template in [
            "UPDATE %I.%I SET actor='foreign-search-path' WHERE stream_id='858-cross-schema'",
            "DELETE FROM %I.%I WHERE stream_id='858-cross-schema'",
            "TRUNCATE %I.%I",
        ] {
            store_a.append(RuntimeEventInput {
                stream_id: "858-cross-schema".into(), scope: RuntimeEventScope::Session,
                kind: "858.append".into(), status: None, actor: None,
                refs: Vec::new(), payload: serde_json::json!({}),
            }).unwrap();
            let before_a = store_a.source_mutation_fence().unwrap();
            let cursor = store_a.durable_commit_cursor().unwrap();
            let mut connection = executor_b.checkout_critical().unwrap();
            let mut transaction = connection.transaction().unwrap();
            let set_path: String = transaction.query_one("SELECT pg_catalog.format('SET LOCAL search_path TO %I, pg_catalog', $1::TEXT)", &[&schema_b]).unwrap().get(0);
            transaction.batch_execute(&set_path).unwrap();
            assert_eq!(transaction.query_one("SELECT current_schema()", &[]).unwrap().get::<_, String>(0), schema_b);
            let mutation: String = transaction.query_one("SELECT pg_catalog.format($1::TEXT, $2::TEXT, 'runtime_events')", &[&template, &schema_a]).unwrap().get(0);
            transaction.batch_execute(&mutation).unwrap();
            let (sender, receiver) = std::sync::mpsc::channel();
            let worker_store = std::sync::Arc::clone(&store_a);
            let old_source = before_a.clone();
            let worker = std::thread::spawn(move || {
                sender.send(worker_store.compare_and_put_projection_checkpoint_fenced("858-cross-schema-reader", cursor, 0, &serde_json::json!({}), 0, &old_source)).unwrap();
            });
            assert!(matches!(receiver.recv_timeout(std::time::Duration::from_millis(100)), Err(std::sync::mpsc::RecvTimeoutError::Timeout)));
            transaction.commit().unwrap();
            assert!(matches!(receiver.recv_timeout(std::time::Duration::from_secs(10)).unwrap(), Err(RuntimeEventStoreError::Corrupt(message)) if message == "source mutation fence changed"));
            worker.join().unwrap();
            let after_a = store_a.source_mutation_fence().unwrap();
            assert_eq!(after_a.incarnation, before_a.incarnation);
            assert_eq!(after_a.mutation_epoch, before_a.mutation_epoch + 1);
            assert_eq!(store_b.source_mutation_fence().unwrap(), source_b);
            assert!(store_a.projection_checkpoint("858-cross-schema-reader").unwrap().is_none());
            revision_b = store_b.compare_and_put_projection_checkpoint_fenced("858-unchanged-schema-reader", 0, revision_b, &serde_json::json!({}), 0, &source_b).unwrap().revision;
        }
    }
}
