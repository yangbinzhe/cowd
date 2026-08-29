//! SQLite Runtime event adapter and SQL-specific migration helpers.

use super::domain::{
    hash_bytes, request_hash_with_terminal, validate_decision_lease_claims, validate_event,
    validate_fenced_terminal, validate_transaction,
};
use super::*;

#[derive(Debug)]
pub(super) struct SqliteRuntimeEventStore {
    executor: SqliteExecutor,
}

impl SqliteRuntimeEventStore {
    pub(super) fn try_open(path: impl AsRef<Path>) -> RuntimeEventStoreResult<Self> {
        let path = path.as_ref().to_path_buf();
        let handle = StorageHandle::sqlite(
            "runtime_events",
            path.clone(),
            "runtime",
            "runtime_event_executor",
        );
        let executor = SqliteExecutor::for_handle(&handle)?;
        let mut conn = executor.checkout()?;
        configure_connection(&conn, false)?;
        migrate_schema(&mut conn)?;
        Ok(Self { executor })
    }

    pub(super) fn try_open_in_memory() -> RuntimeEventStoreResult<Self> {
        let executor = SqliteExecutor::in_memory("runtime-event-store")?;
        let mut conn = executor.checkout()?;
        configure_connection(&conn, true)?;
        migrate_schema(&mut conn)?;
        Ok(Self { executor })
    }

    pub(super) fn checkout_event_connection(
        &self,
    ) -> RuntimeEventStoreResult<SqliteConnectionLease> {
        let busy_timeout_ms = matches!(
            RuntimeEventStore::current_projection_work_class(),
            Some(RuntimeProjectionWorkClass::Background)
        )
        .then_some(BACKGROUND_PROJECTION_BUSY_TIMEOUT_MS);
        self.executor
            .checkout_with_busy_timeout(busy_timeout_ms)
            .map_err(RuntimeEventStoreError::from)
    }

    /// Compatibility convenience for existing single-stream producers.
    ///
    /// New graph/goal lifecycle code must use `append_transaction` with an
    /// explicit expected revision and stable transaction id.
    pub fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String> {
        self.append_single(input).map_err(|error| error.to_string())
    }

    fn append_single(
        &self,
        input: RuntimeEventInput,
    ) -> RuntimeEventStoreResult<DurableRuntimeEvent> {
        validate_event(&input)?;
        let mut conn = self.checkout_event_connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let expected_revision = stream_head(&tx, &input.stream_id)?;
        let request = AppendTransactionRequest {
            transaction_id: format!("runtime-tx-{}", uuid::Uuid::new_v4()),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: input.stream_id.clone(),
                expected_revision,
            }],
            events: vec![input.into()],
        };
        let receipt = append_transaction_in_tx(&tx, &request, None)?;
        tx.commit()?;
        load_transaction_events(&conn, &receipt.transaction_id)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                RuntimeEventStoreError::Corrupt("committed transaction has no event".to_string())
            })
    }

    pub fn append_transaction(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        validate_transaction(&request)?;
        let mut conn = self.checkout_event_connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let receipt = append_transaction_in_tx(&tx, &request, None)?;
        tx.commit()?;
        Ok(receipt)
    }

    pub fn append_transaction_with_terminal(
        &self,
        request: AppendTransactionRequest,
        terminal: SessionTerminalInput,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let mut conn = self.checkout_event_connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let receipt = append_transaction_in_tx(&tx, &request, Some(&terminal))?;
        tx.commit()?;
        Ok(receipt)
    }

    /// Atomically records a previously verified human decision lease.  This
    /// is deliberately narrower than the generic event transaction API: it
    /// cannot create lifecycle events and a duplicate lease is rejected.
    pub(crate) fn consume_verified_decision_lease(
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
        if lease_id.trim().is_empty()
            || principal_id.trim().is_empty()
            || review_id.trim().is_empty()
            || action.trim().is_empty()
            || scope.trim().is_empty()
            || evidence_digest.trim().is_empty()
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "decision lease consumption requires non-empty bound claims".to_string(),
            ));
        }
        let mut conn = self.checkout_event_connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO runtime_consumed_decision_leases \
             (lease_id, principal_id, review_id, action, scope, evidence_digest, credential_epoch, consumed_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                lease_id,
                principal_id,
                review_id,
                action,
                scope,
                evidence_digest,
                credential_epoch as i64,
                consumed_at_ms as i64,
            ],
        )?;
        if inserted == 0 {
            return Err(RuntimeEventStoreError::DecisionLeaseAlreadyConsumed {
                lease_id: lease_id.to_string(),
            });
        }
        tx.commit()?;
        Ok(())
    }

    /// Commit lifecycle events and consume one already-verified human lease in
    /// the same SQLite transaction.  A release decision is never allowed to
    /// consume authorization first and mutate a projection later: either both
    /// durable effects become visible or neither does.
    pub(crate) fn append_transaction_with_verified_decision_lease(
        &self,
        request: AppendTransactionRequest,
        lease: &crate::VerifiedDecisionLease,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        validate_decision_lease_claims(
            lease.lease_id(),
            lease.principal_id(),
            lease.review_id(),
            lease.action(),
            lease.scope(),
            lease.evidence_digest(),
        )?;
        validate_transaction(&request)?;
        let mut conn = self.checkout_event_connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let receipt = append_transaction_in_tx(&tx, &request, None)?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO runtime_consumed_decision_leases \
             (lease_id, principal_id, review_id, action, scope, evidence_digest, credential_epoch, consumed_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                lease.lease_id(),
                lease.principal_id(),
                lease.review_id(),
                lease.action(),
                lease.scope(),
                lease.evidence_digest(),
                lease.credential_epoch() as i64,
                now_ms() as i64,
            ],
        )?;
        if inserted == 0 {
            // A retry of the exact committed transaction is safe only when
            // the stored lease claims are identical. Any other replay is an
            // authorization error, even if the event transaction happens to
            // have an idempotent key collision.
            let existing = tx.query_row(
                "SELECT principal_id, review_id, action, scope, evidence_digest, credential_epoch \
                     FROM runtime_consumed_decision_leases WHERE lease_id = ?1",
                params![lease.lease_id()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )?;
            let matches = existing.0 == lease.principal_id()
                && existing.1 == lease.review_id()
                && existing.2 == lease.action()
                && existing.3 == lease.scope()
                && existing.4 == lease.evidence_digest()
                && existing.5 == lease.credential_epoch() as i64;
            if !receipt.duplicate || !matches {
                return Err(RuntimeEventStoreError::DecisionLeaseAlreadyConsumed {
                    lease_id: lease.lease_id().to_string(),
                });
            }
        }
        tx.commit()?;
        Ok(receipt)
    }

    pub fn append_batch_if_revision(
        &self,
        stream_id: impl Into<String>,
        expected_revision: u64,
        transaction_id: impl Into<String>,
        events: Vec<RuntimeTransactionEventInput>,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let stream_id = stream_id.into();
        if events
            .iter()
            .any(|event| event.event.stream_id != stream_id)
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "single-stream batch contains an event for another stream".to_string(),
            ));
        }
        self.append_transaction(AppendTransactionRequest {
            transaction_id: transaction_id.into(),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id,
                expected_revision,
            }],
            events,
        })
    }

    pub fn events_after_cursor(
        &self,
        cursor: u64,
        max_commits: usize,
    ) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>> {
        if max_commits == 0 {
            return Ok(Vec::new());
        }
        let conn = self.checkout_event_connection()?;
        let mut stmt = conn.prepare(&format!(
            "{} WHERE commit_cursor IN (
                    SELECT commit_cursor FROM runtime_commits
                     WHERE commit_cursor > ?1
                     ORDER BY commit_cursor ASC LIMIT ?2
                )
                ORDER BY commit_cursor ASC, transaction_index ASC",
            event_select()
        ))?;
        let events = stmt
            .query_map(params![cursor as i64, max_commits as i64], row_to_event)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(group_committed_events(events))
    }

    pub fn projection_scan_page(
        &self,
        cursor: u64,
        interest: &RuntimeProjectionInterest,
        max_commits: usize,
        max_events: usize,
        max_bytes: usize,
    ) -> RuntimeEventStoreResult<RuntimeProjectionScanPage> {
        if max_commits == 0 {
            return Ok(RuntimeProjectionScanPage {
                scanned_through_cursor: cursor,
                ..RuntimeProjectionScanPage::default()
            });
        }
        let conn = self.checkout_event_connection()?;
        let selected = {
            let mut statement = conn.prepare(
                "SELECT commit_cursor, transaction_id FROM runtime_commits
                  WHERE commit_cursor > ?1 ORDER BY commit_cursor ASC LIMIT ?2",
            )?;
            let selected = statement
                .query_map(params![cursor as i64, max_commits as i64], |row| {
                    Ok((row.get::<_, i64>(0)? as u64, row.get::<_, String>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            selected
        };
        let Some((highwater, _)) = selected.last() else {
            return Ok(RuntimeProjectionScanPage {
                scanned_through_cursor: cursor,
                ..RuntimeProjectionScanPage::default()
            });
        };
        let events = if interest.events.is_empty() {
            Vec::new()
        } else {
            let predicates = std::iter::repeat_n("(scope = ? AND kind = ?)", interest.events.len())
                .collect::<Vec<_>>()
                .join(" OR ");
            let mut values = vec![
                SqliteValue::Integer(cursor as i64),
                SqliteValue::Integer(*highwater as i64),
            ];
            for event in &interest.events {
                values.push(SqliteValue::Text(event.scope.as_str().to_string()));
                values.push(SqliteValue::Text(event.kind.clone()));
            }
            let mut statement = conn.prepare(&format!(
                "{} WHERE commit_cursor > ? AND commit_cursor <= ? AND ({predicates})
                  ORDER BY commit_cursor ASC, transaction_index ASC",
                event_select()
            ))?;
            let events = statement
                .query_map(params_from_iter(values), row_to_event)?
                .collect::<Result<Vec<_>, _>>()?;
            events
        };
        Ok(build_projection_scan_page(
            cursor, selected, events, max_events, max_bytes,
        ))
    }

    pub fn projection_checkpoint(
        &self,
        projection_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeProjectionCheckpoint>> {
        validate_projection_id(projection_id)?;
        let conn = self.checkout_event_connection()?;
        conn.query_row(
            "SELECT projection_id, source_cursor, revision, payload, updated_at_ms
               FROM runtime_projection_checkpoints WHERE projection_id=?1",
            params![projection_id],
            row_to_projection_checkpoint,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn projection_checkpoints_with_prefix(
        &self,
        prefix: &str,
    ) -> RuntimeEventStoreResult<Vec<RuntimeProjectionCheckpoint>> {
        if prefix.trim().is_empty() {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "projection checkpoint prefix must not be empty".to_string(),
            ));
        }
        let conn = self.checkout_event_connection()?;
        let escaped = prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("{escaped}%");
        let mut statement = conn.prepare(
            "SELECT projection_id, source_cursor, revision, payload, updated_at_ms
               FROM runtime_projection_checkpoints
              WHERE projection_id LIKE ?1 ESCAPE '\\'
              ORDER BY projection_id ASC",
        )?;
        let checkpoints = statement
            .query_map(params![pattern], row_to_projection_checkpoint)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(RuntimeEventStoreError::from)?;
        Ok(checkpoints)
    }

    pub fn put_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        payload: &serde_json::Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        validate_projection_id(projection_id)?;
        let payload_json = serde_json::to_string(payload)?;
        let mut conn = self.checkout_event_connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let current = tx
            .query_row(
                "SELECT projection_id, source_cursor, revision, payload, updated_at_ms
                   FROM runtime_projection_checkpoints WHERE projection_id=?1",
                params![projection_id],
                row_to_projection_checkpoint,
            )
            .optional()?;
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
            tx.execute(
                "UPDATE runtime_projection_checkpoints
                    SET source_cursor=?1, revision=revision+1, payload=?2, updated_at_ms=?3
                  WHERE projection_id=?4",
                params![
                    source_cursor as i64,
                    payload_json,
                    updated_at_ms as i64,
                    projection_id
                ],
            )?;
        } else {
            tx.execute(
                "INSERT INTO runtime_projection_checkpoints
                    (projection_id, source_cursor, revision, payload, updated_at_ms)
                 VALUES (?1, ?2, 1, ?3, ?4)",
                params![
                    projection_id,
                    source_cursor as i64,
                    payload_json,
                    updated_at_ms as i64
                ],
            )?;
        }
        let checkpoint = tx.query_row(
            "SELECT projection_id, source_cursor, revision, payload, updated_at_ms
               FROM runtime_projection_checkpoints WHERE projection_id=?1",
            params![projection_id],
            row_to_projection_checkpoint,
        )?;
        tx.commit()?;
        Ok(checkpoint)
    }

    pub fn compare_and_put_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &serde_json::Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        validate_projection_id(projection_id)?;
        let payload_json = serde_json::to_string(payload)?;
        let mut conn = self.checkout_event_connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let current = tx
            .query_row(
                "SELECT projection_id, source_cursor, revision, payload, updated_at_ms
                   FROM runtime_projection_checkpoints WHERE projection_id=?1",
                params![projection_id],
                row_to_projection_checkpoint,
            )
            .optional()?;
        match current {
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
                tx.execute(
                    "UPDATE runtime_projection_checkpoints
                        SET source_cursor=?1, revision=revision+1, payload=?2, updated_at_ms=?3
                      WHERE projection_id=?4 AND revision=?5",
                    params![
                        source_cursor as i64,
                        payload_json,
                        updated_at_ms as i64,
                        projection_id,
                        expected_revision as i64
                    ],
                )?;
            }
            None => {
                if expected_revision != 0 {
                    return Err(RuntimeEventStoreError::StaleRevision {
                        stream_id: format!("projection:{projection_id}"),
                        expected: expected_revision,
                        actual: 0,
                    });
                }
                tx.execute(
                    "INSERT INTO runtime_projection_checkpoints
                        (projection_id, source_cursor, revision, payload, updated_at_ms)
                     VALUES (?1, ?2, 1, ?3, ?4)",
                    params![
                        projection_id,
                        source_cursor as i64,
                        payload_json,
                        updated_at_ms as i64
                    ],
                )?;
            }
        }
        let checkpoint = tx.query_row(
            "SELECT projection_id, source_cursor, revision, payload, updated_at_ms
               FROM runtime_projection_checkpoints WHERE projection_id=?1",
            params![projection_id],
            row_to_projection_checkpoint,
        )?;
        tx.commit()?;
        Ok(checkpoint)
    }

    pub fn delete_projection_checkpoint(
        &self,
        projection_id: &str,
    ) -> RuntimeEventStoreResult<bool> {
        validate_projection_id(projection_id)?;
        let conn = self.checkout_event_connection()?;
        Ok(conn.execute(
            "DELETE FROM runtime_projection_checkpoints WHERE projection_id=?1",
            params![projection_id],
        )? > 0)
    }

    pub fn event_by_idempotency_key(
        &self,
        stream_id: &str,
        idempotency_key: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>> {
        let conn = self.checkout_event_connection()?;
        conn.query_row(
            &format!(
                "{} WHERE stream_id = ?1 AND idempotency_key = ?2",
                event_select()
            ),
            params![stream_id, idempotency_key],
            row_to_event,
        )
        .optional()
        .map_err(RuntimeEventStoreError::from)
    }

    pub fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64> {
        let conn = self.checkout_event_connection()?;
        stream_head(&conn, stream_id)
    }

    pub fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.query_events(
            &format!(
                "{} WHERE stream_id = ?1 ORDER BY sequence ASC",
                event_select()
            ),
            params![stream_id],
        )
        .map_err(|error| error.to_string())
    }

    pub fn list_scope_page_asc(
        &self,
        scope: RuntimeEventScope,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let (after_cursor, after_index) = after_position.unwrap_or_default();
        self.query_events(
            &format!(
                "{} WHERE scope = ?1
                 AND (?2 = 0 OR commit_cursor > ?2
                      OR (commit_cursor = ?2 AND transaction_index > ?3))
                 ORDER BY commit_cursor ASC, transaction_index ASC
                 LIMIT ?4",
                event_select()
            ),
            params![
                scope.as_str(),
                after_cursor as i64,
                after_index as i64,
                limit as i64
            ],
        )
        .map_err(|error| error.to_string())
    }

    pub fn list_scope_stream_prefix_page_asc(
        &self,
        scope: RuntimeEventScope,
        stream_prefix: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if stream_prefix.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let (after_cursor, after_index) = after_position.unwrap_or_default();
        self.query_events(
            &format!(
                "{} WHERE scope = ?1
                 AND substr(stream_id, 1, length(?2)) = ?2
                 AND (?3 = 0 OR commit_cursor > ?3
                      OR (commit_cursor = ?3 AND transaction_index > ?4))
                 ORDER BY commit_cursor ASC, transaction_index ASC
                 LIMIT ?5",
                event_select()
            ),
            params![
                scope.as_str(),
                stream_prefix,
                after_cursor as i64,
                after_index as i64,
                limit as i64
            ],
        )
        .map_err(|error| error.to_string())
    }

    pub fn list_stream_page_desc(
        &self,
        stream_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.query_events(
            &format!(
                "{} WHERE stream_id = ?1 ORDER BY sequence DESC LIMIT ?2 OFFSET ?3",
                event_select()
            ),
            params![stream_id, limit as i64, offset as i64],
        )
        .map_err(|error| error.to_string())
    }

    pub fn stream_event_count(&self, stream_id: &str) -> Result<usize, String> {
        let conn = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM runtime_events WHERE stream_id = ?1",
                params![stream_id],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        usize::try_from(count).map_err(|_| "runtime stream event count overflow".to_string())
    }

    /// Resolve the canonical graph streams that produced terminal work for a
    /// session. The terminal request is the durable bridge from a session
    /// input to its graph; callers must not reconstruct this relation from
    /// transcript text or a client-side naming convention.
    pub fn execution_events_for_session(
        &self,
        session_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if session_id.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let direct_refs = self.events_for_ref("session", session_id, after_position, limit)?;
        let terminal_requests = self.events_for_ref_kind(
            "session",
            session_id,
            "runtime.session.terminal_requested",
            after_position,
            limit,
        )?;
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
            related.extend(self.list_stream(&graph_id)?);
            let lineage_stream = format!("execution-lineage:{graph_id}");
            let lineage_events = self.list_stream(&lineage_stream)?;
            for event in &lineage_events {
                if event.kind != "execution.lineage.child_registered.v1" {
                    continue;
                }
                if let Some(child_id) = event
                    .payload
                    .get("child_execution_id")
                    .and_then(serde_json::Value::as_str)
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

    pub fn events_for_root_execution(
        &self,
        root_execution_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.events_for_activity_identity_column(
            "root_execution_id",
            root_execution_id,
            after_position,
            limit,
        )
    }

    pub fn events_for_root_execution_kind(
        &self,
        root_execution_id: &str,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if root_execution_id.trim().is_empty() || kind.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let mut values = vec![
            rusqlite::types::Value::Text(root_execution_id.to_string()),
            rusqlite::types::Value::Text(kind.to_string()),
        ];
        let mut sql = format!(
            "{} WHERE root_execution_id = ? AND kind = ?",
            event_select()
        );
        if let Some((cursor, transaction_index)) = after_position {
            sql.push_str(
                " AND (commit_cursor > ? OR
                       (commit_cursor = ? AND transaction_index > ?))",
            );
            let cursor = i64::try_from(cursor)
                .map_err(|_| "execution scope cursor exceeds SQLite range".to_string())?;
            values.push(cursor.into());
            values.push(cursor.into());
            values.push(i64::from(transaction_index).into());
        }
        sql.push_str(" ORDER BY commit_cursor ASC, transaction_index ASC LIMIT ?");
        let limit = i64::try_from(limit)
            .map_err(|_| "execution scope limit exceeds SQLite range".to_string())?;
        values.push(limit.into());
        self.query_events(&sql, params_from_iter(values))
            .map_err(|error| error.to_string())
    }

    pub fn events_for_activity(
        &self,
        activity_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.events_for_activity_identity_column("activity_id", activity_id, after_position, limit)
    }

    fn events_for_activity_identity_column(
        &self,
        column: &'static str,
        identity: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if identity.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        debug_assert!(matches!(column, "root_execution_id" | "activity_id"));
        let mut values = vec![rusqlite::types::Value::Text(identity.to_string())];
        let mut sql = format!("{} WHERE {column} = ?", event_select());
        if let Some((cursor, transaction_index)) = after_position {
            sql.push_str(
                " AND (commit_cursor > ? OR
                       (commit_cursor = ? AND transaction_index > ?))",
            );
            let cursor = i64::try_from(cursor)
                .map_err(|_| "execution scope cursor exceeds SQLite range".to_string())?;
            values.push(cursor.into());
            values.push(cursor.into());
            values.push(i64::from(transaction_index).into());
        }
        sql.push_str(" ORDER BY commit_cursor ASC, transaction_index ASC LIMIT ?");
        let limit = i64::try_from(limit)
            .map_err(|_| "execution scope limit exceeds SQLite range".to_string())?;
        values.push(limit.into());
        self.query_events(&sql, params_from_iter(values))
            .map_err(|error| error.to_string())
    }

    fn events_for_ref(
        &self,
        ref_kind: &str,
        ref_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if ref_kind.trim().is_empty() || ref_id.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let (after_cursor, after_index) = after_position.unwrap_or_default();
        self.query_events(
            &format!(
                "{} WHERE event_id IN (
                    SELECT event_id FROM runtime_event_refs
                    WHERE ref_kind = ?1 AND ref_id = ?2
                )
                AND (?3 = 0 OR commit_cursor > ?3
                     OR (commit_cursor = ?3 AND transaction_index > ?4))
                ORDER BY commit_cursor ASC, transaction_index ASC
                LIMIT ?5",
                event_select()
            ),
            params![
                ref_kind,
                ref_id,
                after_cursor as i64,
                after_index as i64,
                limit as i64
            ],
        )
        .map_err(|error| error.to_string())
    }

    fn events_for_ref_kind(
        &self,
        ref_kind: &str,
        ref_id: &str,
        event_kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if ref_kind.trim().is_empty()
            || ref_id.trim().is_empty()
            || event_kind.trim().is_empty()
            || limit == 0
        {
            return Ok(Vec::new());
        }
        let (after_cursor, after_index) = after_position.unwrap_or_default();
        self.query_events(
            &format!(
                "{} WHERE event_id IN (
                    SELECT event_id FROM runtime_event_refs
                    WHERE ref_kind = ?1 AND ref_id = ?2
                )
                AND kind = ?3
                AND (?4 = 0 OR commit_cursor > ?4
                     OR (commit_cursor = ?4 AND transaction_index > ?5))
                ORDER BY commit_cursor ASC, transaction_index ASC
                LIMIT ?6",
                event_select()
            ),
            params![
                ref_kind,
                ref_id,
                event_kind,
                after_cursor as i64,
                after_index as i64,
                limit as i64
            ],
        )
        .map_err(|error| error.to_string())
    }

    pub fn list_scope(
        &self,
        scope: RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.query_events(
            &format!(
                "{} WHERE scope = ?1 ORDER BY commit_cursor DESC, transaction_index DESC LIMIT ?2",
                event_select()
            ),
            params![scope.as_str(), limit as i64],
        )
        .map_err(|error| error.to_string())
    }

    pub fn list_scope_kind_page_asc(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let (after_cursor, after_index) = after_position.unwrap_or_default();
        self.query_events(
            &format!(
                "{} WHERE scope = ?1 AND kind = ?2
                 AND (?3 = 0 OR commit_cursor > ?3
                      OR (commit_cursor = ?3 AND transaction_index > ?4))
                 ORDER BY commit_cursor ASC, transaction_index ASC LIMIT ?5",
                event_select()
            ),
            params![
                scope.as_str(),
                kind,
                after_cursor as i64,
                after_index as i64,
                limit as i64
            ],
        )
        .map_err(|error| error.to_string())
    }

    pub fn stream_ids_for_scope(
        &self,
        scope: RuntimeEventScope,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        let conn = self.checkout_event_connection()?;
        let mut statement = conn.prepare(
            "SELECT stream_id FROM runtime_events
             WHERE scope = ?1
             GROUP BY stream_id
             ORDER BY MAX(commit_cursor) ASC, stream_id ASC",
        )?;
        let stream_ids = statement
            .query_map(params![scope.as_str()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(RuntimeEventStoreError::from)?;
        Ok(stream_ids)
    }

    pub fn stream_ids_for_scope_kind_at_sequence_page(
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
        let sequence = i64::try_from(sequence).map_err(|_| {
            RuntimeEventStoreError::Corrupt(format!(
                "runtime event sequence `{sequence}` exceeds SQLite range"
            ))
        })?;
        let (after_cursor, after_stream_id) = after.unwrap_or_default();
        let conn = self.checkout_event_connection()?;
        let mut statement = conn.prepare(
            "SELECT stream_id, commit_cursor FROM runtime_events
             WHERE scope = ?1 AND kind = ?2 AND sequence = ?3
               AND (?4 = 0 OR commit_cursor > ?4
                    OR (commit_cursor = ?4 AND stream_id > ?5))
             ORDER BY commit_cursor ASC, stream_id ASC LIMIT ?6",
        )?;
        let rows = statement
            .query_map(
                params![
                    scope.as_str(),
                    kind,
                    sequence,
                    after_cursor as i64,
                    after_stream_id,
                    limit as i64
                ],
                |row| {
                    let stream_id = row.get::<_, String>(0)?;
                    let commit_cursor = row.get::<_, i64>(1)?;
                    Ok((stream_id, commit_cursor.max(0) as u64))
                },
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(RuntimeEventStoreError::from)?;
        Ok(rows)
    }

    pub fn stream_ids_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        let sequence = i64::try_from(sequence).map_err(|_| {
            RuntimeEventStoreError::Corrupt(format!(
                "runtime event sequence `{sequence}` exceeds SQLite range"
            ))
        })?;
        let conn = self.checkout_event_connection()?;
        let mut statement = conn.prepare(
            "SELECT stream_id FROM runtime_events
             WHERE scope = ?1 AND kind = ?2 AND sequence = ?3
             ORDER BY commit_cursor ASC, stream_id ASC",
        )?;
        let stream_ids = statement
            .query_map(params![scope.as_str(), kind, sequence], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(RuntimeEventStoreError::from)?;
        Ok(stream_ids)
    }

    pub fn latest_stream_statuses_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<(String, Option<String>)>> {
        let sequence = i64::try_from(sequence).map_err(|_| {
            RuntimeEventStoreError::Corrupt(format!(
                "runtime event sequence `{sequence}` exceeds SQLite range"
            ))
        })?;
        let conn = self.checkout_event_connection()?;
        let mut statement = conn.prepare(
            "WITH candidates AS (
                 SELECT stream_id FROM runtime_events
                  WHERE scope=?1 AND kind=?2 AND sequence=?3
             ),
             latest AS (
                 SELECT event.stream_id, event.status,
                        ROW_NUMBER() OVER (
                            PARTITION BY event.stream_id
                            ORDER BY event.sequence DESC
                        ) AS rank
                   FROM runtime_events AS event
                   JOIN candidates USING(stream_id)
             )
             SELECT stream_id, status FROM latest
              WHERE rank=1 ORDER BY stream_id ASC",
        )?;
        let statuses = statement
            .query_map(params![scope.as_str(), kind, sequence], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(RuntimeEventStoreError::from)?;
        Ok(statuses)
    }

    pub fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.query_events(
            &format!(
                "{} ORDER BY commit_cursor DESC, transaction_index DESC LIMIT ?1",
                event_select()
            ),
            params![limit as i64],
        )
        .map_err(|error| error.to_string())
    }

    pub fn latest_for_stream(
        &self,
        stream_id: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        let conn = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        conn.query_row(
            &format!(
                "{} WHERE stream_id = ?1 ORDER BY sequence DESC LIMIT 1",
                event_select()
            ),
            params![stream_id],
            row_to_event,
        )
        .optional()
        .map_err(|error| error.to_string())
    }

    pub fn latest_for_stream_kind(
        &self,
        stream_id: &str,
        kind: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        let conn = self
            .executor
            .checkout()
            .map_err(|error| error.to_string())?;
        conn.query_row(
            &format!(
                "{} WHERE stream_id = ?1 AND kind = ?2 ORDER BY sequence DESC LIMIT 1",
                event_select()
            ),
            params![stream_id, kind],
            row_to_event,
        )
        .optional()
        .map_err(|error| error.to_string())
    }

    fn query_events<P>(
        &self,
        sql: &str,
        params: P,
    ) -> RuntimeEventStoreResult<Vec<DurableRuntimeEvent>>
    where
        P: rusqlite::Params,
    {
        let conn = self.checkout_event_connection()?;
        let mut stmt = conn.prepare(sql)?;
        let events = stmt
            .query_map(params, row_to_event)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(events)
    }

    /// Insert one terminal delivery exactly once. A duplicate terminal ID is
    /// accepted only when every immutable field matches the committed row.
    #[cfg(any(test, feature = "test-fixtures"))]
    fn enqueue_unfenced_session_terminal_for_test(
        &self,
        terminal_id: &str,
        message_id: &str,
        session_id: &str,
        commit_cursor: u64,
        payload_ref: &str,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        let conn = self.checkout_event_connection()?;
        conn.execute(
            "INSERT INTO runtime_session_outbox
             (terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
              request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
              input_claim_revision, status, revision)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'pending', 0)
             ON CONFLICT(terminal_id) DO NOTHING",
            params![
                terminal_id,
                message_id,
                session_id,
                commit_cursor as i64,
                payload_ref
            ],
        )?;
        let record = query_runtime_session_outbox(&conn, terminal_id)?.ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!(
                "terminal outbox `{terminal_id}` disappeared after enqueue"
            ))
        })?;
        if record.message_id != message_id
            || record.session_id != session_id
            || record.commit_cursor != commit_cursor
            || record.payload_ref != payload_ref
        {
            return Err(RuntimeEventStoreError::TransactionConflict {
                transaction_id: terminal_id.to_string(),
            });
        }
        Ok(record)
    }

    pub fn claim_session_terminals(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        if worker_id.trim().is_empty() || lease_ms == 0 || limit == 0 {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "terminal claim requires worker, lease and limit".to_string(),
            ));
        }
        let mut conn = self.checkout_event_connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let ids = {
            let mut statement = tx.prepare(
                "SELECT terminal_id FROM runtime_session_outbox
                 WHERE (status IN ('pending','retry_scheduled') AND COALESCE(next_attempt_at, 0) <= ?1)
                    OR (status = 'claimed' AND claim_expires_at <= ?1)
                 ORDER BY commit_cursor, terminal_id LIMIT ?2",
            )?;
            let ids = statement
                .query_map(params![now_ms as i64, limit as i64], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            ids
        };
        let expires = now_ms.saturating_add(lease_ms);
        let mut claimed = Vec::new();
        for id in ids {
            let changed = tx.execute(
                "UPDATE runtime_session_outbox SET status='claimed', attempts=attempts+1,
                 claim_owner=?1, claim_expires_at=?2, revision=revision+1
                 WHERE terminal_id=?3 AND ((status IN ('pending','retry_scheduled') AND
                 COALESCE(next_attempt_at,0)<=?4) OR (status='claimed' AND claim_expires_at<=?4))",
                params![worker_id, expires as i64, id, now_ms as i64],
            )?;
            if changed == 1 {
                claimed.push(query_runtime_session_outbox(&tx, &id)?.ok_or_else(|| {
                    RuntimeEventStoreError::Corrupt(format!("claimed terminal `{id}` vanished"))
                })?);
            }
        }
        tx.commit()?;
        Ok(claimed)
    }

    pub fn session_terminal(
        &self,
        terminal_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
        let connection = self.checkout_event_connection()?;
        query_runtime_session_outbox(&connection, terminal_id)
    }

    pub fn has_unsettled_session_terminals(
        &self,
        session_id: &str,
    ) -> RuntimeEventStoreResult<bool> {
        let connection = self.checkout_event_connection()?;
        connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM runtime_session_outbox
                      WHERE session_id=?1 AND status NOT IN ('materialized','suppressed')
                 )",
                params![session_id],
                |row| row.get(0),
            )
            .map_err(RuntimeEventStoreError::from)
    }

    /// Return already materialized terminal commits after a durable runtime
    /// cursor. Gateway uses this for resumable surface streams; the transient
    /// session bus is deliberately not the source of truth for final replies.
    pub fn materialized_session_terminals_after(
        &self,
        session_id: &str,
        after_commit_cursor: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        let conn = self.checkout_event_connection()?;
        let mut statement = conn.prepare(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                    request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                    input_claim_revision, status,
                    attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                    last_error, materialized_at, revision
               FROM runtime_session_outbox
              WHERE session_id=?1
                AND status='materialized'
                AND commit_cursor>?2
              ORDER BY commit_cursor, terminal_id
              LIMIT ?3",
        )?;
        let records = statement
            .query_map(
                params![
                    session_id,
                    after_commit_cursor as i64,
                    limit.clamp(1, 500) as i64,
                ],
                row_to_runtime_session_outbox,
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(RuntimeEventStoreError::from)?;
        Ok(records)
    }

    pub fn session_terminal_health(&self) -> RuntimeEventStoreResult<RuntimeSessionOutboxHealth> {
        let conn = self.checkout_event_connection()?;
        let mut health = RuntimeSessionOutboxHealth::default();
        let mut statement =
            conn.prepare("SELECT status, COUNT(*) FROM runtime_session_outbox GROUP BY status")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })?;
        for row in rows {
            let (status, count) = row?;
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

    pub fn blocked_session_terminals(
        &self,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        let conn = self.checkout_event_connection()?;
        let mut statement = conn.prepare(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                    request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                    input_claim_revision, status,
                    attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                    last_error, materialized_at, revision
               FROM runtime_session_outbox WHERE status='blocked'
               ORDER BY COALESCE(next_attempt_at, 0), commit_cursor, terminal_id LIMIT ?1",
        )?;
        let records = statement
            .query_map(
                params![limit.clamp(1, 500) as i64],
                row_to_runtime_session_outbox,
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(RuntimeEventStoreError::from)?;
        Ok(records)
    }

    pub fn retry_session_terminal(
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
        let conn = self.checkout_event_connection()?;
        let changed = conn.execute(
            "UPDATE runtime_session_outbox SET status='retry_scheduled', next_attempt_at=?1,
             claim_owner=NULL, claim_expires_at=NULL, failure_class=NULL,
             last_error=?2, revision=revision+1 WHERE terminal_id=?3 AND status='blocked'",
            params![
                now_ms as i64,
                format!("manual retry by {actor}: {reason}"),
                terminal_id
            ],
        )?;
        if changed != 1 {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{terminal_id}` is not blocked"
            )));
        }
        query_runtime_session_outbox(&conn, terminal_id)?.ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!("terminal `{terminal_id}` vanished"))
        })
    }

    pub fn adopt_session_terminal_fence(
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
        let mut conn = self.checkout_event_connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let current =
            query_runtime_session_outbox(&tx, &request.terminal_id)?.ok_or_else(|| {
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
        let changed = tx.execute(
            "UPDATE runtime_session_outbox
                SET input_sequence=?1, input_claim_owner=?2, input_claim_token=?3, input_claim_revision=?4,
                    status='pending', next_attempt_at=0, claim_owner=NULL,
                    claim_expires_at=NULL, failure_class=NULL, last_error=NULL,
                    materialized_at=NULL, revision=revision+1
              WHERE terminal_id=?5 AND revision=?6",
            params![
                request.input_sequence as i64,
                request.claim_owner,
                request.claim_token,
                request.claim_revision as i64,
                request.terminal_id,
                request.expected_terminal_revision as i64,
            ],
        )?;
        if changed != 1 {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: format!("session-terminal:{}", request.terminal_id),
                expected: request.expected_terminal_revision,
                actual: current.revision,
            });
        }
        let adopted =
            query_runtime_session_outbox(&tx, &request.terminal_id)?.ok_or_else(|| {
                RuntimeEventStoreError::Corrupt(format!(
                    "terminal `{}` vanished after fence adoption",
                    request.terminal_id
                ))
            })?;
        tx.commit()?;
        Ok(adopted)
    }

    pub fn ack_session_terminal(
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

    pub fn suppress_session_terminal(
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

    #[allow(clippy::too_many_arguments)]
    pub fn fail_session_terminal(
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
        let current = {
            let conn = self.checkout_event_connection()?;
            query_runtime_session_outbox(&conn, terminal_id)?
        }
        .ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!("terminal `{terminal_id}` missing"))
        })?;
        let retry = class == RuntimeSessionOutboxFailureClass::Retryable
            && current.attempts < max_attempts.max(1);
        self.transition_session_terminal(
            terminal_id,
            worker_id,
            expected_revision,
            if retry { "retry_scheduled" } else { "blocked" },
            Some((class.as_str(), error)),
            retry.then_some(retry_at_ms),
            now_ms,
        )
    }

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
        let conn = self.checkout_event_connection()?;
        let (failure_class, last_error) = failure.unzip();
        let changed = conn.execute(
            "UPDATE runtime_session_outbox SET status=?1, next_attempt_at=?2,
             claim_owner=NULL, claim_expires_at=NULL, failure_class=?3, last_error=?4,
             materialized_at=CASE WHEN ?1='materialized' THEN ?5 ELSE materialized_at END,
             revision=revision+1 WHERE terminal_id=?6 AND status='claimed'
             AND claim_owner=?7 AND revision=?8",
            params![
                status,
                retry_at_ms.map(|value| value as i64),
                failure_class,
                last_error,
                now_ms as i64,
                terminal_id,
                worker_id,
                expected_revision as i64,
            ],
        )?;
        if changed != 1 {
            // P4 idempotent terminal acknowledgement: delivery is
            // at-least-once, so a retried ack after materialization must
            // succeed instead of surfacing `expected == actual`.
            let actual_record = query_runtime_session_outbox(&conn, terminal_id)?;
            if let Some(record) = actual_record.as_ref() {
                if status == "materialized"
                    && record.status == "materialized"
                    && record.revision >= expected_revision
                {
                    return Ok(record.clone());
                }
            }
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: format!("terminal:{terminal_id}"),
                expected: expected_revision,
                actual: actual_record.map_or(0, |record| record.revision),
            });
        }
        query_runtime_session_outbox(&conn, terminal_id)?.ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!("terminal `{terminal_id}` vanished"))
        })
    }
}

impl RuntimeEventStoreBackend for SqliteRuntimeEventStore {
    fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String> {
        Self::append(self, input)
    }

    fn append_transaction(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        Self::append_transaction(self, request)
    }

    fn append_transaction_with_terminal(
        &self,
        request: AppendTransactionRequest,
        terminal: SessionTerminalInput,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        Self::append_transaction_with_terminal(self, request, terminal)
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
        Self::consume_verified_decision_lease(
            self,
            lease_id,
            principal_id,
            review_id,
            action,
            scope,
            evidence_digest,
            credential_epoch,
            consumed_at_ms,
        )
    }

    fn append_transaction_with_verified_decision_lease(
        &self,
        request: AppendTransactionRequest,
        lease: &crate::VerifiedDecisionLease,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        Self::append_transaction_with_verified_decision_lease(self, request, lease)
    }

    fn append_batch_if_revision(
        &self,
        stream_id: String,
        expected_revision: u64,
        transaction_id: String,
        events: Vec<RuntimeTransactionEventInput>,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        Self::append_batch_if_revision(self, stream_id, expected_revision, transaction_id, events)
    }

    fn events_after_cursor(
        &self,
        cursor: u64,
        max_commits: usize,
    ) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>> {
        Self::events_after_cursor(self, cursor, max_commits)
    }

    fn projection_scan_page(
        &self,
        cursor: u64,
        interest: &RuntimeProjectionInterest,
        max_commits: usize,
        max_events: usize,
        max_bytes: usize,
    ) -> RuntimeEventStoreResult<RuntimeProjectionScanPage> {
        Self::projection_scan_page(self, cursor, interest, max_commits, max_events, max_bytes)
    }

    fn background_projection_capacity_hint(&self) -> usize {
        self.executor.profile().max_connections as usize
    }

    fn projection_checkpoint(
        &self,
        projection_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeProjectionCheckpoint>> {
        Self::projection_checkpoint(self, projection_id)
    }

    fn projection_checkpoints_with_prefix(
        &self,
        prefix: &str,
    ) -> RuntimeEventStoreResult<Vec<RuntimeProjectionCheckpoint>> {
        Self::projection_checkpoints_with_prefix(self, prefix)
    }

    fn put_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        payload: &serde_json::Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        Self::put_projection_checkpoint(self, projection_id, source_cursor, payload, updated_at_ms)
    }

    fn compare_and_put_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &serde_json::Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        Self::compare_and_put_projection_checkpoint(
            self,
            projection_id,
            source_cursor,
            expected_revision,
            payload,
            updated_at_ms,
        )
    }

    fn delete_projection_checkpoint(&self, projection_id: &str) -> RuntimeEventStoreResult<bool> {
        Self::delete_projection_checkpoint(self, projection_id)
    }

    fn event_by_idempotency_key(
        &self,
        stream_id: &str,
        idempotency_key: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>> {
        Self::event_by_idempotency_key(self, stream_id, idempotency_key)
    }

    fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64> {
        Self::stream_revision(self, stream_id)
    }

    fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::list_stream(self, stream_id)
    }

    fn list_stream_page_desc(
        &self,
        stream_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::list_stream_page_desc(self, stream_id, limit, offset)
    }

    fn stream_event_count(&self, stream_id: &str) -> Result<usize, String> {
        Self::stream_event_count(self, stream_id)
    }

    fn execution_events_for_session(
        &self,
        session_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::execution_events_for_session(self, session_id, after_position, limit)
    }

    fn events_for_root_execution(
        &self,
        root_execution_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::events_for_root_execution(self, root_execution_id, after_position, limit)
    }

    fn events_for_root_execution_kind(
        &self,
        root_execution_id: &str,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::events_for_root_execution_kind(self, root_execution_id, kind, after_position, limit)
    }

    fn events_for_activity(
        &self,
        activity_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::events_for_activity(self, activity_id, after_position, limit)
    }

    fn list_scope(
        &self,
        scope: RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::list_scope(self, scope, limit)
    }

    fn list_scope_page_asc(
        &self,
        scope: RuntimeEventScope,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::list_scope_page_asc(self, scope, after_position, limit)
    }

    fn list_scope_stream_prefix_page_asc(
        &self,
        scope: RuntimeEventScope,
        stream_prefix: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::list_scope_stream_prefix_page_asc(self, scope, stream_prefix, after_position, limit)
    }

    fn list_scope_kind_page_asc(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::list_scope_kind_page_asc(self, scope, kind, after_position, limit)
    }

    fn stream_ids_for_scope(
        &self,
        scope: RuntimeEventScope,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        Self::stream_ids_for_scope(self, scope)
    }

    fn stream_ids_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        Self::stream_ids_for_scope_kind_at_sequence(self, scope, kind, sequence)
    }

    fn stream_ids_for_scope_kind_at_sequence_page(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
        after: Option<(u64, String)>,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<(String, u64)>> {
        Self::stream_ids_for_scope_kind_at_sequence_page(self, scope, kind, sequence, after, limit)
    }

    fn latest_stream_statuses_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<(String, Option<String>)>> {
        Self::latest_stream_statuses_for_scope_kind_at_sequence(self, scope, kind, sequence)
    }

    fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
        Self::all_events(self, limit)
    }

    fn latest_for_stream(&self, stream_id: &str) -> Result<Option<DurableRuntimeEvent>, String> {
        Self::latest_for_stream(self, stream_id)
    }

    fn latest_for_stream_kind(
        &self,
        stream_id: &str,
        kind: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        Self::latest_for_stream_kind(self, stream_id, kind)
    }

    fn enqueue_session_terminal(
        &self,
        terminal_id: &str,
        message_id: &str,
        session_id: &str,
        commit_cursor: u64,
        payload_ref: &str,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        #[cfg(any(test, feature = "test-fixtures"))]
        {
            Self::enqueue_unfenced_session_terminal_for_test(
                self,
                terminal_id,
                message_id,
                session_id,
                commit_cursor,
                payload_ref,
            )
        }
        #[cfg(not(any(test, feature = "test-fixtures")))]
        {
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
    }

    fn claim_session_terminals(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        Self::claim_session_terminals(self, worker_id, now_ms, lease_ms, limit)
    }

    fn session_terminal(
        &self,
        terminal_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
        Self::session_terminal(self, terminal_id)
    }

    fn has_unsettled_session_terminals(&self, session_id: &str) -> RuntimeEventStoreResult<bool> {
        Self::has_unsettled_session_terminals(self, session_id)
    }

    fn materialized_session_terminals_after(
        &self,
        session_id: &str,
        after_commit_cursor: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        Self::materialized_session_terminals_after(self, session_id, after_commit_cursor, limit)
    }

    fn session_terminal_health(&self) -> RuntimeEventStoreResult<RuntimeSessionOutboxHealth> {
        Self::session_terminal_health(self)
    }

    fn blocked_session_terminals(
        &self,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        Self::blocked_session_terminals(self, limit)
    }

    fn retry_session_terminal(
        &self,
        terminal_id: &str,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        Self::retry_session_terminal(self, terminal_id, actor, reason, now_ms)
    }

    fn adopt_session_terminal_fence(
        &self,
        request: &RuntimeSessionTerminalFenceAdoption,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        Self::adopt_session_terminal_fence(self, request)
    }

    fn ack_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        Self::ack_session_terminal(self, terminal_id, worker_id, expected_revision, now_ms)
    }

    fn suppress_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        reason: &str,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        Self::suppress_session_terminal(
            self,
            terminal_id,
            worker_id,
            expected_revision,
            reason,
            now_ms,
        )
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
        Self::fail_session_terminal(
            self,
            terminal_id,
            worker_id,
            expected_revision,
            class,
            error,
            retry_at_ms,
            max_attempts,
            now_ms,
        )
    }

    fn export_migration_snapshot(&self) -> RuntimeEventStoreResult<RuntimeEventStoreSnapshot> {
        let conn = self.executor.checkout()?;
        export_sqlite_migration_snapshot(&conn)
    }

    fn import_migration_snapshot(
        &self,
        snapshot: &RuntimeEventStoreSnapshot,
    ) -> RuntimeEventStoreResult<()> {
        let mut conn = self.executor.checkout()?;
        import_sqlite_migration_snapshot(&mut conn, snapshot)
    }
}

fn export_sqlite_migration_snapshot(
    conn: &Connection,
) -> RuntimeEventStoreResult<RuntimeEventStoreSnapshot> {
    let commits = conn
        .prepare(
            "SELECT commit_cursor, transaction_id, request_hash, created_at_ms
               FROM runtime_commits ORDER BY commit_cursor ASC",
        )?
        .query_map([], |row| {
            Ok(RuntimeEventCommitSnapshot {
                commit_cursor: row.get::<_, i64>(0)? as u64,
                transaction_id: row.get(1)?,
                request_hash: row.get(2)?,
                created_at_ms: row.get::<_, i64>(3)? as u64,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let events = conn
        .prepare(&format!(
            "{} ORDER BY commit_cursor ASC, transaction_index ASC",
            event_select()
        ))?
        .query_map([], row_to_event)?
        .collect::<Result<Vec<_>, _>>()?;
    let transaction_streams = conn
        .prepare(
            "SELECT transaction_id, stream_id, expected_revision, committed_revision
               FROM runtime_transaction_streams ORDER BY transaction_id ASC, stream_id ASC",
        )?
        .query_map([], |row| {
            Ok(RuntimeEventTransactionStreamSnapshot {
                transaction_id: row.get(0)?,
                stream_id: row.get(1)?,
                expected_revision: row.get::<_, i64>(2)? as u64,
                committed_revision: row.get::<_, i64>(3)? as u64,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let stream_heads = conn
        .prepare("SELECT stream_id, revision FROM runtime_stream_heads ORDER BY stream_id ASC")?
        .query_map([], |row| {
            Ok(RuntimeEventStreamHeadSnapshot {
                stream_id: row.get(0)?,
                revision: row.get::<_, i64>(1)? as u64,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let session_outbox = conn
        .prepare(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                    request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                    input_claim_revision, status,
                    attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                    last_error, materialized_at, revision
               FROM runtime_session_outbox ORDER BY terminal_id ASC",
        )?
        .query_map([], row_to_runtime_session_outbox)?
        .collect::<Result<Vec<_>, _>>()?;
    let decision_leases = conn
        .prepare(
            "SELECT lease_id, principal_id, review_id, action, scope, evidence_digest,
                    credential_epoch, consumed_at_ms
               FROM runtime_consumed_decision_leases ORDER BY lease_id ASC",
        )?
        .query_map([], |row| {
            Ok(RuntimeDecisionLeaseSnapshot {
                lease_id: row.get(0)?,
                principal_id: row.get(1)?,
                review_id: row.get(2)?,
                action: row.get(3)?,
                scope: row.get(4)?,
                evidence_digest: row.get(5)?,
                credential_epoch: row.get::<_, i64>(6)? as u64,
                consumed_at_ms: row.get::<_, i64>(7)? as u64,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut snapshot = RuntimeEventStoreSnapshot {
        commits,
        events,
        transaction_streams,
        stream_heads,
        session_outbox,
        decision_leases,
    };
    snapshot.canonicalize();
    Ok(snapshot)
}

fn import_sqlite_migration_snapshot(
    conn: &mut Connection,
    snapshot: &RuntimeEventStoreSnapshot,
) -> RuntimeEventStoreResult<()> {
    validate_migration_snapshot(snapshot)?;
    let mut snapshot = snapshot.clone();
    snapshot.canonicalize();
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    for table in [
        "runtime_commits",
        "runtime_events",
        "runtime_transaction_streams",
        "runtime_stream_heads",
        "runtime_event_refs",
        "runtime_session_outbox",
        "runtime_consumed_decision_leases",
    ] {
        let count = tx.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get::<_, i64>(0)
        })?;
        if count != 0 {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "runtime event migration target table `{table}` is not empty"
            )));
        }
    }
    for commit in &snapshot.commits {
        tx.execute(
            "INSERT INTO runtime_commits(commit_cursor, transaction_id, request_hash, created_at_ms)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                snapshot_i64(commit.commit_cursor, "commit_cursor")?,
                commit.transaction_id,
                commit.request_hash,
                snapshot_i64(commit.created_at_ms, "created_at_ms")?,
            ],
        )?;
    }
    for event in &snapshot.events {
        let activity_binding = event.activity_binding();
        let root_execution_id = activity_binding
            .as_ref()
            .map(|binding| binding.root_execution_id.as_str());
        let activity_id = activity_binding
            .as_ref()
            .map(|binding| binding.activity_id.as_str());
        tx.execute(
            "INSERT INTO runtime_events
             (event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms,
              commit_cursor, transaction_id, transaction_index, schema_version, idempotency_key,
              root_execution_id, activity_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                event.event_id,
                event.stream_id,
                snapshot_i64(event.sequence, "sequence")?,
                event.scope.as_str(),
                event.kind,
                event.status,
                event.actor,
                serde_json::to_string(&event.payload)?,
                serde_json::to_string(&event.refs)?,
                snapshot_i64(event.created_at_ms, "created_at_ms")?,
                snapshot_i64(event.commit_cursor, "commit_cursor")?,
                event.transaction_id,
                i64::from(event.transaction_index),
                i64::from(event.schema_version),
                event.idempotency_key,
                root_execution_id,
                activity_id,
            ],
        )?;
        insert_event_refs(&tx, &event.event_id, &event.refs)?;
    }
    for stream in &snapshot.transaction_streams {
        tx.execute(
            "INSERT INTO runtime_transaction_streams
             (transaction_id, stream_id, expected_revision, committed_revision)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                stream.transaction_id,
                stream.stream_id,
                snapshot_i64(stream.expected_revision, "expected_revision")?,
                snapshot_i64(stream.committed_revision, "committed_revision")?,
            ],
        )?;
    }
    for head in &snapshot.stream_heads {
        tx.execute(
            "INSERT INTO runtime_stream_heads(stream_id, revision) VALUES (?1, ?2)",
            params![head.stream_id, snapshot_i64(head.revision, "revision")?,],
        )?;
    }
    for terminal in &snapshot.session_outbox {
        tx.execute(
            "INSERT INTO runtime_session_outbox
             (terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
              request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
              input_claim_revision, status, attempts,
              next_attempt_at, claim_owner, claim_expires_at, failure_class, last_error,
              materialized_at, revision)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                     ?17, ?18, ?19, ?20, ?21, ?22)",
            params![
                terminal.terminal_id,
                terminal.message_id,
                terminal.session_id,
                snapshot_i64(terminal.commit_cursor, "commit_cursor")?,
                terminal.payload_ref,
                terminal.execution_id,
                terminal.turn_id,
                terminal.request_id,
                terminal
                    .session_generation
                    .map(|value| snapshot_i64(value, "session_generation"))
                    .transpose()?,
                terminal
                    .input_sequence
                    .map(|value| snapshot_i64(value, "input_sequence"))
                    .transpose()?,
                terminal.input_claim_owner,
                terminal.input_claim_token,
                terminal
                    .input_claim_revision
                    .map(|value| snapshot_i64(value, "input_claim_revision"))
                    .transpose()?,
                terminal.status,
                i64::from(terminal.attempts),
                terminal
                    .next_attempt_at_ms
                    .map(|value| snapshot_i64(value, "next_attempt_at"))
                    .transpose()?,
                terminal.claim_owner,
                terminal
                    .claim_expires_at_ms
                    .map(|value| snapshot_i64(value, "claim_expires_at"))
                    .transpose()?,
                terminal.failure_class,
                terminal.last_error,
                terminal
                    .materialized_at_ms
                    .map(|value| snapshot_i64(value, "materialized_at"))
                    .transpose()?,
                snapshot_i64(terminal.revision, "revision")?,
            ],
        )?;
    }
    for lease in &snapshot.decision_leases {
        tx.execute(
            "INSERT INTO runtime_consumed_decision_leases
             (lease_id, principal_id, review_id, action, scope, evidence_digest, credential_epoch, consumed_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                lease.lease_id,
                lease.principal_id,
                lease.review_id,
                lease.action,
                lease.scope,
                lease.evidence_digest,
                snapshot_i64(lease.credential_epoch, "credential_epoch")?,
                snapshot_i64(lease.consumed_at_ms, "consumed_at_ms")?,
            ],
        )?;
    }
    if let Some(max_cursor) = snapshot.commits.last().map(|commit| commit.commit_cursor) {
        tx.execute(
            "DELETE FROM sqlite_sequence WHERE name='runtime_commits'",
            [],
        )?;
        tx.execute(
            "INSERT INTO sqlite_sequence(name, seq) VALUES ('runtime_commits', ?1)",
            params![snapshot_i64(max_cursor, "commit_cursor")?],
        )?;
    }
    tx.commit()?;
    Ok(())
}

pub(super) fn validate_migration_snapshot(
    snapshot: &RuntimeEventStoreSnapshot,
) -> RuntimeEventStoreResult<()> {
    let mut commits = BTreeMap::new();
    let mut commit_cursors = BTreeSet::new();
    for commit in &snapshot.commits {
        if commit.commit_cursor == 0
            || commit.transaction_id.trim().is_empty()
            || commit.request_hash.trim().is_empty()
            || commits
                .insert(commit.transaction_id.as_str(), commit.commit_cursor)
                .is_some()
            || !commit_cursors.insert(commit.commit_cursor)
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "runtime event migration snapshot has an invalid commit".to_string(),
            ));
        }
    }
    let mut event_ids = BTreeSet::new();
    let mut event_sequences = BTreeSet::new();
    let mut event_indexes = BTreeSet::new();
    let mut events_per_transaction_stream = BTreeMap::<(&str, &str), u64>::new();
    for event in &snapshot.events {
        if !event_ids.insert(event.event_id.as_str())
            || commits.get(event.transaction_id.as_str()) != Some(&event.commit_cursor)
            || !commit_cursors.contains(&event.commit_cursor)
            || event.sequence == 0
            || event.schema_version == 0
            || !event_sequences.insert((event.stream_id.as_str(), event.sequence))
            || !event_indexes.insert((event.transaction_id.as_str(), event.transaction_index))
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "runtime event migration snapshot has an invalid event linkage".to_string(),
            ));
        }
        *events_per_transaction_stream
            .entry((event.transaction_id.as_str(), event.stream_id.as_str()))
            .or_default() += 1;
    }
    let mut transaction_streams = BTreeSet::new();
    let mut heads_from_transactions = BTreeMap::<&str, (u64, u64)>::new();
    for stream in &snapshot.transaction_streams {
        if !commits.contains_key(stream.transaction_id.as_str())
            || stream.stream_id.trim().is_empty()
            || !transaction_streams
                .insert((stream.transaction_id.as_str(), stream.stream_id.as_str()))
            || stream.committed_revision
                != stream.expected_revision
                    + events_per_transaction_stream
                        .get(&(stream.transaction_id.as_str(), stream.stream_id.as_str()))
                        .copied()
                        .unwrap_or_default()
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "runtime event migration snapshot has an invalid transaction stream".to_string(),
            ));
        }
        let commit_cursor = commits[stream.transaction_id.as_str()];
        heads_from_transactions
            .entry(stream.stream_id.as_str())
            .and_modify(|current| {
                if commit_cursor > current.0 {
                    *current = (commit_cursor, stream.committed_revision);
                }
            })
            .or_insert((commit_cursor, stream.committed_revision));
    }
    if events_per_transaction_stream
        .keys()
        .any(|stream| !transaction_streams.contains(stream))
    {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "runtime event migration snapshot is missing a transaction stream".to_string(),
        ));
    }
    let mut stream_heads = BTreeMap::new();
    for head in &snapshot.stream_heads {
        if head.stream_id.trim().is_empty()
            || stream_heads
                .insert(head.stream_id.as_str(), head.revision)
                .is_some()
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "runtime event migration snapshot has an invalid stream head".to_string(),
            ));
        }
    }
    if stream_heads.len() != heads_from_transactions.len()
        || heads_from_transactions
            .iter()
            .any(|(stream_id, (_, revision))| stream_heads.get(stream_id) != Some(revision))
    {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "runtime event migration snapshot stream heads do not match events".to_string(),
        ));
    }
    let mut terminal_ids = BTreeSet::new();
    let mut message_ids = BTreeSet::new();
    for terminal in &snapshot.session_outbox {
        if terminal.terminal_id.trim().is_empty()
            || terminal.message_id.trim().is_empty()
            || !terminal_ids.insert(terminal.terminal_id.as_str())
            || !message_ids.insert(terminal.message_id.as_str())
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "runtime event migration snapshot has an invalid terminal outbox row".to_string(),
            ));
        }
    }
    let mut lease_ids = BTreeSet::new();
    for lease in &snapshot.decision_leases {
        if lease.lease_id.trim().is_empty()
            || lease.principal_id.trim().is_empty()
            || lease.review_id.trim().is_empty()
            || lease.action.trim().is_empty()
            || lease.scope.trim().is_empty()
            || lease.evidence_digest.trim().is_empty()
            || !lease_ids.insert(lease.lease_id.as_str())
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "runtime event migration snapshot has an invalid decision lease".to_string(),
            ));
        }
    }
    Ok(())
}

fn snapshot_i64(value: u64, field: &str) -> RuntimeEventStoreResult<i64> {
    i64::try_from(value).map_err(|_| {
        RuntimeEventStoreError::InvalidTransaction(format!(
            "runtime event migration `{field}` exceeds i64"
        ))
    })
}

fn configure_connection(conn: &Connection, in_memory: bool) -> RuntimeEventStoreResult<()> {
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    if !in_memory {
        let mode: String = conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            let activated: String =
                conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
            if !activated.eq_ignore_ascii_case("wal") {
                return Err(RuntimeEventStoreError::Corrupt(format!(
                    "failed to activate WAL journal mode; SQLite selected `{activated}`"
                )));
            }
        }
    }
    Ok(())
}

fn migrate_schema(conn: &mut Connection) -> RuntimeEventStoreResult<()> {
    let current: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if current > STORE_SCHEMA_VERSION {
        return Err(RuntimeEventStoreError::Corrupt(format!(
            "database schema version {current} is newer than supported {STORE_SCHEMA_VERSION}"
        )));
    }
    if current == STORE_SCHEMA_VERSION {
        return validate_schema(conn);
    }
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    create_current_tables(&tx)?;
    migrate_legacy_runtime_events(&tx)?;
    backfill_terminal_session_refs(&tx)?;
    tx.pragma_update(None, "user_version", STORE_SCHEMA_VERSION)?;
    tx.commit()?;
    validate_schema(conn)
}

pub(super) fn create_current_tables(tx: &Transaction<'_>) -> RuntimeEventStoreResult<()> {
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS runtime_events (
            event_id TEXT PRIMARY KEY,
            stream_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            scope TEXT NOT NULL,
            kind TEXT NOT NULL,
            status TEXT,
            actor TEXT,
            payload TEXT NOT NULL,
            refs TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL,
            commit_cursor INTEGER,
            transaction_id TEXT,
            transaction_index INTEGER,
            schema_version INTEGER NOT NULL DEFAULT 1,
            idempotency_key TEXT,
            root_execution_id TEXT,
            activity_id TEXT
        );
        CREATE TABLE IF NOT EXISTS runtime_commits (
            commit_cursor INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id TEXT NOT NULL UNIQUE,
            request_hash TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runtime_transaction_streams (
            transaction_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            expected_revision INTEGER NOT NULL,
            committed_revision INTEGER NOT NULL,
            PRIMARY KEY(transaction_id, stream_id),
            FOREIGN KEY(transaction_id) REFERENCES runtime_commits(transaction_id)
        );
        CREATE TABLE IF NOT EXISTS runtime_stream_heads (
            stream_id TEXT PRIMARY KEY,
            revision INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runtime_event_refs (
            event_id TEXT NOT NULL,
            ref_kind TEXT NOT NULL,
            ref_id TEXT NOT NULL,
            PRIMARY KEY(event_id, ref_kind, ref_id),
            FOREIGN KEY(event_id) REFERENCES runtime_events(event_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS runtime_session_outbox (
            terminal_id TEXT PRIMARY KEY,
            message_id TEXT NOT NULL UNIQUE,
            session_id TEXT NOT NULL,
            commit_cursor INTEGER NOT NULL,
            payload_ref TEXT NOT NULL,
            execution_id TEXT,
            turn_id TEXT,
            request_id TEXT,
            session_generation INTEGER,
            input_sequence INTEGER,
            input_claim_owner TEXT,
            input_claim_token TEXT,
            input_claim_revision INTEGER,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at INTEGER,
            claim_owner TEXT,
            claim_expires_at INTEGER,
            failure_class TEXT,
            last_error TEXT,
            materialized_at INTEGER,
            revision INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS runtime_consumed_decision_leases (
            lease_id TEXT PRIMARY KEY,
            principal_id TEXT NOT NULL,
            review_id TEXT NOT NULL,
            action TEXT NOT NULL,
            scope TEXT NOT NULL,
            evidence_digest TEXT NOT NULL,
            credential_epoch INTEGER NOT NULL,
            consumed_at_ms INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runtime_projection_checkpoints (
            projection_id TEXT PRIMARY KEY,
            source_cursor INTEGER NOT NULL,
            revision INTEGER NOT NULL,
            payload TEXT NOT NULL,
            updated_at_ms INTEGER NOT NULL
        );",
    )?;

    for (column, definition) in [
        ("commit_cursor", "INTEGER"),
        ("transaction_id", "TEXT"),
        ("transaction_index", "INTEGER"),
        ("schema_version", "INTEGER NOT NULL DEFAULT 1"),
        ("idempotency_key", "TEXT"),
        ("root_execution_id", "TEXT"),
        ("activity_id", "TEXT"),
    ] {
        if !table_has_column(tx, "runtime_events", column)? {
            tx.execute(
                &format!("ALTER TABLE runtime_events ADD COLUMN {column} {definition}"),
                [],
            )?;
        }
    }
    for (column, definition) in [
        ("execution_id", "TEXT"),
        ("turn_id", "TEXT"),
        ("request_id", "TEXT"),
        ("session_generation", "INTEGER"),
        ("input_sequence", "INTEGER"),
        ("input_claim_owner", "TEXT"),
        ("input_claim_token", "TEXT"),
        ("input_claim_revision", "INTEGER"),
    ] {
        if !table_has_column(tx, "runtime_session_outbox", column)? {
            tx.execute(
                &format!("ALTER TABLE runtime_session_outbox ADD COLUMN {column} {definition}"),
                [],
            )?;
        }
    }
    migrate_projection_checkpoints(tx)?;
    Ok(())
}

fn migrate_projection_checkpoints(tx: &Transaction<'_>) -> RuntimeEventStoreResult<()> {
    for (projection_id, kind, cursor_path, payload_expression) in [
        (
            "projector:knowledge-candidate",
            "knowledge.candidate.projector.checkpoint.v1",
            "$.source_cursor",
            "payload",
        ),
        (
            "projector:evolution-signal",
            "evolution.signal.projector.checkpoint.v1",
            "$.source_cursor",
            "payload",
        ),
        (
            "projector:outcome",
            "runtime.outcome.projector.checkpoint.v1",
            "$.checkpoint.source_cursor",
            "payload",
        ),
        (
            "projector:mission-evidence",
            "mission_evidence.projector.checkpoint.v1",
            "$.projection.source_cursor",
            "json_extract(payload, '$.projection')",
        ),
    ] {
        tx.execute(
            &format!(
                "INSERT OR IGNORE INTO runtime_projection_checkpoints
                    (projection_id, source_cursor, revision, payload, updated_at_ms)
                 SELECT ?1, CAST(json_extract(payload, '{cursor_path}') AS INTEGER), 1,
                        {payload_expression}, created_at_ms
                   FROM runtime_events
                  WHERE kind=?2
                  ORDER BY commit_cursor DESC, transaction_index DESC
                  LIMIT 1"
            ),
            params![projection_id, kind],
        )?;
    }
    // V504/V505 wrote complete live reducer snapshots into immutable event
    // streams. Preserve only the newest non-terminal state for each execution
    // as a mutable checkpoint before removing that derived history.
    tx.execute(
        "INSERT OR IGNORE INTO runtime_projection_checkpoints
            (projection_id, source_cursor, revision, payload, updated_at_ms)
         SELECT 'execution-live:' || json_extract(event.payload, '$.execution_id'),
                event.commit_cursor,
                1,
                event.payload,
                event.created_at_ms
           FROM runtime_events AS event
          WHERE event.kind='execution.live.snapshot.v1'
            AND json_extract(event.payload, '$.execution_id') IS NOT NULL
            AND json_extract(event.payload, '$.live.status')
                NOT IN ('complete', 'error', 'cancelled')
            AND event.commit_cursor = (
                SELECT MAX(candidate.commit_cursor)
                  FROM runtime_events AS candidate
                 WHERE candidate.kind='execution.live.snapshot.v1'
                   AND json_extract(candidate.payload, '$.execution_id')
                       = json_extract(event.payload, '$.execution_id')
            )",
        [],
    )?;
    tx.execute(
        "DELETE FROM runtime_projection_checkpoints
          WHERE projection_id LIKE 'execution-live:%'
            AND json_extract(payload, '$.live.status')
                IN ('complete', 'error', 'cancelled')",
        [],
    )?;
    tx.execute(
        "DELETE FROM runtime_event_refs
          WHERE event_id IN (
              SELECT event_id FROM runtime_events
               WHERE kind LIKE '%.projector.checkpoint.v1'
                  OR kind='execution.live.snapshot.v1'
          )",
        [],
    )?;
    tx.execute(
        "DELETE FROM runtime_events
          WHERE kind LIKE '%.projector.checkpoint.v1'
             OR kind='execution.live.snapshot.v1'",
        [],
    )?;
    tx.execute(
        "DELETE FROM runtime_transaction_streams
          WHERE NOT EXISTS (
              SELECT 1 FROM runtime_events
               WHERE runtime_events.transaction_id=runtime_transaction_streams.transaction_id
          )",
        [],
    )?;
    tx.execute(
        "DELETE FROM runtime_commits
          WHERE NOT EXISTS (
              SELECT 1 FROM runtime_events
               WHERE runtime_events.transaction_id=runtime_commits.transaction_id
          )",
        [],
    )?;
    tx.execute(
        "DELETE FROM runtime_stream_heads
          WHERE NOT EXISTS (
              SELECT 1 FROM runtime_events
               WHERE runtime_events.stream_id=runtime_stream_heads.stream_id
          )",
        [],
    )?;
    Ok(())
}

fn query_runtime_session_outbox(
    conn: &Connection,
    terminal_id: &str,
) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
    conn.query_row(
        "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                input_claim_revision, status,
                attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                last_error, materialized_at, revision
         FROM runtime_session_outbox WHERE terminal_id=?1",
        params![terminal_id],
        row_to_runtime_session_outbox,
    )
    .optional()
    .map_err(Into::into)
}

fn row_to_runtime_session_outbox(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RuntimeSessionOutboxRecord> {
    Ok(RuntimeSessionOutboxRecord {
        terminal_id: row.get(0)?,
        message_id: row.get(1)?,
        session_id: row.get(2)?,
        commit_cursor: row.get::<_, i64>(3)? as u64,
        payload_ref: row.get(4)?,
        execution_id: row.get(5)?,
        turn_id: row.get(6)?,
        request_id: row.get(7)?,
        session_generation: row.get::<_, Option<i64>>(8)?.map(|value| value as u64),
        input_sequence: row.get::<_, Option<i64>>(9)?.map(|value| value as u64),
        input_claim_owner: row.get(10)?,
        input_claim_token: row.get(11)?,
        input_claim_revision: row.get::<_, Option<i64>>(12)?.map(|value| value as u64),
        status: row.get(13)?,
        attempts: row.get::<_, i64>(14)? as u32,
        next_attempt_at_ms: row.get::<_, Option<i64>>(15)?.map(|value| value as u64),
        claim_owner: row.get(16)?,
        claim_expires_at_ms: row.get::<_, Option<i64>>(17)?.map(|value| value as u64),
        failure_class: row.get(18)?,
        last_error: row.get(19)?,
        materialized_at_ms: row.get::<_, Option<i64>>(20)?.map(|value| value as u64),
        revision: row.get::<_, i64>(21)? as u64,
    })
}

fn migrate_legacy_runtime_events(tx: &Transaction<'_>) -> RuntimeEventStoreResult<()> {
    let mut stmt = tx.prepare(
        "SELECT event_id, stream_id, sequence, scope, created_at_ms FROM runtime_events \
         WHERE commit_cursor IS NULL OR transaction_id IS NULL OR transaction_index IS NULL \
         ORDER BY created_at_ms ASC, event_id ASC",
    )?;
    let legacy = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? as u64,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)? as u64,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);

    for (event_id, stream_id, sequence, scope, created_at_ms) in legacy {
        RuntimeEventScope::parse(&scope)?;
        let transaction_id = format!("legacy:{event_id}");
        let request_hash =
            hash_bytes(format!("legacy:{event_id}:{stream_id}:{sequence}").as_bytes());
        tx.execute(
            "INSERT INTO runtime_commits(transaction_id, request_hash, created_at_ms) VALUES (?1, ?2, ?3)",
            params![transaction_id, request_hash, created_at_ms as i64],
        )?;
        let cursor = tx.last_insert_rowid() as u64;
        tx.execute(
            "UPDATE runtime_events SET commit_cursor = ?1, transaction_id = ?2, \
             transaction_index = 0, schema_version = COALESCE(schema_version, 1) WHERE event_id = ?3",
            params![cursor as i64, transaction_id, event_id],
        )?;
        tx.execute(
            "INSERT INTO runtime_transaction_streams \
             (transaction_id, stream_id, expected_revision, committed_revision) VALUES (?1, ?2, ?3, ?4)",
            params![transaction_id, stream_id, sequence.saturating_sub(1) as i64, sequence as i64],
        )?;
    }
    tx.execute(
        "INSERT INTO runtime_stream_heads(stream_id, revision) \
         SELECT stream_id, MAX(sequence) FROM runtime_events GROUP BY stream_id \
         ON CONFLICT(stream_id) DO UPDATE SET revision = MAX(revision, excluded.revision)",
        [],
    )?;
    tx.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_stream_sequence
             ON runtime_events(stream_id, sequence);
         CREATE INDEX IF NOT EXISTS idx_runtime_events_stream_kind_sequence
             ON runtime_events(stream_id, kind, sequence DESC);
         CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_created
            ON runtime_events(scope, created_at_ms);
         CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_commit
            ON runtime_events(scope, commit_cursor, transaction_index);
         CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_kind_commit
            ON runtime_events(scope, kind, commit_cursor, transaction_index);
         CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_stream_commit
            ON runtime_events(scope, stream_id, commit_cursor, transaction_index);
         CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_stream_sequence
            ON runtime_events(scope, stream_id, sequence DESC);
         CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_commit_index
            ON runtime_events(commit_cursor, transaction_index);
         CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_transaction_index
            ON runtime_events(transaction_id, transaction_index);
         CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_stream_idempotency
            ON runtime_events(stream_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
         CREATE INDEX IF NOT EXISTS idx_runtime_events_root_execution_commit
            ON runtime_events(root_execution_id, commit_cursor, transaction_index)
            WHERE root_execution_id IS NOT NULL;
         CREATE INDEX IF NOT EXISTS idx_runtime_events_root_kind_commit
            ON runtime_events(root_execution_id, kind, commit_cursor, transaction_index)
            WHERE root_execution_id IS NOT NULL;
         CREATE INDEX IF NOT EXISTS idx_runtime_events_activity_commit
            ON runtime_events(activity_id, commit_cursor, transaction_index)
            WHERE activity_id IS NOT NULL;
         CREATE INDEX IF NOT EXISTS idx_runtime_commits_cursor
            ON runtime_commits(commit_cursor);
         CREATE INDEX IF NOT EXISTS idx_runtime_event_refs_lookup
            ON runtime_event_refs(ref_kind, ref_id, event_id);
         CREATE INDEX IF NOT EXISTS idx_runtime_consumed_decision_leases_review
            ON runtime_consumed_decision_leases(review_id, action);",
    )?;
    let existing_refs = {
        let mut statement = tx.prepare("SELECT event_id, refs FROM runtime_events")?;
        let refs = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        refs
    };
    for (event_id, refs) in existing_refs {
        let refs = serde_json::from_str::<Vec<RuntimeEventRef>>(&refs)?;
        insert_event_refs(tx, &event_id, &refs)?;
    }
    tx.execute(
        "UPDATE runtime_events
            SET root_execution_id =
                    json_extract(payload, '$._runtime_activity_binding.root_execution_id'),
                activity_id =
                    json_extract(payload, '$._runtime_activity_binding.activity_id')
          WHERE root_execution_id IS NULL OR activity_id IS NULL",
        [],
    )?;
    Ok(())
}

fn backfill_terminal_session_refs(tx: &Transaction<'_>) -> RuntimeEventStoreResult<()> {
    let terminal_events = {
        let mut statement = tx.prepare(
            "SELECT event_id, refs, payload FROM runtime_events
             WHERE kind = 'runtime.session.terminal_requested'",
        )?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows
    };
    for (event_id, refs_json, payload_json) in terminal_events {
        let payload = serde_json::from_str::<serde_json::Value>(&payload_json)?;
        let Some(session_id) = payload
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let mut refs = serde_json::from_str::<Vec<RuntimeEventRef>>(&refs_json)?;
        if !refs
            .iter()
            .any(|reference| reference.kind == "session" && reference.id == session_id)
        {
            refs.push(RuntimeEventRef {
                kind: "session".to_string(),
                id: session_id.to_string(),
            });
            tx.execute(
                "UPDATE runtime_events SET refs = ?1 WHERE event_id = ?2",
                params![serde_json::to_string(&refs)?, event_id],
            )?;
        }
        insert_event_refs(tx, &event_id, &refs)?;
    }
    Ok(())
}

fn validate_schema(conn: &Connection) -> RuntimeEventStoreResult<()> {
    for table in [
        "runtime_events",
        "runtime_commits",
        "runtime_transaction_streams",
        "runtime_stream_heads",
        "runtime_event_refs",
        "runtime_session_outbox",
        "runtime_consumed_decision_leases",
        "runtime_projection_checkpoints",
    ] {
        if !table_exists(conn, table)? {
            return Err(RuntimeEventStoreError::Corrupt(format!(
                "required table `{table}` is missing"
            )));
        }
    }
    let mut stmt = conn.prepare("SELECT DISTINCT scope FROM runtime_events")?;
    let scopes = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    for scope in scopes {
        RuntimeEventScope::parse(&scope)?;
    }
    Ok(())
}

fn append_transaction_in_tx(
    tx: &Transaction<'_>,
    request: &AppendTransactionRequest,
    terminal: Option<&SessionTerminalInput>,
) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
    validate_transaction(request)?;
    if let Some(terminal) = terminal {
        validate_fenced_terminal(terminal)?;
        if serde_json::to_vec(&(request, terminal))?.len() > MAX_TRANSACTION_BYTES {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "serialized terminal transaction exceeds hard limit {MAX_TRANSACTION_BYTES} bytes"
            )));
        }
    }
    let request_hash = request_hash_with_terminal(request, terminal)?;
    if let Some(committed_hash) = tx
        .query_row(
            "SELECT request_hash FROM runtime_commits WHERE transaction_id = ?1",
            params![request.transaction_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
    {
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

    let expected = request
        .expected_streams
        .iter()
        .map(|stream| (stream.stream_id.as_str(), stream.expected_revision))
        .collect::<BTreeMap<_, _>>();
    for stream in &request.expected_streams {
        let actual = stream_head(tx, &stream.stream_id)?;
        if actual != stream.expected_revision {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: stream.stream_id.clone(),
                expected: stream.expected_revision,
                actual,
            });
        }
    }

    let created_at_ms = now_ms();
    tx.execute(
        "INSERT INTO runtime_commits(transaction_id, request_hash, created_at_ms) VALUES (?1, ?2, ?3)",
        params![request.transaction_id, request_hash, created_at_ms as i64],
    )?;
    let commit_cursor = tx.last_insert_rowid() as u64;
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
        let activity_binding = input.event.activity_binding();
        let root_execution_id = activity_binding
            .as_ref()
            .map(|binding| binding.root_execution_id.as_str());
        let activity_id = activity_binding
            .as_ref()
            .map(|binding| binding.activity_id.as_str());
        tx.execute(
            "INSERT INTO runtime_events \
             (event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms, \
              commit_cursor, transaction_id, transaction_index, schema_version, idempotency_key, \
              root_execution_id, activity_id) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                event_id,
                input.event.stream_id,
                sequence as i64,
                input.event.scope.as_str(),
                input.event.kind,
                input.event.status,
                input.event.actor,
                serde_json::to_string(&input.event.payload)?,
                serde_json::to_string(&input.event.refs)?,
                created_at_ms as i64,
                commit_cursor as i64,
                request.transaction_id,
                transaction_index as i64,
                input.schema_version as i64,
                input.idempotency_key,
                root_execution_id,
                activity_id,
            ],
        )?;
        insert_event_refs(tx, &event_id, &input.event.refs)?;
        event_ids.push(event_id);
    }

    let mut stream_revisions = Vec::with_capacity(request.expected_streams.len());
    for stream in &request.expected_streams {
        let committed_revision = stream.expected_revision
            + increments
                .get(stream.stream_id.as_str())
                .copied()
                .unwrap_or_default();
        tx.execute(
            "INSERT INTO runtime_stream_heads(stream_id, revision) VALUES (?1, ?2) \
             ON CONFLICT(stream_id) DO UPDATE SET revision = excluded.revision",
            params![stream.stream_id, committed_revision as i64],
        )?;
        tx.execute(
            "INSERT INTO runtime_transaction_streams \
             (transaction_id, stream_id, expected_revision, committed_revision) VALUES (?1, ?2, ?3, ?4)",
            params![
                request.transaction_id,
                stream.stream_id,
                stream.expected_revision as i64,
                committed_revision as i64,
            ],
        )?;
        stream_revisions.push(CommittedStreamRevision {
            stream_id: stream.stream_id.clone(),
            expected_revision: stream.expected_revision,
            committed_revision,
        });
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
    tx: &Transaction<'_>,
    terminal: &SessionTerminalInput,
    commit_cursor: u64,
) -> RuntimeEventStoreResult<()> {
    tx.execute(
        "INSERT INTO runtime_session_outbox
         (terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
          request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
          input_claim_revision, status, revision)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, 'pending', 0)
         ON CONFLICT(terminal_id) DO NOTHING",
        params![
            terminal.terminal_id,
            terminal.message_id,
            terminal.session_id,
            commit_cursor as i64,
            terminal.payload_ref,
            terminal.execution_id,
            terminal.turn_id,
            terminal.request_id,
            terminal.session_generation.map(|value| value as i64),
            terminal.input_sequence.map(|value| value as i64),
            terminal.input_claim_owner,
            terminal.input_claim_token,
            terminal.input_claim_revision.map(|value| value as i64),
        ],
    )?;
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
    conn: &Connection,
    terminal: &SessionTerminalInput,
    commit_cursor: u64,
) -> RuntimeEventStoreResult<()> {
    let mut statement = conn.prepare(
        "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                input_claim_revision, status, attempts, next_attempt_at, claim_owner,
                claim_expires_at, failure_class, last_error, materialized_at, revision
           FROM runtime_session_outbox WHERE commit_cursor=?1
           ORDER BY terminal_id LIMIT 2",
    )?;
    let records = statement
        .query_map(params![commit_cursor as i64], row_to_runtime_session_outbox)?
        .collect::<Result<Vec<_>, _>>()?;
    if records.len() != 1 {
        return Err(RuntimeEventStoreError::TransactionConflict {
            transaction_id: terminal.terminal_id.clone(),
        });
    }
    let stored = &records[0];
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
    conn: &Connection,
    transaction_id: &str,
    duplicate: bool,
) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
    let (commit_cursor, request_hash) = conn.query_row(
        "SELECT commit_cursor, request_hash FROM runtime_commits WHERE transaction_id = ?1",
        params![transaction_id],
        |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, String>(1)?)),
    )?;
    let mut stream_stmt = conn.prepare(
        "SELECT stream_id, expected_revision, committed_revision FROM runtime_transaction_streams \
         WHERE transaction_id = ?1 ORDER BY stream_id ASC",
    )?;
    let stream_revisions = stream_stmt
        .query_map(params![transaction_id], |row| {
            Ok(CommittedStreamRevision {
                stream_id: row.get(0)?,
                expected_revision: row.get::<_, i64>(1)? as u64,
                committed_revision: row.get::<_, i64>(2)? as u64,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut event_stmt = conn.prepare(
        "SELECT event_id FROM runtime_events WHERE transaction_id = ?1 ORDER BY transaction_index ASC",
    )?;
    let event_ids = event_stmt
        .query_map(params![transaction_id], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AppendTransactionReceipt {
        commit_cursor,
        transaction_id: transaction_id.to_string(),
        request_hash,
        stream_revisions,
        event_ids,
        duplicate,
    })
}

fn insert_event_refs(
    tx: &Transaction<'_>,
    event_id: &str,
    refs: &[RuntimeEventRef],
) -> RuntimeEventStoreResult<()> {
    for reference in refs {
        tx.execute(
            "INSERT OR IGNORE INTO runtime_event_refs(event_id, ref_kind, ref_id)
             VALUES (?1, ?2, ?3)",
            params![event_id, reference.kind, reference.id],
        )?;
    }
    Ok(())
}

fn load_transaction_events(
    conn: &Connection,
    transaction_id: &str,
) -> RuntimeEventStoreResult<Vec<RuntimeEventRecord>> {
    let mut stmt = conn.prepare(&format!(
        "{} WHERE transaction_id = ?1 ORDER BY transaction_index ASC",
        event_select()
    ))?;
    let events = stmt
        .query_map(params![transaction_id], row_to_event)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(events)
}

fn event_select() -> &'static str {
    "SELECT event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms, \
     commit_cursor, transaction_id, transaction_index, schema_version, idempotency_key FROM runtime_events"
}

fn row_to_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<DurableRuntimeEvent> {
    let scope: String = row.get(3)?;
    let payload: String = row.get(7)?;
    let refs: String = row.get(8)?;
    let scope = RuntimeEventScope::parse(&scope).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(DurableRuntimeEvent {
        event_id: row.get(0)?,
        stream_id: row.get(1)?,
        sequence: row.get::<_, i64>(2)? as u64,
        scope,
        kind: row.get(4)?,
        status: row.get(5)?,
        actor: row.get(6)?,
        payload: serde_json::from_str(&payload).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                7,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        refs: serde_json::from_str(&refs).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        created_at_ms: row.get::<_, i64>(9)? as u64,
        commit_cursor: row.get::<_, i64>(10)? as u64,
        transaction_id: row.get(11)?,
        transaction_index: row.get::<_, i64>(12)? as u32,
        schema_version: row.get::<_, i64>(13)? as u32,
        idempotency_key: row.get(14)?,
    })
}

fn row_to_projection_checkpoint(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RuntimeProjectionCheckpoint> {
    let payload: String = row.get(3)?;
    Ok(RuntimeProjectionCheckpoint {
        projection_id: row.get(0)?,
        source_cursor: row.get::<_, i64>(1)? as u64,
        revision: row.get::<_, i64>(2)? as u64,
        payload: serde_json::from_str(&payload).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        updated_at_ms: row.get::<_, i64>(4)? as u64,
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
    max_events: usize,
    max_bytes: usize,
) -> RuntimeProjectionScanPage {
    let mut events_by_commit = group_committed_events(events)
        .into_iter()
        .map(|batch| (batch.commit_cursor, batch.events))
        .collect::<BTreeMap<_, _>>();
    let mut page = RuntimeProjectionScanPage {
        scanned_through_cursor: cursor,
        ..RuntimeProjectionScanPage::default()
    };
    let mut matched_bytes = 0_usize;
    for (commit_cursor, transaction_id) in selected {
        let events = events_by_commit.remove(&commit_cursor).unwrap_or_default();
        let batch_bytes = events.iter().fold(0_usize, |total, event| {
            total.saturating_add(serde_json::to_vec(event).map_or(0, |bytes| bytes.len()))
        });
        if !page.batches.is_empty()
            && (!events.is_empty())
            && (page.matched_events.saturating_add(events.len()) > max_events.max(1)
                || matched_bytes.saturating_add(batch_bytes) > max_bytes.max(1))
        {
            break;
        }
        page.scanned_through_cursor = commit_cursor;
        page.scanned_commits = page.scanned_commits.saturating_add(1);
        if events.is_empty() {
            continue;
        }
        page.matched_events = page.matched_events.saturating_add(events.len());
        matched_bytes = matched_bytes.saturating_add(batch_bytes);
        page.batches.push(CommittedEventBatch {
            commit_cursor,
            transaction_id,
            events,
        });
    }
    page
}

fn stream_head(conn: &Connection, stream_id: &str) -> RuntimeEventStoreResult<u64> {
    Ok(conn
        .query_row(
            "SELECT revision FROM runtime_stream_heads WHERE stream_id = ?1",
            params![stream_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .unwrap_or_default() as u64)
}

fn table_exists(conn: &Connection, table: &str) -> RuntimeEventStoreResult<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![table],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

pub(super) fn table_has_column(
    conn: &Connection,
    table: &str,
    column: &str,
) -> RuntimeEventStoreResult<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns.iter().any(|candidate| candidate == column))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
