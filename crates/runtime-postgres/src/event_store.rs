//! PostgreSQL Runtime event-store adapter and migration pipeline.

use super::*;

/// Complete PostgreSQL implementation of the Runtime event backend contract.
#[derive(Clone, Debug)]
pub struct PostgresRuntimeEventStore {
    executor: PostgresExecutor,
}

/// Immutable proof written only after a domain-owned RuntimeEvent copy has
/// reached digest equality. It intentionally contains no backend URL or path.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeEventMigrationManifest {
    pub domain: String,
    pub source_digest: String,
    pub target_digest: String,
    pub commit_count: usize,
    pub event_count: usize,
    pub terminal_count: usize,
    pub decision_lease_count: usize,
}

impl PostgresRuntimeEventStore {
    pub fn new(executor: PostgresExecutor) -> RuntimeEventStoreResult<Self> {
        executor.apply_migrations(RUNTIME_EVENT_DOMAIN, RUNTIME_EVENT_MIGRATIONS)?;
        Ok(Self { executor })
    }

    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> RuntimeEventStoreResult<Self> {
        Self::new(PostgresExecutor::connect(config, resolver)?)
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

impl RuntimeEventStoreBackend for PostgresRuntimeEventStore {
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
        let mut connection = self.checkout_event_read()?;
        let rows = pg(connection.query(
            &format!(
                "WITH selected_commits AS (
                    SELECT commit_cursor FROM runtime_commits
                     WHERE commit_cursor>$1
                     ORDER BY commit_cursor ASC LIMIT $2
                 )
                 SELECT event_id, stream_id, sequence, scope, kind, status, actor, payload, refs,
                        created_at_ms, event.commit_cursor, transaction_id, transaction_index,
                        schema_version, idempotency_key
                   FROM runtime_events AS event
                   JOIN selected_commits AS selected
                     ON selected.commit_cursor=event.commit_cursor
                  ORDER BY event.commit_cursor ASC, transaction_index ASC"
            ),
            &[
                &to_i64(cursor, "cursor")?,
                &to_i64(max_commits as u64, "max_commits")?,
            ],
        ))?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            events.push(row_to_event(&row)?);
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
        if max_commits == 0 {
            return Ok(RuntimeProjectionScanPage {
                scanned_through_cursor: cursor,
                ..RuntimeProjectionScanPage::default()
            });
        }
        let mut connection = self.checkout_event_read()?;
        let selected_rows = pg(connection.query(
            "SELECT commit_cursor, transaction_id FROM runtime_commits
              WHERE commit_cursor > $1 ORDER BY commit_cursor ASC LIMIT $2",
            &[
                &to_i64(cursor, "cursor")?,
                &to_i64(max_commits as u64, "max_commits")?,
            ],
        ))?;
        let selected = selected_rows
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
        let events = if interest.events.is_empty() {
            Vec::new()
        } else {
            let interest_json = Value::Array(
                interest
                    .events
                    .iter()
                    .map(|event| {
                        serde_json::json!({
                            "scope": event.scope.as_str(),
                            "kind": event.kind,
                        })
                    })
                    .collect(),
            );
            let rows = pg(connection.query(
                "SELECT event_id, stream_id, sequence, scope, kind, status, actor, payload, refs,
                        created_at_ms, commit_cursor, transaction_id, transaction_index,
                        schema_version, idempotency_key
                   FROM runtime_events AS event
                  WHERE commit_cursor > $1 AND commit_cursor <= $2
                    AND EXISTS (
                        SELECT 1 FROM jsonb_array_elements($3::jsonb) AS wanted
                         WHERE event.scope = wanted->>'scope' AND event.kind = wanted->>'kind'
                    )
                  ORDER BY commit_cursor ASC, transaction_index ASC",
                &[
                    &to_i64(cursor, "cursor")?,
                    &to_i64(*highwater, "highwater")?,
                    &interest_json,
                ],
            ))?;
            rows_to_events(rows)?
        };
        Ok(build_projection_scan_page(
            cursor, selected, events, max_events, max_bytes,
        ))
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

    fn compare_and_put_projection_checkpoint(
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

    fn list_stream_page_desc(
        &self,
        stream_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
        let offset = to_i64(offset as u64, "offset").map_err(|error| error.to_string())?;
        pg(connection.query(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 ORDER BY sequence DESC LIMIT $2 OFFSET $3"),
            &[&stream_id, &limit, &offset],
        ))
        .and_then(rows_to_events)
        .map_err(|error| error.to_string())
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

    fn export_migration_snapshot(&self) -> RuntimeEventStoreResult<RuntimeEventStoreSnapshot> {
        let mut connection = self.executor.checkout_background()?;
        export_postgres_migration_snapshot(&mut connection)
    }

    fn import_migration_snapshot(
        &self,
        snapshot: &RuntimeEventStoreSnapshot,
    ) -> RuntimeEventStoreResult<()> {
        let mut connection = self.executor.checkout_background()?;
        import_postgres_migration_snapshot(&mut connection, snapshot)
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
pub fn copy_quiesced_runtime_event_store(
    source: &RuntimeEventStore,
    target: &RuntimeEventStore,
    manifest_path: impl AsRef<Path>,
) -> RuntimeEventStoreResult<RuntimeEventMigrationManifest> {
    let snapshot = source.export_migration_snapshot()?;
    snapshot.validate()?;
    let source_digest = snapshot.canonical_digest()?;
    target.import_migration_snapshot(&snapshot)?;
    let target_snapshot = target.export_migration_snapshot()?;
    let target_digest = target_snapshot.canonical_digest()?;
    if source_digest != target_digest {
        return Err(RuntimeEventStoreError::Corrupt(
            "runtime event migration digest mismatch".to_string(),
        ));
    }
    let manifest = RuntimeEventMigrationManifest {
        domain: RUNTIME_EVENT_DOMAIN.to_string(),
        source_digest,
        target_digest,
        commit_count: snapshot.commits.len(),
        event_count: snapshot.events.len(),
        terminal_count: snapshot.session_outbox.len(),
        decision_lease_count: snapshot.decision_leases.len(),
    };
    write_migration_manifest(manifest_path.as_ref(), &manifest)?;
    Ok(manifest)
}

fn write_migration_manifest(
    manifest_path: &Path,
    manifest: &RuntimeEventMigrationManifest,
) -> RuntimeEventStoreResult<()> {
    if let Some(parent) = manifest_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary_path = PathBuf::from(format!(
        "{}.{}.tmp",
        manifest_path.display(),
        uuid::Uuid::new_v4()
    ));
    fs::write(&temporary_path, serde_json::to_vec_pretty(manifest)?)?;
    fs::rename(temporary_path, manifest_path)?;
    Ok(())
}

fn export_postgres_migration_snapshot(
    connection: &mut PostgresConnection,
) -> RuntimeEventStoreResult<RuntimeEventStoreSnapshot> {
    let commits = pg(connection.query(
        "SELECT commit_cursor, transaction_id, request_hash, created_at_ms
           FROM runtime_commits ORDER BY commit_cursor ASC",
        &[],
    ))?
    .iter()
    .map(|row| {
        Ok(RuntimeEventCommitSnapshot {
            commit_cursor: from_i64(pg(row.try_get(0))?, "commit_cursor")?,
            transaction_id: pg(row.try_get(1))?,
            request_hash: pg(row.try_get(2))?,
            created_at_ms: from_i64(pg(row.try_get(3))?, "created_at_ms")?,
        })
    })
    .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
    let events = rows_to_events(pg(connection.query(
        &format!("SELECT {EVENT_COLUMNS} FROM runtime_events ORDER BY commit_cursor ASC, transaction_index ASC"),
        &[],
    ))?)?;
    let transaction_streams = pg(connection.query(
        "SELECT transaction_id, stream_id, expected_revision, committed_revision
           FROM runtime_transaction_streams ORDER BY transaction_id ASC, stream_id ASC",
        &[],
    ))?
    .iter()
    .map(|row| {
        Ok(RuntimeEventTransactionStreamSnapshot {
            transaction_id: pg(row.try_get(0))?,
            stream_id: pg(row.try_get(1))?,
            expected_revision: from_i64(pg(row.try_get(2))?, "expected_revision")?,
            committed_revision: from_i64(pg(row.try_get(3))?, "committed_revision")?,
        })
    })
    .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
    let stream_heads = pg(connection.query(
        "SELECT stream_id, revision FROM runtime_stream_heads ORDER BY stream_id ASC",
        &[],
    ))?
    .iter()
    .map(|row| {
        Ok(RuntimeEventStreamHeadSnapshot {
            stream_id: pg(row.try_get(0))?,
            revision: from_i64(pg(row.try_get(1))?, "revision")?,
        })
    })
    .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
    let session_outbox = pg(connection.query(
        "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                input_claim_revision, status,
                attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                last_error, materialized_at, revision
           FROM runtime_session_outbox ORDER BY terminal_id ASC",
        &[],
    ))?
    .iter()
    .map(row_to_runtime_session_outbox)
    .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
    let decision_leases = pg(connection.query(
        "SELECT lease_id, principal_id, review_id, action, scope, evidence_digest,
                credential_epoch, consumed_at_ms
           FROM runtime_consumed_decision_leases ORDER BY lease_id ASC",
        &[],
    ))?
    .iter()
    .map(|row| {
        Ok(RuntimeDecisionLeaseSnapshot {
            lease_id: pg(row.try_get(0))?,
            principal_id: pg(row.try_get(1))?,
            review_id: pg(row.try_get(2))?,
            action: pg(row.try_get(3))?,
            scope: pg(row.try_get(4))?,
            evidence_digest: pg(row.try_get(5))?,
            credential_epoch: from_i64(pg(row.try_get(6))?, "credential_epoch")?,
            consumed_at_ms: from_i64(pg(row.try_get(7))?, "consumed_at_ms")?,
        })
    })
    .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
    let snapshot = RuntimeEventStoreSnapshot {
        commits,
        events,
        transaction_streams,
        stream_heads,
        session_outbox,
        decision_leases,
    };
    snapshot.validate()?;
    Ok(snapshot)
}

fn import_postgres_migration_snapshot(
    connection: &mut PostgresConnection,
    snapshot: &RuntimeEventStoreSnapshot,
) -> RuntimeEventStoreResult<()> {
    snapshot.validate()?;
    let snapshot = canonical_snapshot(snapshot);
    let mut tx = pg(connection.transaction())?;
    for table in [
        "runtime_commits",
        "runtime_events",
        "runtime_transaction_streams",
        "runtime_stream_heads",
        "runtime_session_outbox",
        "runtime_consumed_decision_leases",
    ] {
        let row = pg(tx.query_one(&format!("SELECT COUNT(*) FROM {table}"), &[]))?;
        let count = from_i64(pg(row.try_get(0))?, "target row count")?;
        if count != 0 {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "runtime event migration target table `{table}` is not empty"
            )));
        }
    }
    for commit in &snapshot.commits {
        pg(tx.execute(
            "INSERT INTO runtime_commits(commit_cursor, transaction_id, request_hash, created_at_ms)
             VALUES ($1,$2,$3,$4)",
            &[
                &to_i64(commit.commit_cursor, "commit_cursor")?,
                &commit.transaction_id,
                &commit.request_hash,
                &to_i64(commit.created_at_ms, "created_at_ms")?,
            ],
        ))?;
    }
    for event in &snapshot.events {
        let refs = serde_json::to_value(&event.refs)?;
        let activity_binding = event.activity_binding();
        let root_execution_id = activity_binding
            .as_ref()
            .map(|binding| binding.root_execution_id.as_str());
        let activity_id = activity_binding
            .as_ref()
            .map(|binding| binding.activity_id.as_str());
        pg(tx.execute(
            "INSERT INTO runtime_events (event_id, stream_id, sequence, scope, kind, status, actor,
                payload, refs, created_at_ms, commit_cursor, transaction_id, transaction_index,
                schema_version, idempotency_key, root_execution_id, activity_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
            &[
                &event.event_id,
                &event.stream_id,
                &to_i64(event.sequence, "sequence")?,
                &event.scope.as_str(),
                &event.kind,
                &event.status,
                &event.actor,
                &event.payload,
                &refs,
                &to_i64(event.created_at_ms, "created_at_ms")?,
                &to_i64(event.commit_cursor, "commit_cursor")?,
                &event.transaction_id,
                &i64::from(event.transaction_index),
                &i64::from(event.schema_version),
                &event.idempotency_key,
                &root_execution_id,
                &activity_id,
            ],
        ))?;
    }
    for stream in &snapshot.transaction_streams {
        pg(tx.execute(
            "INSERT INTO runtime_transaction_streams
             (transaction_id, stream_id, expected_revision, committed_revision)
             VALUES ($1,$2,$3,$4)",
            &[
                &stream.transaction_id,
                &stream.stream_id,
                &to_i64(stream.expected_revision, "expected_revision")?,
                &to_i64(stream.committed_revision, "committed_revision")?,
            ],
        ))?;
    }
    for head in &snapshot.stream_heads {
        pg(tx.execute(
            "INSERT INTO runtime_stream_heads(stream_id, revision) VALUES ($1,$2)",
            &[&head.stream_id, &to_i64(head.revision, "revision")?],
        ))?;
    }
    for terminal in &snapshot.session_outbox {
        let next_attempt_at = terminal
            .next_attempt_at_ms
            .map(|value| to_i64(value, "next_attempt_at"))
            .transpose()?;
        let claim_expires_at = terminal
            .claim_expires_at_ms
            .map(|value| to_i64(value, "claim_expires_at"))
            .transpose()?;
        let materialized_at = terminal
            .materialized_at_ms
            .map(|value| to_i64(value, "materialized_at"))
            .transpose()?;
        let session_generation = terminal
            .session_generation
            .map(|value| to_i64(value, "session_generation"))
            .transpose()?;
        let input_claim_revision = terminal
            .input_claim_revision
            .map(|value| to_i64(value, "input_claim_revision"))
            .transpose()?;
        let input_sequence = terminal
            .input_sequence
            .map(|value| to_i64(value, "input_sequence"))
            .transpose()?;
        pg(tx.execute(
            "INSERT INTO runtime_session_outbox
             (terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
              request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
              input_claim_revision, status, attempts,
              next_attempt_at, claim_owner, claim_expires_at, failure_class, last_error,
              materialized_at, revision)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)",
            &[
                &terminal.terminal_id,
                &terminal.message_id,
                &terminal.session_id,
                &to_i64(terminal.commit_cursor, "commit_cursor")?,
                &terminal.payload_ref,
                &terminal.execution_id,
                &terminal.turn_id,
                &terminal.request_id,
                &session_generation,
                &input_sequence,
                &terminal.input_claim_owner,
                &terminal.input_claim_token,
                &input_claim_revision,
                &terminal.status,
                &i64::from(terminal.attempts),
                &next_attempt_at,
                &terminal.claim_owner,
                &claim_expires_at,
                &terminal.failure_class,
                &terminal.last_error,
                &materialized_at,
                &to_i64(terminal.revision, "revision")?,
            ],
        ))?;
    }
    for lease in &snapshot.decision_leases {
        pg(tx.execute(
            "INSERT INTO runtime_consumed_decision_leases
             (lease_id, principal_id, review_id, action, scope, evidence_digest, credential_epoch, consumed_at_ms)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            &[
                &lease.lease_id,
                &lease.principal_id,
                &lease.review_id,
                &lease.action,
                &lease.scope,
                &lease.evidence_digest,
                &to_i64(lease.credential_epoch, "credential_epoch")?,
                &to_i64(lease.consumed_at_ms, "consumed_at_ms")?,
            ],
        ))?;
    }
    if let Some(commit) = snapshot.commits.last() {
        pg(tx.query_one(
            "SELECT setval(pg_get_serial_sequence('runtime_commits', 'commit_cursor'), $1, true)",
            &[&to_i64(commit.commit_cursor, "commit_cursor")?],
        ))?;
    }
    pg(tx.commit())?;
    Ok(())
}

fn canonical_snapshot(snapshot: &RuntimeEventStoreSnapshot) -> RuntimeEventStoreSnapshot {
    let mut canonical = snapshot.clone();
    canonical.commits.sort_by_key(|commit| commit.commit_cursor);
    canonical
        .events
        .sort_by_key(|event| (event.commit_cursor, event.transaction_index));
    canonical.transaction_streams.sort_by(|left, right| {
        (&left.transaction_id, &left.stream_id).cmp(&(&right.transaction_id, &right.stream_id))
    });
    canonical
        .stream_heads
        .sort_by(|left, right| left.stream_id.cmp(&right.stream_id));
    canonical
        .session_outbox
        .sort_by(|left, right| left.terminal_id.cmp(&right.terminal_id));
    canonical
        .decision_leases
        .sort_by(|left, right| left.lease_id.cmp(&right.lease_id));
    canonical
}

fn append_transaction_in_tx(
    tx: &mut PostgresTransaction<'_>,
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
        let refs = serde_json::to_value(&input.event.refs)?;
        let activity_binding = input.event.activity_binding();
        let root_execution_id = activity_binding
            .as_ref()
            .map(|binding| binding.root_execution_id.as_str());
        let activity_id = activity_binding
            .as_ref()
            .map(|binding| binding.activity_id.as_str());
        pg(tx.execute(
            "INSERT INTO runtime_events (event_id, stream_id, sequence, scope, kind, status, actor,
                payload, refs, created_at_ms, commit_cursor, transaction_id, transaction_index,
                schema_version, idempotency_key, root_execution_id, activity_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
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
            ],
        ))?;
        event_ids.push(event_id);
    }
    let mut stream_revisions = Vec::with_capacity(request.expected_streams.len());
    for stream in &request.expected_streams {
        let committed_revision = stream.expected_revision
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
            && !events.is_empty()
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
