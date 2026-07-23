#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]
//! PostgreSQL adapters for Runtime durable domains.
//!
//! This crate owns PostgreSQL SQL and depends on the Runtime backend contract.
//! The `runtime` crate itself remains free of PostgreSQL drivers.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use postgres::Row;
use runtime::task::{TaskKernel, TaskRecord, TaskStoreBackend, TaskStoreSnapshot};
use runtime::{
    AppendTransactionReceipt, AppendTransactionRequest, CommittedEventBatch,
    CommittedStreamRevision, DurableRuntimeEvent, ExpectedStreamRevision,
    RuntimeDecisionLeaseSnapshot, RuntimeEventCommitSnapshot, RuntimeEventInput,
    RuntimeEventRecord, RuntimeEventScope, RuntimeEventStore, RuntimeEventStoreBackend,
    RuntimeEventStoreError, RuntimeEventStoreResult, RuntimeEventStoreSnapshot,
    RuntimeEventStreamHeadSnapshot, RuntimeEventTransactionStreamSnapshot,
    RuntimeSessionOutboxFailureClass, RuntimeSessionOutboxHealth, RuntimeSessionOutboxRecord,
    RuntimeTransactionEventInput, SessionTerminalInput, VerifiedDecisionLease,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use storage::{
    PostgresClient, PostgresConnection, PostgresConnectionConfig, PostgresExecutor,
    PostgresMigrationSpec, PostgresTransaction, SecretRefResolver, StorageError,
};

const RUNTIME_EVENT_DOMAIN: &str = "runtime_event";
const TASK_DOMAIN: &str = "runtime_task";
const MAX_TRANSACTION_EVENTS: usize = 10_000;
const MAX_TRANSACTION_BYTES: usize = 32 * 1024 * 1024;
const EVENT_COLUMNS: &str =
    "event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms, \
    commit_cursor, transaction_id, transaction_index, schema_version, idempotency_key";

const RUNTIME_EVENT_MIGRATIONS: &[PostgresMigrationSpec] = &[PostgresMigrationSpec {
    id: "runtime_event.0001.initial",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 1,
    description: "create durable runtime event ledger",
    statements: &[
        "CREATE TABLE IF NOT EXISTS runtime_commits (
            commit_cursor BIGSERIAL PRIMARY KEY,
            transaction_id TEXT NOT NULL UNIQUE,
            request_hash TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS runtime_events (
            event_id TEXT PRIMARY KEY,
            stream_id TEXT NOT NULL,
            sequence BIGINT NOT NULL,
            scope TEXT NOT NULL,
            kind TEXT NOT NULL,
            status TEXT,
            actor TEXT,
            payload JSONB NOT NULL,
            refs JSONB NOT NULL,
            created_at_ms BIGINT NOT NULL,
            commit_cursor BIGINT NOT NULL,
            transaction_id TEXT NOT NULL REFERENCES runtime_commits(transaction_id),
            transaction_index BIGINT NOT NULL,
            schema_version BIGINT NOT NULL DEFAULT 1,
            idempotency_key TEXT
        )",
        "CREATE TABLE IF NOT EXISTS runtime_transaction_streams (
            transaction_id TEXT NOT NULL REFERENCES runtime_commits(transaction_id),
            stream_id TEXT NOT NULL,
            expected_revision BIGINT NOT NULL,
            committed_revision BIGINT NOT NULL,
            PRIMARY KEY(transaction_id, stream_id)
        )",
        "CREATE TABLE IF NOT EXISTS runtime_stream_heads (
            stream_id TEXT PRIMARY KEY,
            revision BIGINT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS runtime_session_outbox (
            terminal_id TEXT PRIMARY KEY,
            message_id TEXT NOT NULL UNIQUE,
            session_id TEXT NOT NULL,
            commit_cursor BIGINT NOT NULL REFERENCES runtime_commits(commit_cursor),
            payload_ref TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts BIGINT NOT NULL DEFAULT 0,
            next_attempt_at BIGINT,
            claim_owner TEXT,
            claim_expires_at BIGINT,
            failure_class TEXT,
            last_error TEXT,
            materialized_at BIGINT,
            revision BIGINT NOT NULL DEFAULT 0
        )",
        "CREATE TABLE IF NOT EXISTS runtime_consumed_decision_leases (
            lease_id TEXT PRIMARY KEY,
            principal_id TEXT NOT NULL,
            review_id TEXT NOT NULL,
            action TEXT NOT NULL,
            scope TEXT NOT NULL,
            evidence_digest TEXT NOT NULL,
            credential_epoch BIGINT NOT NULL,
            consumed_at_ms BIGINT NOT NULL
        )",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_stream_sequence
            ON runtime_events(stream_id, sequence)",
        "CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_created
            ON runtime_events(scope, created_at_ms)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_commit_index
            ON runtime_events(commit_cursor, transaction_index)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_transaction_index
            ON runtime_events(transaction_id, transaction_index)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_stream_idempotency
            ON runtime_events(stream_id, idempotency_key) WHERE idempotency_key IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_runtime_commits_cursor ON runtime_commits(commit_cursor)",
        "CREATE INDEX IF NOT EXISTS idx_runtime_session_outbox_claim
            ON runtime_session_outbox(status, next_attempt_at, claim_expires_at, commit_cursor)",
        "CREATE INDEX IF NOT EXISTS idx_runtime_consumed_decision_leases_review
            ON runtime_consumed_decision_leases(review_id, action)",
    ],
}];

const TASK_MIGRATIONS: &[PostgresMigrationSpec] = &[PostgresMigrationSpec {
    id: "runtime_task.0001.initial",
    domain: TASK_DOMAIN,
    version: 1,
    description: "create durable task control-plane records",
    statements: &[
        "CREATE TABLE IF NOT EXISTS runtime_tasks (
            task_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL,
            record_json JSONB NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_runtime_tasks_status_created
            ON runtime_tasks(status, created_at_ms DESC, task_id DESC)",
    ],
}];

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

    #[must_use]
    pub fn into_runtime_event_store(self) -> RuntimeEventStore {
        RuntimeEventStore::from_backend(Arc::new(self))
    }
}

impl RuntimeEventStoreBackend for PostgresRuntimeEventStore {
    fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String> {
        validate_event(&input).map_err(|error| error.to_string())?;
        let mut connection = self
            .executor
            .checkout_runtime()
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
        let mut connection = self.executor.checkout_runtime()?;
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
        let mut connection = self.executor.checkout_runtime()?;
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
        let mut connection = self.executor.checkout_runtime()?;
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
        let mut connection = self.executor.checkout_runtime()?;
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
        let mut connection = self.executor.checkout_runtime()?;
        let rows = pg(connection.query(
            "SELECT commit_cursor, transaction_id FROM runtime_commits
             WHERE commit_cursor>$1 ORDER BY commit_cursor ASC LIMIT $2",
            &[
                &to_i64(cursor, "cursor")?,
                &to_i64(max_commits as u64, "max_commits")?,
            ],
        ))?;
        let mut batches = Vec::with_capacity(rows.len());
        for row in rows {
            let commit_cursor = from_i64(pg(row.try_get(0))?, "commit_cursor")?;
            let transaction_id: String = pg(row.try_get(1))?;
            batches.push(CommittedEventBatch {
                commit_cursor,
                events: load_transaction_events(&mut connection, &transaction_id)?,
                transaction_id,
            });
        }
        Ok(batches)
    }

    fn event_by_idempotency_key(
        &self,
        stream_id: &str,
        idempotency_key: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>> {
        let mut connection = self.executor.checkout_runtime()?;
        pg(connection.query_opt(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 AND idempotency_key=$2"),
            &[&stream_id, &idempotency_key],
        ))?
        .map(|row| row_to_event(&row))
        .transpose()
    }

    fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64> {
        let mut connection = self.executor.checkout_runtime()?;
        stream_head(&mut connection, stream_id)
    }

    fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut connection = self
            .executor
            .checkout_runtime()
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
            .executor
            .checkout_runtime()
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
            .executor
            .checkout_runtime()
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
        after_commit_cursor: u64,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if session_id.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let terminal_requests =
            RuntimeEventStoreBackend::list_scope(self, RuntimeEventScope::SessionInput, 10_000)?
                .into_iter()
                .filter(|event| event.kind == "runtime.session.terminal_requested")
                .filter(|event| {
                    event.payload.get("session_id").and_then(Value::as_str) == Some(session_id)
                })
                .collect::<Vec<_>>();
        let graph_ids = terminal_requests
            .iter()
            .flat_map(|event| event.refs.iter())
            .filter(|reference| reference.kind == "execution_graph")
            .map(|reference| reference.id.clone())
            .collect::<BTreeSet<_>>();
        let mut related = terminal_requests;
        let mut pending = graph_ids.into_iter().collect::<VecDeque<_>>();
        let mut visited = BTreeSet::new();
        while let Some(graph_id) = pending.pop_front() {
            if visited.len() >= limit || !visited.insert(graph_id.clone()) {
                continue;
            }
            related.extend(RuntimeEventStoreBackend::list_stream(self, &graph_id)?);
            related.extend(RuntimeEventStoreBackend::list_stream(
                self,
                &format!("execution-live:{graph_id}"),
            )?);
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
            .filter(|event| event.commit_cursor > after_commit_cursor)
            .take(limit)
            .collect())
    }

    fn list_scope(
        &self,
        scope: RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut connection = self
            .executor
            .checkout_runtime()
            .map_err(|error| error.to_string())?;
        pg(connection.query(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE scope=$1 ORDER BY commit_cursor DESC, transaction_index DESC LIMIT $2"),
            &[&scope.as_str(), &to_i64(limit as u64, "limit").map_err(|error| error.to_string())?],
        ))
        .and_then(rows_to_events)
        .map_err(|error| error.to_string())
    }

    fn stream_ids_for_scope(
        &self,
        scope: RuntimeEventScope,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        let mut connection = self.executor.checkout_runtime()?;
        let rows = pg(connection.query(
            "SELECT stream_id FROM runtime_events WHERE scope=$1
             GROUP BY stream_id ORDER BY MAX(commit_cursor) ASC, stream_id ASC",
            &[&scope.as_str()],
        ))?;
        rows.into_iter().map(|row| pg(row.try_get(0))).collect()
    }

    fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut connection = self
            .executor
            .checkout_runtime()
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
            .executor
            .checkout_runtime()
            .map_err(|error| error.to_string())?;
        pg(connection.query_opt(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 ORDER BY sequence DESC LIMIT 1"),
            &[&stream_id],
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
        let mut connection = self.executor.checkout_runtime()?;
        let mut tx = pg(connection.transaction())?;
        insert_terminal_in_tx(
            &mut tx,
            &SessionTerminalInput {
                terminal_id: terminal_id.to_string(),
                message_id: message_id.to_string(),
                session_id: session_id.to_string(),
                payload_ref: payload_ref.to_string(),
            },
            commit_cursor,
        )?;
        let record = query_runtime_session_outbox(&mut tx, terminal_id)?.ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!("terminal outbox `{terminal_id}` disappeared"))
        })?;
        pg(tx.commit())?;
        Ok(record)
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
        let mut connection = self.executor.checkout_runtime()?;
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
                    outbox.commit_cursor, outbox.payload_ref, outbox.status, outbox.attempts,
                    outbox.next_attempt_at, outbox.claim_owner, outbox.claim_expires_at,
                    outbox.failure_class, outbox.last_error, outbox.materialized_at, outbox.revision
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
        let mut connection = self.executor.checkout_runtime()?;
        query_runtime_session_outbox(&mut connection, terminal_id)
    }

    fn materialized_session_terminals_after(
        &self,
        session_id: &str,
        after_commit_cursor: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        let mut connection = self.executor.checkout_runtime()?;
        let rows = pg(connection.query(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, status,
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
        let mut connection = self.executor.checkout_runtime()?;
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
                _ => {}
            }
        }
        Ok(health)
    }

    fn blocked_session_terminals(
        &self,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        let mut connection = self.executor.checkout_runtime()?;
        let rows = pg(connection.query(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, status,
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
        let mut connection = self.executor.checkout_runtime()?;
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
        let mut connection = self.executor.checkout_runtime()?;
        export_postgres_migration_snapshot(&mut connection)
    }

    fn import_migration_snapshot(
        &self,
        snapshot: &RuntimeEventStoreSnapshot,
    ) -> RuntimeEventStoreResult<()> {
        let mut connection = self.executor.checkout_runtime()?;
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
        let mut connection = self.executor.checkout_runtime()?;
        let (failure_class, last_error) = failure.unzip();
        let row = pg(connection.query_opt(
            "UPDATE runtime_session_outbox SET status=$1, next_attempt_at=$2,
             claim_owner=NULL, claim_expires_at=NULL, failure_class=$3, last_error=$4,
             materialized_at=CASE WHEN $1='materialized' THEN $5 ELSE materialized_at END,
             revision=revision+1 WHERE terminal_id=$6 AND status='claimed'
             AND claim_owner=$7 AND revision=$8
             RETURNING terminal_id, message_id, session_id, commit_cursor, payload_ref, status,
                 attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                 last_error, materialized_at, revision",
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
        let actual = query_runtime_session_outbox(&mut connection, terminal_id)?
            .map_or(0, |record| record.revision);
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
        "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, status,
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
        pg(tx.execute(
            "INSERT INTO runtime_events (event_id, stream_id, sequence, scope, kind, status, actor,
                payload, refs, created_at_ms, commit_cursor, transaction_id, transaction_index,
                schema_version, idempotency_key)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
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
        pg(tx.execute(
            "INSERT INTO runtime_session_outbox
             (terminal_id, message_id, session_id, commit_cursor, payload_ref, status, attempts,
              next_attempt_at, claim_owner, claim_expires_at, failure_class, last_error,
              materialized_at, revision)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
            &[
                &terminal.terminal_id,
                &terminal.message_id,
                &terminal.session_id,
                &to_i64(terminal.commit_cursor, "commit_cursor")?,
                &terminal.payload_ref,
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
    let request_hash = request_hash(request)?;
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
            insert_terminal_in_tx(tx, terminal, receipt.commit_cursor)?;
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
        pg(tx.execute(
            "INSERT INTO runtime_events (event_id, stream_id, sequence, scope, kind, status, actor,
                payload, refs, created_at_ms, commit_cursor, transaction_id, transaction_index,
                schema_version, idempotency_key)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
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
         (terminal_id, message_id, session_id, commit_cursor, payload_ref, status, revision)
         VALUES ($1,$2,$3,$4,$5,'pending',0) ON CONFLICT(terminal_id) DO NOTHING",
        &[
            &terminal.terminal_id,
            &terminal.message_id,
            &terminal.session_id,
            &to_i64(commit_cursor, "commit_cursor")?,
            &terminal.payload_ref,
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

fn load_transaction_events(
    client: &mut impl PostgresClient,
    transaction_id: &str,
) -> RuntimeEventStoreResult<Vec<RuntimeEventRecord>> {
    let rows = pg(client.query(
        &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE transaction_id=$1 ORDER BY transaction_index ASC"),
        &[&transaction_id],
    ))?;
    rows_to_events(rows)
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
        "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, status,
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

fn row_to_runtime_session_outbox(row: &Row) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
    Ok(RuntimeSessionOutboxRecord {
        terminal_id: pg(row.try_get(0))?,
        message_id: pg(row.try_get(1))?,
        session_id: pg(row.try_get(2))?,
        commit_cursor: from_i64(pg(row.try_get(3))?, "commit_cursor")?,
        payload_ref: pg(row.try_get(4))?,
        status: pg(row.try_get(5))?,
        attempts: u32::try_from(from_i64(pg(row.try_get(6))?, "attempts")?)
            .map_err(|_| RuntimeEventStoreError::Corrupt("attempts exceeds u32".to_string()))?,
        next_attempt_at_ms: pg(row.try_get::<_, Option<i64>>(7))?
            .map(|value| from_i64(value, "next_attempt_at"))
            .transpose()?,
        claim_owner: pg(row.try_get(8))?,
        claim_expires_at_ms: pg(row.try_get::<_, Option<i64>>(9))?
            .map(|value| from_i64(value, "claim_expires_at"))
            .transpose()?,
        failure_class: pg(row.try_get(10))?,
        last_error: pg(row.try_get(11))?,
        materialized_at_ms: pg(row.try_get::<_, Option<i64>>(12))?
            .map(|value| from_i64(value, "materialized_at"))
            .transpose()?,
        revision: from_i64(pg(row.try_get(13))?, "revision")?,
    })
}

fn validate_transaction(request: &AppendTransactionRequest) -> RuntimeEventStoreResult<()> {
    if request.transaction_id.trim().is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "transaction_id must not be empty".to_string(),
        ));
    }
    if request.events.is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "events must not be empty".to_string(),
        ));
    }
    if request.events.len() > MAX_TRANSACTION_EVENTS {
        return Err(RuntimeEventStoreError::InvalidTransaction(format!(
            "event count exceeds hard limit {MAX_TRANSACTION_EVENTS}"
        )));
    }
    if serde_json::to_vec(request)?.len() > MAX_TRANSACTION_BYTES {
        return Err(RuntimeEventStoreError::InvalidTransaction(format!(
            "serialized transaction exceeds hard limit {MAX_TRANSACTION_BYTES} bytes"
        )));
    }
    let mut expected = BTreeSet::new();
    for stream in &request.expected_streams {
        if stream.stream_id.trim().is_empty() || !expected.insert(stream.stream_id.as_str()) {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "expected streams must be non-empty and unique".to_string(),
            ));
        }
    }
    for event in &request.events {
        validate_event(&event.event)?;
        if event.schema_version == 0 {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "event schema_version must be positive".to_string(),
            ));
        }
        if !expected.contains(event.event.stream_id.as_str()) {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "event stream `{}` has no expected revision",
                event.event.stream_id
            )));
        }
    }
    Ok(())
}

fn validate_event(input: &RuntimeEventInput) -> RuntimeEventStoreResult<()> {
    if input.stream_id.trim().is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "event stream_id must not be empty".to_string(),
        ));
    }
    if input.kind.trim().is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "event kind must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_decision_lease_claims(
    lease_id: &str,
    principal_id: &str,
    review_id: &str,
    action: &str,
    scope: &str,
    evidence_digest: &str,
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
    Ok(())
}

const fn failure_class_as_str(class: RuntimeSessionOutboxFailureClass) -> &'static str {
    match class {
        RuntimeSessionOutboxFailureClass::Retryable => "retryable",
        RuntimeSessionOutboxFailureClass::Permanent => "permanent",
        RuntimeSessionOutboxFailureClass::AuthorizationBlocked => "authorization_blocked",
        RuntimeSessionOutboxFailureClass::CorruptPayload => "corrupt_payload",
    }
}

fn request_hash(request: &AppendTransactionRequest) -> RuntimeEventStoreResult<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(request)?)
    ))
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

/// Complete PostgreSQL implementation of the Task control-plane backend.
///
/// The store locks only the task being updated. Independent task lifecycles
/// can therefore use separate PostgreSQL connections concurrently; task-level
/// transitions remain atomic even across gateway processes.
#[derive(Clone, Debug)]
pub struct PostgresTaskStore {
    executor: PostgresExecutor,
}

/// Immutable proof written only after an explicit quiesced task copy reaches
/// canonical digest equality. It intentionally carries no backend URL/path.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TaskMigrationManifest {
    pub domain: String,
    pub source_digest: String,
    pub target_digest: String,
    pub task_count: usize,
}

impl PostgresTaskStore {
    pub fn new(executor: PostgresExecutor) -> Result<Self, String> {
        executor
            .apply_migrations(TASK_DOMAIN, TASK_MIGRATIONS)
            .map_err(|error| error.to_string())?;
        Ok(Self { executor })
    }

    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> Result<Self, String> {
        Self::new(PostgresExecutor::connect(config, resolver).map_err(|error| error.to_string())?)
    }

    #[must_use]
    pub fn executor(&self) -> &PostgresExecutor {
        &self.executor
    }

    #[must_use]
    pub fn into_task_kernel(self) -> TaskKernel {
        TaskKernel::from_backend(Arc::new(self))
    }
}

impl TaskStoreBackend for PostgresTaskStore {
    fn list(&self) -> Result<Vec<TaskRecord>, String> {
        let mut connection = self
            .executor
            .checkout_runtime()
            .map_err(|error| error.to_string())?;
        let rows = connection
            .query(
                "SELECT record_json FROM runtime_tasks ORDER BY created_at_ms ASC, task_id ASC",
                &[],
            )
            .map_err(|error| error.to_string())?;
        rows.iter().map(task_record_from_row).collect()
    }

    fn get(&self, task_id: &str) -> Result<Option<TaskRecord>, String> {
        let mut connection = self
            .executor
            .checkout_runtime()
            .map_err(|error| error.to_string())?;
        connection
            .query_opt(
                "SELECT record_json FROM runtime_tasks WHERE task_id=$1",
                &[&task_id],
            )
            .map_err(|error| error.to_string())?
            .map(|row| task_record_from_row(&row))
            .transpose()
    }

    fn current(&self) -> Result<Option<TaskRecord>, String> {
        let mut connection = self
            .executor
            .checkout_runtime()
            .map_err(|error| error.to_string())?;
        connection
            .query_opt(
                "SELECT record_json FROM runtime_tasks
                 WHERE status IN ('pending', 'running', 'reviewing')
                 ORDER BY created_at_ms DESC, task_id DESC LIMIT 1",
                &[],
            )
            .map_err(|error| error.to_string())?
            .map(|row| task_record_from_row(&row))
            .transpose()
    }

    fn update_task(
        &self,
        task_id: &str,
        updater: &mut dyn FnMut(Option<TaskRecord>) -> Result<TaskRecord, String>,
    ) -> Result<TaskRecord, String> {
        if task_id.trim().is_empty() {
            return Err("task id is required".to_string());
        }
        let mut connection = self
            .executor
            .checkout_runtime()
            .map_err(|error| error.to_string())?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let lock_key = format!("cowd-runtime-task:{task_id}");
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&lock_key],
            )
            .map_err(|error| error.to_string())?;
        let current = transaction
            .query_opt(
                "SELECT record_json FROM runtime_tasks WHERE task_id=$1 FOR UPDATE",
                &[&task_id],
            )
            .map_err(|error| error.to_string())?
            .map(|row| task_record_from_row(&row))
            .transpose()?;
        let next = updater(current)?;
        validate_task_update(task_id, &next)?;
        let record_json = serde_json::to_value(&next).map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO runtime_tasks
                    (task_id, status, created_at_ms, updated_at_ms, record_json)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT(task_id) DO UPDATE SET
                    status=EXCLUDED.status,
                    created_at_ms=EXCLUDED.created_at_ms,
                    updated_at_ms=EXCLUDED.updated_at_ms,
                    record_json=EXCLUDED.record_json",
                &[
                    &next.id,
                    &next.status.as_str(),
                    &task_time_i64(next.created_at_ms, "created_at_ms")?,
                    &task_time_i64(next.updated_at_ms, "updated_at_ms")?,
                    &record_json,
                ],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(next)
    }

    fn import_migration_snapshot(&self, snapshot: &TaskStoreSnapshot) -> Result<(), String> {
        snapshot.validate()?;
        let mut connection = self
            .executor
            .checkout_runtime()
            .map_err(|error| error.to_string())?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        transaction
            .batch_execute("LOCK TABLE runtime_tasks IN EXCLUSIVE MODE")
            .map_err(|error| error.to_string())?;
        let existing: i64 = transaction
            .query_one("SELECT COUNT(*) FROM runtime_tasks", &[])
            .map_err(|error| error.to_string())?
            .get(0);
        if existing != 0 {
            return Err("task migration target must be empty".to_string());
        }
        for task in &snapshot.tasks {
            let record_json = serde_json::to_value(task).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_tasks
                        (task_id, status, created_at_ms, updated_at_ms, record_json)
                     VALUES ($1, $2, $3, $4, $5)",
                    &[
                        &task.id,
                        &task.status.as_str(),
                        &task_time_i64(task.created_at_ms, "created_at_ms")?,
                        &task_time_i64(task.updated_at_ms, "updated_at_ms")?,
                        &record_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())
    }
}

/// Copy a quiesced Task control plane exactly once, prove canonical digest
/// equality, then atomically write a backend-neutral cutover manifest.
pub fn copy_quiesced_task_kernel(
    source: &TaskKernel,
    target: &TaskKernel,
    manifest_path: impl AsRef<Path>,
) -> Result<TaskMigrationManifest, String> {
    let snapshot = source.export_migration_snapshot()?;
    snapshot.validate()?;
    let source_digest = snapshot.canonical_digest()?;
    target.import_migration_snapshot(&snapshot)?;
    let target_snapshot = target.export_migration_snapshot()?;
    let target_digest = target_snapshot.canonical_digest()?;
    if source_digest != target_digest {
        return Err("task migration digest mismatch".to_string());
    }
    let manifest = TaskMigrationManifest {
        domain: TASK_DOMAIN.to_string(),
        source_digest,
        target_digest,
        task_count: snapshot.tasks.len(),
    };
    write_task_migration_manifest(manifest_path.as_ref(), &manifest)?;
    Ok(manifest)
}

fn task_record_from_row(row: &Row) -> Result<TaskRecord, String> {
    let record_json: Value = row.try_get(0).map_err(|error| error.to_string())?;
    serde_json::from_value(record_json).map_err(|error| error.to_string())
}

fn validate_task_update(task_id: &str, task: &TaskRecord) -> Result<(), String> {
    if task.id.trim().is_empty() || task.id != task_id {
        return Err("task backend updater returned a record for another task id".to_string());
    }
    Ok(())
}

fn task_time_i64(value: u64, field: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("task `{field}` exceeds i64"))
}

fn write_task_migration_manifest(
    manifest_path: &Path,
    manifest: &TaskMigrationManifest,
) -> Result<(), String> {
    if let Some(parent) = manifest_path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let temporary_path = PathBuf::from(format!(
        "{}.{}.tmp",
        manifest_path.display(),
        uuid::Uuid::new_v4()
    ));
    fs::write(
        &temporary_path,
        serde_json::to_vec_pretty(manifest).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    fs::rename(temporary_path, manifest_path).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use runtime::{RuntimeServices, TaskLifecycleEvent, TaskLifecycleKind};
    use storage::StaticSecretRefResolver;

    use super::*;

    fn input(stream_id: &str, scope: RuntimeEventScope, kind: &str) -> RuntimeEventInput {
        RuntimeEventInput {
            stream_id: stream_id.to_string(),
            scope,
            kind: kind.to_string(),
            status: Some("running".to_string()),
            actor: Some("runtime-postgres-test".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({"kind": kind}),
        }
    }

    fn open_real_store() -> Option<(RuntimeEventStore, String)> {
        let url = match std::env::var("COWD_TEST_POSTGRES_URL") {
            Ok(url) if !url.trim().is_empty() => url,
            _ => {
                eprintln!("skipping real PostgreSQL test: COWD_TEST_POSTGRES_URL is not set");
                return None;
            }
        };
        let resolver = StaticSecretRefResolver::new([("test.pg".to_string(), url.clone())]);
        let store = PostgresRuntimeEventStore::connect(
            PostgresConnectionConfig::new("runtime-event-test", "test.pg", "cowd-v569-test"),
            &resolver,
        )
        .expect("postgres runtime event store opens")
        .into_runtime_event_store();
        Some((store, url))
    }

    #[test]
    fn postgres_runtime_event_store_preserves_fences_outbox_restart_and_runtime_composition() {
        let Some((store, url)) = open_real_store() else {
            return;
        };
        let sqlite_source = RuntimeEventStore::try_open_in_memory().expect("SQLite source opens");
        sqlite_source
            .append_transaction(AppendTransactionRequest {
                transaction_id: "copy-source-transaction".to_string(),
                expected_streams: vec![
                    ExpectedStreamRevision {
                        stream_id: "copy:stream".to_string(),
                        expected_revision: 0,
                    },
                    ExpectedStreamRevision {
                        stream_id: "copy:empty-stream".to_string(),
                        expected_revision: 0,
                    },
                ],
                events: vec![input(
                    "copy:stream",
                    RuntimeEventScope::Recovery,
                    "migration.source_seeded",
                )
                .into()],
            })
            .expect("source event");
        let manifest_root = tempfile::tempdir().expect("migration manifest root");
        let manifest_path = manifest_root.path().join("runtime-event-cutover.json");
        let copy = copy_quiesced_runtime_event_store(&sqlite_source, &store, &manifest_path)
            .expect("SQLite to PostgreSQL migration copy");
        assert_eq!(copy.source_digest, copy.target_digest);
        assert!(manifest_path.is_file());
        assert_eq!(
            store
                .export_migration_snapshot()
                .expect("target snapshot")
                .canonical_digest()
                .expect("target digest"),
            sqlite_source
                .export_migration_snapshot()
                .expect("source snapshot")
                .canonical_digest()
                .expect("source digest")
        );
        let store = Arc::new(store);
        store
            .append(input(
                "graph:concurrent",
                RuntimeEventScope::ExecutionGraph,
                "graph.seeded",
            ))
            .expect("seed append");

        let barrier = Arc::new(Barrier::new(2));
        let writers = (0..2)
            .map(|writer| {
                let barrier = Arc::clone(&barrier);
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    barrier.wait();
                    store.append_transaction(AppendTransactionRequest {
                        transaction_id: format!("concurrent-writer-{writer}"),
                        expected_streams: vec![ExpectedStreamRevision {
                            stream_id: "graph:concurrent".to_string(),
                            expected_revision: 1,
                        }],
                        events: vec![RuntimeTransactionEventInput {
                            event: input(
                                "graph:concurrent",
                                RuntimeEventScope::ExecutionGraph,
                                "graph.concurrent",
                            ),
                            idempotency_key: Some(format!("writer-{writer}")),
                            schema_version: 1,
                        }],
                    })
                })
            })
            .collect::<Vec<_>>();
        let outcomes = writers
            .into_iter()
            .map(|writer| writer.join().expect("writer thread"))
            .collect::<Vec<_>>();
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(RuntimeEventStoreError::StaleRevision { .. })
                ))
                .count(),
            1
        );
        assert_eq!(store.stream_revision("graph:concurrent").unwrap(), 2);

        let terminal_receipt = store
            .append_transaction_with_terminal(
                AppendTransactionRequest {
                    transaction_id: "terminal-transaction".to_string(),
                    expected_streams: vec![ExpectedStreamRevision {
                        stream_id: "session-input:real".to_string(),
                        expected_revision: 0,
                    }],
                    events: vec![RuntimeTransactionEventInput {
                        event: input(
                            "session-input:real",
                            RuntimeEventScope::SessionInput,
                            "runtime.session.terminal_requested",
                        ),
                        idempotency_key: Some("terminal-request".to_string()),
                        schema_version: 1,
                    }],
                },
                SessionTerminalInput {
                    terminal_id: "terminal-real".to_string(),
                    message_id: "message-real".to_string(),
                    session_id: "session-real".to_string(),
                    payload_ref: "payload-real".to_string(),
                },
            )
            .expect("terminal transaction");
        assert!(terminal_receipt.commit_cursor > 0);

        let claim_barrier = Arc::new(Barrier::new(2));
        let workers = (0..2)
            .map(|worker| {
                let barrier = Arc::clone(&claim_barrier);
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    barrier.wait();
                    store.claim_session_terminals(&format!("worker-{worker}"), 100, 1_000, 1)
                })
            })
            .collect::<Vec<_>>();
        let claims = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker thread").expect("claim"))
            .collect::<Vec<_>>();
        let claimed = claims.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(claimed.len(), 1);
        let claim = &claimed[0];
        let materialized = store
            .ack_session_terminal(
                &claim.terminal_id,
                claim.claim_owner.as_deref().expect("claim owner"),
                claim.revision,
                200,
            )
            .expect("ack claimed terminal");
        assert_eq!(materialized.status, "materialized");
        assert_eq!(
            store
                .materialized_session_terminals_after("session-real", 0, 10)
                .unwrap()
                .len(),
            1
        );

        let duplicate_request = AppendTransactionRequest {
            transaction_id: "duplicate-transaction".to_string(),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: "graph:duplicate".to_string(),
                expected_revision: 0,
            }],
            events: vec![RuntimeTransactionEventInput {
                event: input(
                    "graph:duplicate",
                    RuntimeEventScope::ExecutionGraph,
                    "graph.duplicate",
                ),
                idempotency_key: Some("duplicate".to_string()),
                schema_version: 1,
            }],
        };
        assert!(
            !store
                .append_transaction(duplicate_request.clone())
                .expect("first idempotent transaction")
                .duplicate
        );
        assert!(
            store
                .append_transaction(duplicate_request)
                .expect("duplicate transaction")
                .duplicate
        );

        drop(store);
        let resolver = StaticSecretRefResolver::new([("test.pg".to_string(), url)]);
        let reopened = Arc::new(
            PostgresRuntimeEventStore::connect(
                PostgresConnectionConfig::new(
                    "runtime-event-reopen-test",
                    "test.pg",
                    "cowd-v569-reopen-test",
                ),
                &resolver,
            )
            .expect("postgres event store reopens")
            .into_runtime_event_store(),
        );
        assert_eq!(reopened.stream_revision("graph:concurrent").unwrap(), 2);
        assert_eq!(
            reopened
                .session_terminal("terminal-real")
                .unwrap()
                .expect("terminal persists")
                .status,
            "materialized"
        );

        let temp = tempfile::tempdir().expect("temporary Runtime host");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace exists");
        let services = RuntimeServices::builder(temp.path().join("home"), &workspace)
            .runtime_event_store(reopened)
            .build()
            .expect("RuntimeServices composes PostgreSQL event backend");
        services
            .record_task_lifecycle(TaskLifecycleEvent {
                task_id: "task:postgres-composed".to_string(),
                kind: TaskLifecycleKind::Started,
                payload: serde_json::json!({"source": "runtime-postgres"}),
            })
            .expect("Runtime lifecycle write reaches PostgreSQL backend");
        assert!(services
            .event_reader()
            .list_stream("task:postgres-composed")
            .expect("read composed event")
            .iter()
            .any(|event| event.kind == "task.started"));
    }

    #[test]
    fn postgres_task_store_preserves_migration_restart_and_per_task_concurrency() {
        let url = match std::env::var("COWD_TEST_POSTGRES_URL") {
            Ok(url) if !url.trim().is_empty() => url,
            _ => {
                eprintln!("skipping real PostgreSQL task test: COWD_TEST_POSTGRES_URL is not set");
                return;
            }
        };
        let temp = tempfile::tempdir().expect("temporary task migration root");
        let source_path = temp.path().join("source-tasks.db");
        let source = TaskKernel::open(source_path).expect("SQLite task source opens");
        let source_task = source
            .start_goal_idempotent("task-pg-migration", "Migrate the task control plane", true)
            .expect("source task starts");
        let phase = source
            .start_phase(
                &source_task.id,
                "postgres-verification",
                "prove target preserves the task record",
                vec!["copy task snapshot".to_string()],
                vec!["digest equality".to_string()],
                vec!["real PostgreSQL task test".to_string()],
            )
            .expect("source phase starts");
        let phase_id = phase.phases.last().expect("phase exists").id.clone();
        source
            .record_phase_artifact(
                &source_task.id,
                &phase_id,
                "evidence",
                "migration",
                "source snapshot is canonical",
            )
            .expect("source artifact persists");

        let resolver = StaticSecretRefResolver::new([("task.pg".to_string(), url.clone())]);
        let pg_store = PostgresTaskStore::connect(
            PostgresConnectionConfig::new("runtime-task-test", "task.pg", "cowd-v570-test"),
            &resolver,
        )
        .expect("postgres task store opens");
        let executor = pg_store.executor().clone();
        let target = Arc::new(pg_store.into_task_kernel());
        let manifest_path = temp.path().join("task-migration-manifest.json");
        let manifest = copy_quiesced_task_kernel(&source, target.as_ref(), &manifest_path)
            .expect("quiesced SQLite to PostgreSQL copy succeeds");
        assert_eq!(manifest.source_digest, manifest.target_digest);
        assert_eq!(manifest.task_count, 1);
        assert!(manifest_path.is_file());
        assert_eq!(
            source
                .export_migration_snapshot()
                .expect("source snapshot")
                .canonical_digest()
                .expect("source digest"),
            target
                .export_migration_snapshot()
                .expect("target snapshot")
                .canonical_digest()
                .expect("target digest")
        );

        let barrier = Arc::new(Barrier::new(2));
        let workers = (0..2)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                let target = Arc::clone(&target);
                thread::spawn(move || {
                    barrier.wait();
                    target.start_goal_idempotent(
                        "task-pg-concurrent",
                        "one governed concurrent task",
                        true,
                    )
                })
            })
            .collect::<Vec<_>>();
        let results = workers
            .into_iter()
            .map(|worker| worker.join().expect("task worker joins"))
            .collect::<Vec<_>>();
        assert!(results.iter().all(Result::is_ok));
        let concurrent = target
            .list()
            .expect("target task list")
            .into_iter()
            .find(|task| task.id == "task-pg-concurrent")
            .expect("one concurrent task persists");
        assert_eq!(concurrent.objective, "one governed concurrent task");
        assert!(target
            .start_goal_idempotent("task-pg-concurrent", "a conflicting objective", true,)
            .is_err());

        let reopened_resolver = StaticSecretRefResolver::new([("task.pg".to_string(), url)]);
        let reopened = PostgresTaskStore::connect(
            PostgresConnectionConfig::new(
                "runtime-task-reopen-test",
                "task.pg",
                "cowd-v570-reopen-test",
            ),
            &reopened_resolver,
        )
        .expect("postgres task store reopens")
        .into_task_kernel();
        let restored = reopened
            .list()
            .expect("reopened task list")
            .into_iter()
            .find(|task| task.id == source_task.id)
            .expect("migrated task survives reopen");
        assert!(restored
            .phases
            .iter()
            .any(|candidate| candidate.id == phase_id && !candidate.artifacts.is_empty()));
        assert!(
            copy_quiesced_task_kernel(&source, &reopened, temp.path().join("rejected.json"))
                .is_err()
        );
        assert!(executor.health().metrics.checkout_count > 0);
    }
}
