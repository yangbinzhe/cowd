//! Durable, transactional runtime lifecycle event store.
//!
//! A committed transaction is the only externally visible write unit. Graph,
//! node, goal, agent, team, and mission projections therefore observe one
//! monotonic commit cursor and never a partially appended multi-stream update.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const STORE_SCHEMA_VERSION: i64 = 3;
const EVENT_SCHEMA_VERSION: u32 = 1;
const MAX_TRANSACTION_EVENTS: usize = 10_000;
const MAX_TRANSACTION_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEventScope {
    ExecutionGraph,
    ExecutionNode,
    Goal,
    Mission,
    Session,
    SessionInput,
    /// Historical durable record for a session command dispatch.  These
    /// records remain replay-only evidence; command execution is now owned by
    /// the typed SessionInput/ExecutionGraph path.
    SessionCommand,
    Team,
    Agent,
    Approval,
    Relation,
    Steward,
    Task,
    Worker,
    Schedule,
    Tool,
    Recovery,
    CrossPlane,
}

impl RuntimeEventScope {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExecutionGraph => "execution_graph",
            Self::ExecutionNode => "execution_node",
            Self::Goal => "goal",
            Self::Mission => "mission",
            Self::Session => "session",
            Self::SessionInput => "session_input",
            Self::SessionCommand => "session_command",
            Self::Team => "team",
            Self::Agent => "agent",
            Self::Approval => "approval",
            Self::Relation => "relation",
            Self::Steward => "steward",
            Self::Task => "task",
            Self::Worker => "worker",
            Self::Schedule => "schedule",
            Self::Tool => "tool",
            Self::Recovery => "recovery",
            Self::CrossPlane => "cross_plane",
        }
    }

    pub fn parse(value: &str) -> Result<Self, RuntimeEventStoreError> {
        match value {
            "execution_graph" => Ok(Self::ExecutionGraph),
            "execution_node" => Ok(Self::ExecutionNode),
            "goal" => Ok(Self::Goal),
            "mission" => Ok(Self::Mission),
            "session" => Ok(Self::Session),
            "session_input" => Ok(Self::SessionInput),
            "session_command" => Ok(Self::SessionCommand),
            "team" => Ok(Self::Team),
            "agent" => Ok(Self::Agent),
            "approval" => Ok(Self::Approval),
            "relation" => Ok(Self::Relation),
            "steward" => Ok(Self::Steward),
            "task" => Ok(Self::Task),
            "worker" => Ok(Self::Worker),
            "schedule" => Ok(Self::Schedule),
            "tool" => Ok(Self::Tool),
            "recovery" => Ok(Self::Recovery),
            "cross_plane" => Ok(Self::CrossPlane),
            unknown => Err(RuntimeEventStoreError::UnknownScope(unknown.to_string())),
        }
    }
}

#[derive(Debug, Error)]
pub enum RuntimeEventStoreError {
    #[error("runtime event scope `{0}` is unknown or requires migration")]
    UnknownScope(String),
    #[error("runtime event store is corrupt: {0}")]
    Corrupt(String),
    #[error("runtime event transaction `{transaction_id}` conflicts with its committed hash")]
    TransactionConflict { transaction_id: String },
    #[error(
        "runtime stream `{stream_id}` revision mismatch: expected {expected}, actual {actual}"
    )]
    StaleRevision {
        stream_id: String,
        expected: u64,
        actual: u64,
    },
    #[error("invalid runtime event transaction: {0}")]
    InvalidTransaction(String),
    #[error("runtime event store SQL failure: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("runtime event serialization failure: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("runtime event store I/O failure: {0}")]
    Io(#[from] std::io::Error),
}

pub type RuntimeEventStoreResult<T> = Result<T, RuntimeEventStoreError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEventRef {
    pub kind: String,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableRuntimeEvent {
    pub event_id: String,
    pub stream_id: String,
    pub sequence: u64,
    pub scope: RuntimeEventScope,
    pub kind: String,
    pub status: Option<String>,
    pub actor: Option<String>,
    pub refs: Vec<RuntimeEventRef>,
    pub payload: serde_json::Value,
    pub created_at_ms: u64,
    pub commit_cursor: u64,
    pub transaction_id: String,
    pub transaction_index: u32,
    pub schema_version: u32,
    pub idempotency_key: Option<String>,
}

pub type RuntimeEventRecord = DurableRuntimeEvent;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEventInput {
    pub stream_id: String,
    pub scope: RuntimeEventScope,
    pub kind: String,
    pub status: Option<String>,
    pub actor: Option<String>,
    pub refs: Vec<RuntimeEventRef>,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeTransactionEventInput {
    pub event: RuntimeEventInput,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default = "default_event_schema_version")]
    pub schema_version: u32,
}

impl From<RuntimeEventInput> for RuntimeTransactionEventInput {
    fn from(event: RuntimeEventInput) -> Self {
        Self {
            event,
            idempotency_key: None,
            schema_version: EVENT_SCHEMA_VERSION,
        }
    }
}

const fn default_event_schema_version() -> u32 {
    EVENT_SCHEMA_VERSION
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedStreamRevision {
    pub stream_id: String,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendTransactionRequest {
    pub transaction_id: String,
    pub expected_streams: Vec<ExpectedStreamRevision>,
    pub events: Vec<RuntimeTransactionEventInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionTerminalInput {
    pub terminal_id: String,
    pub message_id: String,
    pub session_id: String,
    pub payload_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedStreamRevision {
    pub stream_id: String,
    pub expected_revision: u64,
    pub committed_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendTransactionReceipt {
    pub commit_cursor: u64,
    pub transaction_id: String,
    pub request_hash: String,
    pub stream_revisions: Vec<CommittedStreamRevision>,
    pub event_ids: Vec<String>,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedEventBatch {
    pub commit_cursor: u64,
    pub transaction_id: String,
    pub events: Vec<RuntimeEventRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeSessionOutboxFailureClass {
    Retryable,
    Permanent,
    AuthorizationBlocked,
    CorruptPayload,
}

impl RuntimeSessionOutboxFailureClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
            Self::AuthorizationBlocked => "authorization_blocked",
            Self::CorruptPayload => "corrupt_payload",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSessionOutboxRecord {
    pub terminal_id: String,
    pub message_id: String,
    pub session_id: String,
    pub commit_cursor: u64,
    pub payload_ref: String,
    pub status: String,
    pub attempts: u32,
    pub next_attempt_at_ms: Option<u64>,
    pub claim_owner: Option<String>,
    pub claim_expires_at_ms: Option<u64>,
    pub failure_class: Option<String>,
    pub last_error: Option<String>,
    pub materialized_at_ms: Option<u64>,
    pub revision: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSessionOutboxHealth {
    pub pending: u64,
    pub claimed: u64,
    pub retry_scheduled: u64,
    pub materialized: u64,
    pub blocked: u64,
}

#[derive(Debug)]
pub struct RuntimeEventStore {
    path: PathBuf,
    conn: Mutex<Connection>,
}

impl RuntimeEventStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::try_open(path).map_err(|error| error.to_string())
    }

    pub fn try_open(path: impl AsRef<Path>) -> RuntimeEventStoreResult<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut conn = Connection::open(&path)?;
        configure_connection(&conn, false)?;
        migrate_schema(&mut conn)?;
        Ok(Self {
            path,
            conn: Mutex::new(conn),
        })
    }

    pub fn open_in_memory() -> Result<Self, String> {
        Self::try_open_in_memory().map_err(|error| error.to_string())
    }

    pub fn try_open_in_memory() -> RuntimeEventStoreResult<Self> {
        let mut conn = Connection::open_in_memory()?;
        configure_connection(&conn, true)?;
        migrate_schema(&mut conn)?;
        Ok(Self {
            path: PathBuf::from(":memory:"),
            conn: Mutex::new(conn),
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
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
        let mut conn = lock_connection(&self.conn);
        let tx = conn.transaction()?;
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
        let mut conn = lock_connection(&self.conn);
        let tx = conn.transaction()?;
        let receipt = append_transaction_in_tx(&tx, &request, None)?;
        tx.commit()?;
        Ok(receipt)
    }

    pub fn append_transaction_with_terminal(
        &self,
        request: AppendTransactionRequest,
        terminal: SessionTerminalInput,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let mut conn = lock_connection(&self.conn);
        let tx = conn.transaction()?;
        let receipt = append_transaction_in_tx(&tx, &request, Some(&terminal))?;
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
        let conn = lock_connection(&self.conn);
        let mut stmt = conn.prepare(
            "SELECT commit_cursor, transaction_id FROM runtime_commits \
             WHERE commit_cursor > ?1 ORDER BY commit_cursor ASC LIMIT ?2",
        )?;
        let commits = stmt
            .query_map(params![cursor as i64, max_commits as i64], |row| {
                Ok((row.get::<_, i64>(0)? as u64, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        commits
            .into_iter()
            .map(|(commit_cursor, transaction_id)| {
                Ok(CommittedEventBatch {
                    commit_cursor,
                    events: load_transaction_events(&conn, &transaction_id)?,
                    transaction_id,
                })
            })
            .collect()
    }

    pub fn event_by_idempotency_key(
        &self,
        stream_id: &str,
        idempotency_key: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>> {
        let conn = lock_connection(&self.conn);
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
        let conn = lock_connection(&self.conn);
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

    /// Resolve the canonical graph streams that produced terminal work for a
    /// session. The terminal request is the durable bridge from a session
    /// input to its graph; callers must not reconstruct this relation from
    /// transcript text or a client-side naming convention.
    pub fn execution_events_for_session(
        &self,
        session_id: &str,
        after_commit_cursor: u64,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if session_id.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let terminal_requests = self
            .list_scope(RuntimeEventScope::SessionInput, 10_000)?
            .into_iter()
            .filter(|event| event.kind == "runtime.session.terminal_requested")
            .filter(|event| {
                event
                    .payload
                    .get("session_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(session_id)
            })
            .collect::<Vec<_>>();
        let graph_ids = terminal_requests
            .iter()
            .flat_map(|event| event.refs.iter())
            .filter(|reference| reference.kind == "execution_graph")
            .map(|reference| reference.id.clone())
            .collect::<BTreeSet<_>>();
        let mut related = terminal_requests;
        for graph_id in graph_ids {
            related.extend(self.list_stream(&graph_id)?);
        }
        related.sort_by_key(|event| (event.commit_cursor, event.transaction_index));
        related.dedup_by(|left, right| left.event_id == right.event_id);
        Ok(related
            .into_iter()
            .filter(|event| event.commit_cursor > after_commit_cursor)
            .take(limit)
            .collect())
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

    pub fn stream_ids_for_scope(
        &self,
        scope: RuntimeEventScope,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        let conn = lock_connection(&self.conn);
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
        let conn = lock_connection(&self.conn);
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

    fn query_events<P>(
        &self,
        sql: &str,
        params: P,
    ) -> RuntimeEventStoreResult<Vec<DurableRuntimeEvent>>
    where
        P: rusqlite::Params,
    {
        let conn = lock_connection(&self.conn);
        let mut stmt = conn.prepare(sql)?;
        let events = stmt
            .query_map(params, row_to_event)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(events)
    }

    /// Insert one terminal delivery exactly once. A duplicate terminal ID is
    /// accepted only when every immutable field matches the committed row.
    pub fn enqueue_session_terminal(
        &self,
        terminal_id: &str,
        message_id: &str,
        session_id: &str,
        commit_cursor: u64,
        payload_ref: &str,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        let conn = lock_connection(&self.conn);
        conn.execute(
            "INSERT INTO runtime_session_outbox
             (terminal_id, message_id, session_id, commit_cursor, payload_ref, status, revision)
             VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0)
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
        let mut conn = lock_connection(&self.conn);
        let tx = conn.transaction()?;
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
        query_runtime_session_outbox(&lock_connection(&self.conn), terminal_id)
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
        let conn = lock_connection(&self.conn);
        let mut statement = conn.prepare(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, status,
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
        let conn = lock_connection(&self.conn);
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
                _ => {}
            }
        }
        Ok(health)
    }

    pub fn blocked_session_terminals(
        &self,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        let conn = lock_connection(&self.conn);
        let mut statement = conn.prepare(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, status,
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
        let conn = lock_connection(&self.conn);
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
            let conn = lock_connection(&self.conn);
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
        let conn = lock_connection(&self.conn);
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
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: format!("terminal:{terminal_id}"),
                expected: expected_revision,
                actual: query_runtime_session_outbox(&conn, terminal_id)?
                    .map_or(0, |record| record.revision),
            });
        }
        query_runtime_session_outbox(&conn, terminal_id)?.ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!("terminal `{terminal_id}` vanished"))
        })
    }
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
    let tx = conn.transaction()?;
    create_current_tables(&tx)?;
    migrate_legacy_runtime_events(&tx)?;
    tx.pragma_update(None, "user_version", STORE_SCHEMA_VERSION)?;
    tx.commit()?;
    validate_schema(conn)
}

fn create_current_tables(tx: &Transaction<'_>) -> RuntimeEventStoreResult<()> {
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
            idempotency_key TEXT
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
        CREATE TABLE IF NOT EXISTS runtime_session_outbox (
            terminal_id TEXT PRIMARY KEY,
            message_id TEXT NOT NULL UNIQUE,
            session_id TEXT NOT NULL,
            commit_cursor INTEGER NOT NULL,
            payload_ref TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at INTEGER,
            claim_owner TEXT,
            claim_expires_at INTEGER,
            failure_class TEXT,
            last_error TEXT,
            materialized_at INTEGER,
            revision INTEGER NOT NULL DEFAULT 0
        );",
    )?;

    for (column, definition) in [
        ("commit_cursor", "INTEGER"),
        ("transaction_id", "TEXT"),
        ("transaction_index", "INTEGER"),
        ("schema_version", "INTEGER NOT NULL DEFAULT 1"),
        ("idempotency_key", "TEXT"),
    ] {
        if !table_has_column(tx, "runtime_events", column)? {
            tx.execute(
                &format!("ALTER TABLE runtime_events ADD COLUMN {column} {definition}"),
                [],
            )?;
        }
    }
    Ok(())
}

fn query_runtime_session_outbox(
    conn: &Connection,
    terminal_id: &str,
) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
    conn.query_row(
        "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, status,
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
        status: row.get(5)?,
        attempts: row.get::<_, i64>(6)? as u32,
        next_attempt_at_ms: row.get::<_, Option<i64>>(7)?.map(|value| value as u64),
        claim_owner: row.get(8)?,
        claim_expires_at_ms: row.get::<_, Option<i64>>(9)?.map(|value| value as u64),
        failure_class: row.get(10)?,
        last_error: row.get(11)?,
        materialized_at_ms: row.get::<_, Option<i64>>(12)?.map(|value| value as u64),
        revision: row.get::<_, i64>(13)? as u64,
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
         CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_created
            ON runtime_events(scope, created_at_ms);
         CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_commit_index
            ON runtime_events(commit_cursor, transaction_index);
         CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_transaction_index
            ON runtime_events(transaction_id, transaction_index);
         CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_stream_idempotency
            ON runtime_events(stream_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
         CREATE INDEX IF NOT EXISTS idx_runtime_commits_cursor
            ON runtime_commits(commit_cursor);",
    )?;
    Ok(())
}

fn validate_schema(conn: &Connection) -> RuntimeEventStoreResult<()> {
    for table in [
        "runtime_events",
        "runtime_commits",
        "runtime_transaction_streams",
        "runtime_stream_heads",
        "runtime_session_outbox",
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
    let request_hash = request_hash(request)?;
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
            insert_terminal_in_tx(tx, terminal, receipt.commit_cursor)?;
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
        tx.execute(
            "INSERT INTO runtime_events \
             (event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms, \
              commit_cursor, transaction_id, transaction_index, schema_version, idempotency_key) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
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
            ],
        )?;
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
         (terminal_id, message_id, session_id, commit_cursor, payload_ref, status, revision)
         VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0)
         ON CONFLICT(terminal_id) DO NOTHING",
        params![
            terminal.terminal_id,
            terminal.message_id,
            terminal.session_id,
            commit_cursor as i64,
            terminal.payload_ref,
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
    {
        return Err(RuntimeEventStoreError::TransactionConflict {
            transaction_id: terminal.terminal_id.clone(),
        });
    }
    Ok(())
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
    let bytes = serde_json::to_vec(request)?.len();
    if bytes > MAX_TRANSACTION_BYTES {
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

fn request_hash(request: &AppendTransactionRequest) -> RuntimeEventStoreResult<String> {
    Ok(hash_bytes(&serde_json::to_vec(request)?))
}

fn hash_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
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

fn table_has_column(conn: &Connection, table: &str, column: &str) -> RuntimeEventStoreResult<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns.iter().any(|candidate| candidate == column))
}

fn lock_connection(mutex: &Mutex<Connection>) -> std::sync::MutexGuard<'_, Connection> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn input(stream_id: &str, scope: RuntimeEventScope, kind: &str) -> RuntimeEventInput {
        RuntimeEventInput {
            stream_id: stream_id.to_string(),
            scope,
            kind: kind.to_string(),
            status: Some("running".to_string()),
            actor: Some("test".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({"kind": kind}),
        }
    }

    fn transaction(id: &str) -> AppendTransactionRequest {
        AppendTransactionRequest {
            transaction_id: id.to_string(),
            expected_streams: vec![
                ExpectedStreamRevision {
                    stream_id: "graph:g1".to_string(),
                    expected_revision: 0,
                },
                ExpectedStreamRevision {
                    stream_id: "node:n1".to_string(),
                    expected_revision: 0,
                },
            ],
            events: vec![
                RuntimeTransactionEventInput {
                    event: input(
                        "graph:g1",
                        RuntimeEventScope::ExecutionGraph,
                        "graph.started",
                    ),
                    idempotency_key: Some("graph-start".to_string()),
                    schema_version: 1,
                },
                RuntimeTransactionEventInput {
                    event: input("node:n1", RuntimeEventScope::ExecutionNode, "node.running"),
                    idempotency_key: Some("node-run".to_string()),
                    schema_version: 1,
                },
            ],
        }
    }

    #[test]
    fn multi_stream_transaction_is_atomic_and_idempotent() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        let request = transaction("tx-1");
        let first = store
            .append_transaction(request.clone())
            .expect("first commit");
        let duplicate = store.append_transaction(request).expect("idempotent retry");
        assert_eq!(first.commit_cursor, duplicate.commit_cursor);
        assert!(!first.duplicate);
        assert!(duplicate.duplicate);
        assert_eq!(store.stream_revision("graph:g1").unwrap(), 1);
        assert_eq!(store.stream_revision("node:n1").unwrap(), 1);
        assert_eq!(store.all_events(100).unwrap().len(), 2);
    }

    #[test]
    fn session_execution_events_follow_durable_terminal_graph_reference() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        let graph_id = "graph:session-a";
        store
            .append(input(
                graph_id,
                RuntimeEventScope::ExecutionGraph,
                "execution_graph.planned",
            ))
            .unwrap();
        let mut terminal = input(
            "session-terminal:request-a",
            RuntimeEventScope::SessionInput,
            "runtime.session.terminal_requested",
        );
        terminal.payload = serde_json::json!({"session_id": "session-a"});
        terminal.refs = vec![RuntimeEventRef {
            kind: "execution_graph".to_string(),
            id: graph_id.to_string(),
        }];
        store.append(terminal).unwrap();

        let related = store
            .execution_events_for_session("session-a", 0, 20)
            .unwrap();
        assert!(related
            .iter()
            .any(|event| event.stream_id == graph_id && event.kind == "execution_graph.planned"));
        assert!(related
            .iter()
            .any(|event| event.kind == "runtime.session.terminal_requested"));
        assert!(store
            .execution_events_for_session("session-b", 0, 20)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn transaction_id_reuse_with_different_request_is_rejected() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        let request = transaction("tx-conflict");
        store.append_transaction(request.clone()).expect("commit");
        let mut changed = request;
        changed.events[0].event.kind = "graph.changed".to_string();
        assert!(matches!(
            store.append_transaction(changed),
            Err(RuntimeEventStoreError::TransactionConflict { .. })
        ));
    }

    #[test]
    fn stale_revision_rolls_back_entire_transaction_without_visible_cursor() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        store
            .append(input(
                "node:n1",
                RuntimeEventScope::ExecutionNode,
                "node.created",
            ))
            .expect("seed");
        let before = store.events_after_cursor(0, 100).unwrap();
        let request = transaction("tx-stale");
        assert!(matches!(
            store.append_transaction(request),
            Err(RuntimeEventStoreError::StaleRevision { .. })
        ));
        let after = store.events_after_cursor(0, 100).unwrap();
        assert_eq!(before, after);
        assert!(store.list_stream("graph:g1").unwrap().is_empty());
    }

    #[test]
    fn cursor_pagination_never_splits_a_transaction() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        let first = store.append_transaction(transaction("tx-page-1")).unwrap();
        let second = store
            .append_batch_if_revision(
                "graph:g1",
                1,
                "tx-page-2",
                vec![input(
                    "graph:g1",
                    RuntimeEventScope::ExecutionGraph,
                    "graph.completed",
                )
                .into()],
            )
            .unwrap();
        let page = store.events_after_cursor(0, 1).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].commit_cursor, first.commit_cursor);
        assert_eq!(page[0].events.len(), 2);
        let next = store.events_after_cursor(first.commit_cursor, 1).unwrap();
        assert_eq!(next[0].commit_cursor, second.commit_cursor);
        assert_eq!(next[0].events.len(), 1);
    }

    #[test]
    fn legacy_version_zero_database_is_migrated_without_losing_events() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("runtime.sqlite");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE runtime_events (
                event_id TEXT PRIMARY KEY, stream_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                scope TEXT NOT NULL, kind TEXT NOT NULL, status TEXT, actor TEXT,
                payload TEXT NOT NULL, refs TEXT NOT NULL, created_at_ms INTEGER NOT NULL
             );
             INSERT INTO runtime_events VALUES
                ('old-1', 'mission:m1', 1, 'mission', 'mission.started', 'running', NULL, '{}', '[]', 1);",
        )
        .unwrap();
        drop(conn);

        let store = RuntimeEventStore::try_open(&path).expect("legacy migration");
        let events = store.list_stream("mission:m1").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id, "old-1");
        assert!(events[0].commit_cursor > 0);
        let conn = Connection::open(path).unwrap();
        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, STORE_SCHEMA_VERSION);
    }

    #[test]
    fn historical_session_command_scope_remains_replayable_after_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("runtime.sqlite");
        let store = RuntimeEventStore::try_open(&path).expect("event store opens");
        store
            .append(input(
                "session-command:legacy-1",
                RuntimeEventScope::SessionCommand,
                "session_execution.dispatched",
            ))
            .expect("historical command event persists");
        drop(store);

        let reopened = RuntimeEventStore::try_open(&path)
            .expect("historical session command scope remains readable");
        let events = reopened
            .list_scope(RuntimeEventScope::SessionCommand, 10)
            .expect("historical command scope lists");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].scope, RuntimeEventScope::SessionCommand);
        assert_eq!(events[0].kind, "session_execution.dispatched");
    }

    #[test]
    fn unknown_legacy_scope_aborts_migration_and_preserves_version_zero() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("runtime.sqlite");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE runtime_events (
                event_id TEXT PRIMARY KEY, stream_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                scope TEXT NOT NULL, kind TEXT NOT NULL, status TEXT, actor TEXT,
                payload TEXT NOT NULL, refs TEXT NOT NULL, created_at_ms INTEGER NOT NULL
             );
             INSERT INTO runtime_events VALUES
                ('bad-1', 'bad:x', 1, 'unknown_scope', 'bad', NULL, NULL, '{}', '[]', 1);",
        )
        .unwrap();
        drop(conn);

        assert!(matches!(
            RuntimeEventStore::try_open(&path),
            Err(RuntimeEventStoreError::UnknownScope(_))
        ));
        let conn = Connection::open(path).unwrap();
        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 0);
        assert!(!table_has_column(&conn, "runtime_events", "commit_cursor").unwrap());
    }

    #[test]
    fn idempotency_key_can_resolve_the_committed_side_effect() {
        let store = RuntimeEventStore::try_open_in_memory().expect("event store");
        store.append_transaction(transaction("tx-idem")).unwrap();
        let event = store
            .event_by_idempotency_key("node:n1", "node-run")
            .unwrap()
            .expect("idempotent event");
        assert_eq!(event.kind, "node.running");
    }

    #[test]
    fn terminal_outbox_reclaims_expired_lease_and_materializes_once() {
        let store = RuntimeEventStore::try_open_in_memory().unwrap();
        store
            .enqueue_session_terminal("t1", "m1", "s1", 7, "e:1")
            .unwrap();
        let first = store.claim_session_terminals("a", 100, 50, 8).unwrap();
        assert_eq!(first.len(), 1);
        assert!(store
            .claim_session_terminals("b", 149, 50, 8)
            .unwrap()
            .is_empty());
        let reclaimed = store.claim_session_terminals("b", 150, 50, 8).unwrap();
        let done = store
            .ack_session_terminal("t1", "b", reclaimed[0].revision, 151)
            .unwrap();
        assert_eq!(done.status, "materialized");
        assert!(store
            .claim_session_terminals("c", 1_000, 50, 8)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn materialized_terminal_replay_is_scoped_cursor_ordered_and_excludes_pending() {
        let store = RuntimeEventStore::try_open_in_memory().unwrap();
        for (terminal, message, session, cursor) in [
            ("t-old", "m-old", "s-a", 4_u64),
            ("t-new", "m-new", "s-a", 9_u64),
            ("t-other", "m-other", "s-b", 12_u64),
            ("t-pending", "m-pending", "s-a", 15_u64),
        ] {
            store
                .enqueue_session_terminal(
                    terminal,
                    message,
                    session,
                    cursor,
                    "assistant_json:\"ok\"",
                )
                .unwrap();
        }
        let claims = store
            .claim_session_terminals("worker", 100, 50, 10)
            .unwrap();
        for terminal in ["t-old", "t-new", "t-other"] {
            let claimed = claims
                .iter()
                .find(|record| record.terminal_id == terminal)
                .unwrap();
            store
                .ack_session_terminal(terminal, "worker", claimed.revision, 101)
                .unwrap();
        }

        let replay = store
            .materialized_session_terminals_after("s-a", 4, 10)
            .unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].terminal_id, "t-new");
        assert_eq!(replay[0].commit_cursor, 9);
    }

    #[test]
    fn terminal_outbox_retries_then_blocks_and_rejects_conflict() {
        let store = RuntimeEventStore::try_open_in_memory().unwrap();
        store
            .enqueue_session_terminal("t2", "m2", "s2", 8, "e:2")
            .unwrap();
        assert!(matches!(
            store.enqueue_session_terminal("t2", "different", "s2", 8, "e:2"),
            Err(RuntimeEventStoreError::TransactionConflict { .. })
        ));
        let first = store
            .claim_session_terminals("w", 200, 50, 1)
            .unwrap()
            .pop()
            .unwrap();
        let retry = store
            .fail_session_terminal(
                "t2",
                "w",
                first.revision,
                RuntimeSessionOutboxFailureClass::Retryable,
                "temporary",
                300,
                2,
                201,
            )
            .unwrap();
        assert_eq!(retry.status, "retry_scheduled");
        let second = store
            .claim_session_terminals("w", 300, 50, 1)
            .unwrap()
            .pop()
            .unwrap();
        let blocked = store
            .fail_session_terminal(
                "t2",
                "w",
                second.revision,
                RuntimeSessionOutboxFailureClass::Retryable,
                "still unavailable",
                400,
                2,
                301,
            )
            .unwrap();
        assert_eq!(blocked.status, "blocked");
    }

    #[test]
    fn terminal_outbox_permanent_failure_never_retries() {
        let store = RuntimeEventStore::try_open_in_memory().unwrap();
        store
            .enqueue_session_terminal("t3", "m3", "s3", 9, "e:3")
            .unwrap();
        let claim = store
            .claim_session_terminals("w", 400, 50, 1)
            .unwrap()
            .pop()
            .unwrap();
        let blocked = store
            .fail_session_terminal(
                "t3",
                "w",
                claim.revision,
                RuntimeSessionOutboxFailureClass::CorruptPayload,
                "invalid payload",
                500,
                10,
                401,
            )
            .unwrap();
        assert_eq!(blocked.status, "blocked");
        assert!(store
            .claim_session_terminals("w", 10_000, 50, 1)
            .unwrap()
            .is_empty());
        assert_eq!(store.blocked_session_terminals(10).unwrap().len(), 1);
        let retried = store
            .retry_session_terminal("t3", "operator", "payload repaired", 10_001)
            .unwrap();
        assert_eq!(retried.status, "retry_scheduled");
    }

    #[test]
    fn terminal_outbox_survives_restart_and_two_workers_claim_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime-events.db");
        {
            let store = RuntimeEventStore::open(&path).unwrap();
            store
                .enqueue_session_terminal("restart", "m4", "s4", 10, "assistant_json:\"ok\"")
                .unwrap();
        }
        let store = Arc::new(RuntimeEventStore::open(&path).unwrap());
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let mut handles = Vec::new();
        for worker in ["worker-a", "worker-b"] {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                store.claim_session_terminals(worker, 100, 50, 1).unwrap()
            }));
        }
        barrier.wait();
        let claimed = handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            claimed.len(),
            1,
            "concurrent workers must claim exactly once"
        );
    }
}
