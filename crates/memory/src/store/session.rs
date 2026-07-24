//! SQLite-backed session store for Gateway [`SessionContext`] persistence.
//!
//! Follows the same design as [`super::sqlite`]: the database **path** is
//! stored and a fresh `rusqlite::Connection` is opened for every synchronous
//! operation (WAL mode ensures safe concurrent access).
//!
//! ## Schema
//!
//! Two tables are managed:
//!
//! * `sessions` – one row per conversation session.
//! * `session_memories` – many-to-many join between sessions and memory IDs.

use std::path::Path;

use chrono::{DateTime, Utc};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::types::Value;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};

use crate::{error::MemoryError, runtime_event::SESSION_DOMAIN_EVENT_TYPE, store::Result};

// ---------------------------------------------------------------------------
// Sentinel for in-memory databases (tests only)
// ---------------------------------------------------------------------------

const IN_MEMORY_PATH: &str = ":memory:";

fn new_pool(db_path: &str, max_size: u32) -> Result<Pool<SqliteConnectionManager>> {
    let manager = SqliteConnectionManager::file(db_path);
    Pool::builder()
        .max_size(max_size)
        .build(manager)
        .map_err(|e| MemoryError::Store(e.to_string()))
}

fn sql_err(e: rusqlite::Error) -> MemoryError {
    MemoryError::Store(e.to_string())
}

/// Configure per-connection pragmas (WAL mode, foreign keys, busy timeout).
/// Execute a pragma that may return rows (rusqlite 0.31 treats this as an error).
fn exec_pragma(conn: &Connection, sql: &str) -> Result<()> {
    match conn.execute(sql, []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::ExecuteReturnedResults) => Ok(()),
        Err(e) => Err(sql_err(e)),
    }
}

fn set_conn_pragmas(conn: &Connection) -> Result<()> {
    exec_pragma(conn, "PRAGMA journal_mode=WAL")?;
    exec_pragma(conn, "PRAGMA foreign_keys=ON")?;
    exec_pragma(conn, "PRAGMA busy_timeout=5000")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Schema DDL
// ---------------------------------------------------------------------------

/// FTS5 search result for sessions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionSearchResult {
    pub session_id: String,
    pub platform: String,
    pub chat_id: String,
    pub user_id: Option<String>,
    pub created_at: String,
    pub last_activity: String,
    pub message_count: i64,
    /// Highlighted snippet from metadata_json
    pub snippet: Option<String>,
}

/// Filter/sort/page options for DB-backed session listing.
#[derive(Debug, Clone, Default)]
pub struct SessionListOptions<'a> {
    pub query: Option<&'a str>,
    pub model: Option<&'a str>,
    pub status: Option<&'a str>,
    pub sort: &'a str,
    pub order: &'a str,
    pub limit: usize,
    pub offset: usize,
}

/// A page of session records plus the total number of rows matching the
/// filters before pagination.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionListPage {
    pub records: Vec<SessionRecord>,
    pub total: usize,
}

/// A single message within a conversation session.
///
/// Each message belongs to a session and is ordered by `sequence`.
/// The `content_json` field stores the message blocks as a JSON array
/// of `ContentBlock` objects (text, tool_use, tool_result, etc.).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionMessage {
    /// Immutable cross-surface identity. Sequence is ordering metadata, not a
    /// durable identity: clients must use this value for replay and dedupe.
    pub stable_message_id: String,
    pub session_id: String,
    pub sequence: usize,
    pub role: String,
    pub content_json: String,
    pub blocks_count: usize,
    pub tool_use_id: Option<String>,
    pub tool_name: Option<String>,
    pub token_usage_json: Option<String>,
    pub created_at_ms: u64,
}

/// Durable state of one Session -> Runtime materialization request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxStatus {
    Pending,
    Claimed,
    RetryScheduled,
    Materialized,
    BlockedMaterialization,
}

impl OutboxStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Claimed => "claimed",
            Self::RetryScheduled => "retry_scheduled",
            Self::Materialized => "materialized",
            Self::BlockedMaterialization => "blocked_materialization",
        }
    }

    pub fn parse(value: &str) -> rusqlite::Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "claimed" => Ok(Self::Claimed),
            "retry_scheduled" => Ok(Self::RetryScheduled),
            "materialized" => Ok(Self::Materialized),
            "blocked_materialization" => Ok(Self::BlockedMaterialization),
            other => Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                format!("unknown session runtime outbox status `{other}`").into(),
            )),
        }
    }
}

/// Failure classes determine whether the bridge may retry automatically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxFailureClass {
    Retryable,
    Permanent,
    AuthorizationBlocked,
    CorruptPayload,
}

impl OutboxFailureClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
            Self::AuthorizationBlocked => "authorization_blocked",
            Self::CorruptPayload => "corrupt_payload",
        }
    }

    pub fn parse(value: &str) -> rusqlite::Result<Self> {
        match value {
            "retryable" => Ok(Self::Retryable),
            "permanent" => Ok(Self::Permanent),
            "authorization_blocked" => Ok(Self::AuthorizationBlocked),
            "corrupt_payload" => Ok(Self::CorruptPayload),
            other => Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                format!("unknown session runtime outbox failure class `{other}`").into(),
            )),
        }
    }
}

/// Stable IDs supplied by ingress for one user message and Runtime request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRuntimeOutboxRequest {
    pub request_id: String,
    pub turn_id: String,
    pub message_id: String,
    pub created_at_ms: u64,
    /// Opaque, versioned Runtime-owned ingress options.  Memory persists this
    /// value but never interprets it, preserving the Session→Runtime boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_options_json: Option<String>,
}

/// Persisted bridge work item. `revision` protects every status transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRuntimeOutboxRecord {
    pub request_id: String,
    pub turn_id: String,
    pub message_id: String,
    pub session_id: String,
    pub sequence: usize,
    pub status: OutboxStatus,
    pub runtime_commit_cursor: Option<u64>,
    pub attempts: u32,
    pub next_attempt_at_ms: u64,
    pub claim_owner: Option<String>,
    pub claim_expires_at_ms: Option<u64>,
    pub failure_class: Option<OutboxFailureClass>,
    pub last_error: Option<String>,
    pub revision: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_options_json: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRuntimeOutboxHealth {
    pub pending: usize,
    pub claimed: usize,
    pub retry_scheduled: usize,
    pub materialized: usize,
    pub blocked: usize,
}

/// Intent written by the Session authority and materialized by the Runtime
/// bridge into the workspace Mission aggregate.  This is deliberately a
/// separate outbox from ingress: Session records are owned by this SQLite
/// store, while Mission events are owned by RuntimeEventStore.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionMissionOutboxOperation {
    Register,
    Start,
    Close,
}

impl SessionMissionOutboxOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Register => "register",
            Self::Start => "start",
            Self::Close => "close",
        }
    }

    pub fn parse(value: &str) -> rusqlite::Result<Self> {
        match value {
            "register" => Ok(Self::Register),
            "start" => Ok(Self::Start),
            "close" => Ok(Self::Close),
            other => Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                format!("unknown session mission outbox operation `{other}`").into(),
            )),
        }
    }
}

/// Stable request identity for one Session -> Mission lifecycle intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMissionOutboxRequest {
    pub request_id: String,
    pub session_id: String,
    pub title: String,
    pub workspace_key: String,
    pub operation: SessionMissionOutboxOperation,
    pub created_at_ms: u64,
}

/// Persisted Mission lifecycle work item. `revision` protects transitions so
/// a stale bridge process cannot acknowledge or fail another worker's lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMissionOutboxRecord {
    pub request_id: String,
    pub session_id: String,
    pub title: String,
    pub workspace_key: String,
    pub operation: SessionMissionOutboxOperation,
    pub status: OutboxStatus,
    pub attempts: u32,
    pub next_attempt_at_ms: u64,
    pub claim_owner: Option<String>,
    pub claim_expires_at_ms: Option<u64>,
    pub failure_class: Option<OutboxFailureClass>,
    pub last_error: Option<String>,
    pub revision: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;").map_err(sql_err)?;

    // Execute each DDL statement individually to avoid rusqlite's execute_batch
    // returning "Execute returned results" errors when FTS5 virtual tables or
    // triggers are involved in a multi-statement batch.
    let statements: &[&str] = &[
        r"CREATE TABLE IF NOT EXISTS sessions (
            session_id    TEXT PRIMARY KEY,
            platform      TEXT NOT NULL,
            chat_id       TEXT NOT NULL,
            user_id       TEXT,
            model         TEXT,
            created_at    TEXT NOT NULL,
            last_activity TEXT NOT NULL,
            message_count INTEGER NOT NULL DEFAULT 0,
            reset_policy  TEXT NOT NULL,
            metadata_json TEXT,
            input_tokens  INTEGER NOT NULL DEFAULT 0,
            output_tokens INTEGER NOT NULL DEFAULT 0,
            estimated_cost_usd REAL NOT NULL DEFAULT 0.0,
            status TEXT NOT NULL DEFAULT 'active',
            created_at_ms INTEGER NOT NULL DEFAULT 0,
            updated_at_ms INTEGER NOT NULL DEFAULT 0
        )",
        r"CREATE TABLE IF NOT EXISTS session_memories (
            session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
            memory_id  TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (session_id, memory_id)
        )",
        r"CREATE VIRTUAL TABLE IF NOT EXISTS sessions_fts USING fts5(
            session_id UNINDEXED,
            platform,
            chat_id,
            user_id,
            metadata_json,
            content=sessions,
            content_rowid=rowid
        )",
        r"CREATE TRIGGER IF NOT EXISTS sessions_fts_ai AFTER INSERT ON sessions BEGIN
            INSERT INTO sessions_fts(rowid, session_id, platform, chat_id, user_id, metadata_json)
                VALUES (new.rowid, new.session_id, new.platform, new.chat_id, new.user_id, new.metadata_json);
        END",
        r"CREATE TRIGGER IF NOT EXISTS sessions_fts_ad AFTER DELETE ON sessions BEGIN
            INSERT INTO sessions_fts(sessions_fts, rowid, session_id, platform, chat_id, user_id, metadata_json)
                VALUES ('delete', old.rowid, old.session_id, old.platform, old.chat_id, old.user_id, old.metadata_json);
        END",
        r"CREATE TRIGGER IF NOT EXISTS sessions_fts_au AFTER UPDATE ON sessions BEGIN
            INSERT INTO sessions_fts(sessions_fts, rowid, session_id, platform, chat_id, user_id, metadata_json)
                VALUES ('delete', old.rowid, old.session_id, old.platform, old.chat_id, old.user_id, old.metadata_json);
            INSERT INTO sessions_fts(rowid, session_id, platform, chat_id, user_id, metadata_json)
                VALUES (new.rowid, new.session_id, new.platform, new.chat_id, new.user_id, new.metadata_json);
        END",
        r"CREATE INDEX IF NOT EXISTS idx_sessions_platform      ON sessions(platform)",
        r"CREATE INDEX IF NOT EXISTS idx_sessions_last_activity ON sessions(last_activity)",
        r"CREATE TABLE IF NOT EXISTS messages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            stable_message_id TEXT NOT NULL,
            session_id      TEXT NOT NULL,
            sequence        INTEGER NOT NULL,
            role            TEXT NOT NULL,
            content_json    TEXT NOT NULL,
            blocks_count    INTEGER NOT NULL DEFAULT 1,
            tool_use_id     TEXT,
            tool_name       TEXT,
            token_usage_json TEXT,
            created_at_ms   INTEGER NOT NULL,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
            UNIQUE(stable_message_id),
            UNIQUE(session_id, sequence)
        )",
        r"CREATE INDEX IF NOT EXISTS idx_messages_session     ON messages(session_id)",
        r"CREATE INDEX IF NOT EXISTS idx_messages_session_seq ON messages(session_id, sequence)",
        r"CREATE TABLE IF NOT EXISTS session_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_json TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            created_at_ms INTEGER NOT NULL,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
        )",
        r"CREATE INDEX IF NOT EXISTS idx_session_events_session     ON session_events(session_id)",
        r"CREATE INDEX IF NOT EXISTS idx_session_events_session_seq ON session_events(session_id, sequence)",
        r"CREATE INDEX IF NOT EXISTS idx_session_events_session_type_seq
            ON session_events(session_id, event_type, sequence)",
        r"CREATE INDEX IF NOT EXISTS idx_session_events_context_envelope_id
            ON session_events(json_extract(event_json, '$.envelope.id'))
            WHERE event_type = 'ContextEnvelope'",
        r"CREATE TABLE IF NOT EXISTS session_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            event_idx INTEGER NOT NULL,
            messages_json TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
        )",
        r"CREATE INDEX IF NOT EXISTS idx_session_snapshots_session ON session_snapshots(session_id)",
        r"CREATE INDEX IF NOT EXISTS idx_session_snapshots_latest  ON session_snapshots(session_id, event_idx DESC)",
    ];

    for stmt in statements {
        conn.execute_batch(stmt).map_err(sql_err)?;
    }

    if let Err(error) = ensure_session_event_sequence_constraint(conn) {
        let _ = conn.execute_batch("ROLLBACK;");
        return Err(error);
    }

    ensure_messages_schema(conn)?;
    ensure_session_runtime_outbox_schema(conn)?;
    ensure_session_mission_outbox_schema(conn)?;
    migrate_legacy_session_domain_events(conn)?;

    let existing_session_columns = {
        let mut stmt = conn
            .prepare("PRAGMA table_info(sessions)")
            .map_err(sql_err)?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(sql_err)?;
        let mut columns = std::collections::BTreeSet::new();
        for row in rows {
            columns.insert(row.map_err(sql_err)?);
        }
        columns
    };
    if !existing_session_columns.contains("input_tokens") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN input_tokens INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_session_columns.contains("output_tokens") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN output_tokens INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_session_columns.contains("estimated_cost_usd") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN estimated_cost_usd REAL NOT NULL DEFAULT 0.0",
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_session_columns.contains("status") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN status TEXT NOT NULL DEFAULT 'active'",
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_session_columns.contains("created_at_ms") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN created_at_ms INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_session_columns.contains("updated_at_ms") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN updated_at_ms INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map_err(sql_err)?;
    }
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status)",
        [],
    )
    .map_err(sql_err)?;
    conn.execute(
        r"CREATE INDEX IF NOT EXISTS idx_sessions_status_model_last_activity
            ON sessions(status COLLATE NOCASE, model COLLATE NOCASE, last_activity DESC)",
        [],
    )
    .map_err(sql_err)?;
    conn.execute(
        r"CREATE INDEX IF NOT EXISTS idx_sessions_model_last_activity
            ON sessions(model COLLATE NOCASE, last_activity DESC)",
        [],
    )
    .map_err(sql_err)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions(created_at)",
        [],
    )
    .map_err(sql_err)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sessions_message_count ON sessions(message_count)",
        [],
    )
    .map_err(sql_err)?;
    // The reconciliation reads and writes the token summary columns. Legacy
    // databases receive those columns above, so this must remain after every
    // sessions-table ALTER and before the schema transaction commits.
    reconcile_legacy_session_summaries(conn)?;

    conn.execute_batch("COMMIT;").map_err(sql_err)?;
    Ok(())
}

fn reconcile_legacy_session_summaries(conn: &Connection) -> Result<()> {
    const MIGRATION_ID: &str = "session.0003.reconcile-message-summaries";
    conn.execute(
        "CREATE TABLE IF NOT EXISTS session_store_migrations (
            migration_id TEXT PRIMARY KEY,
            applied_at_ms INTEGER NOT NULL
        )",
        [],
    )
    .map_err(sql_err)?;
    let applied = conn
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM session_store_migrations WHERE migration_id=?1
            )",
            params![MIGRATION_ID],
            |row| row.get::<_, bool>(0),
        )
        .map_err(sql_err)?;
    if applied {
        return Ok(());
    }
    // Legacy databases can already contain `sessions` rows before the
    // external-content FTS table and its UPDATE trigger are installed.
    // Rebuild first; otherwise the trigger's delete command targets a missing
    // FTS row and SQLite reports the database image as malformed.
    conn.execute(
        "INSERT INTO sessions_fts(sessions_fts) VALUES('rebuild')",
        [],
    )
    .map_err(sql_err)?;
    conn.execute_batch(
        r"UPDATE sessions
              SET message_count = (
                      SELECT COUNT(*) FROM messages WHERE session_id=sessions.session_id
                  ),
                  input_tokens = COALESCE((
                      SELECT SUM(
                          CASE WHEN token_usage_json IS NOT NULL
                                    AND json_valid(token_usage_json)
                                    AND json_type(token_usage_json, '$.input_tokens') = 'integer'
                                    AND json_extract(token_usage_json, '$.input_tokens') >= 0
                               THEN COALESCE(json_extract(token_usage_json, '$.input_tokens'), 0)
                               ELSE 0 END
                      )
                        FROM messages WHERE session_id=sessions.session_id
                  ), 0),
                  output_tokens = COALESCE((
                      SELECT SUM(
                          CASE WHEN token_usage_json IS NOT NULL
                                    AND json_valid(token_usage_json)
                                    AND json_type(token_usage_json, '$.output_tokens') = 'integer'
                                    AND json_extract(token_usage_json, '$.output_tokens') >= 0
                               THEN COALESCE(json_extract(token_usage_json, '$.output_tokens'), 0)
                               ELSE 0 END
                      )
                        FROM messages WHERE session_id=sessions.session_id
                  ), 0)",
    )
    .map_err(sql_err)?;
    conn.execute(
        "INSERT INTO session_store_migrations(migration_id, applied_at_ms)
         VALUES (?1, ?2)",
        params![MIGRATION_ID, Utc::now().timestamp_millis().max(0)],
    )
    .map_err(sql_err)?;
    Ok(())
}

fn ensure_session_event_sequence_constraint(conn: &Connection) -> Result<()> {
    let duplicate_groups: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM (\
                 SELECT session_id, sequence FROM session_events \
                 GROUP BY session_id, sequence HAVING COUNT(*) > 1\
             )",
            [],
            |row| row.get(0),
        )
        .map_err(sql_err)?;
    if duplicate_groups > 0 {
        // Legacy writers could race while allocating sequence numbers. Keep
        // every event and derive one stable session-local order from the
        // existing sequence, timestamp, and row id; then keep JSON payload
        // sequence metadata consistent with the durable column. This runs in
        // the schema transaction before the unique index is created.
        conn.execute_batch(
            r"
            WITH resequenced AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY session_id
                        ORDER BY sequence ASC, created_at_ms ASC, id ASC
                    ) - 1 AS next_sequence
                  FROM session_events
            )
            UPDATE session_events
               SET sequence = (
                       SELECT next_sequence
                         FROM resequenced
                        WHERE resequenced.id = session_events.id
                   ),
                   event_json = CASE
                       WHEN json_valid(event_json) THEN json_set(
                           event_json,
                           '$.sequence',
                           (
                               SELECT next_sequence
                                 FROM resequenced
                                WHERE resequenced.id = session_events.id
                           )
                       )
                       ELSE event_json
                   END
             WHERE id IN (SELECT id FROM resequenced);
            ",
        )
        .map_err(sql_err)?;
        tracing::warn!(
            duplicate_groups,
            "repaired legacy duplicate session event sequences before enforcing uniqueness"
        );
    }
    conn.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_session_events_session_sequence \
         ON session_events(session_id, sequence)",
    )
    .map_err(sql_err)?;
    Ok(())
}

fn table_columns(conn: &Connection, table: &str) -> Result<std::collections::BTreeSet<String>> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(sql_err)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(sql_err)?;
    let mut columns = std::collections::BTreeSet::new();
    for row in rows {
        columns.insert(row.map_err(sql_err)?);
    }
    Ok(columns)
}

fn ensure_messages_schema(conn: &Connection) -> Result<()> {
    let existing_message_columns = table_columns(conn, "messages")?;
    if !existing_message_columns.contains("stable_message_id") {
        conn.execute("ALTER TABLE messages ADD COLUMN stable_message_id TEXT", [])
            .map_err(sql_err)?;
        conn.execute_batch(
            r#"
            UPDATE messages
               SET stable_message_id = 'legacy:' || lower(hex(CAST(session_id AS BLOB))) || ':' || sequence
             WHERE stable_message_id IS NULL OR stable_message_id = '';
            "#,
        )
        .map_err(sql_err)?;
    }
    if !existing_message_columns.contains("content_json") {
        conn.execute(
            r#"ALTER TABLE messages ADD COLUMN content_json TEXT NOT NULL DEFAULT '[{"type":"text","text":""}]'"#,
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_message_columns.contains("blocks_count") {
        conn.execute(
            "ALTER TABLE messages ADD COLUMN blocks_count INTEGER NOT NULL DEFAULT 1",
            [],
        )
        .map_err(sql_err)?;
    }
    if !existing_message_columns.contains("tool_use_id") {
        conn.execute("ALTER TABLE messages ADD COLUMN tool_use_id TEXT", [])
            .map_err(sql_err)?;
    }
    if !existing_message_columns.contains("tool_name") {
        conn.execute("ALTER TABLE messages ADD COLUMN tool_name TEXT", [])
            .map_err(sql_err)?;
    }
    if !existing_message_columns.contains("token_usage_json") {
        conn.execute("ALTER TABLE messages ADD COLUMN token_usage_json TEXT", [])
            .map_err(sql_err)?;
    }

    if table_columns(conn, "message_blocks").is_ok_and(|columns| columns.contains("block_type")) {
        conn.execute_batch(
            r#"
            UPDATE messages
               SET content_json = COALESCE(
                       (
                         SELECT json_group_array(json(block_json))
                           FROM (
                             SELECT json_object(
                                      'type',
                                      CASE
                                        WHEN block_type = 'tool_use' THEN 'tool_use'
                                        WHEN block_type = 'tool_result' THEN 'tool_result'
                                        WHEN block_type = 'thinking' THEN 'thinking'
                                        ELSE 'text'
                                      END,
                                      'text', COALESCE(text, tool_output, ''),
                                      'id', COALESCE(tool_id, ''),
                                      'name', COALESCE(tool_name, ''),
                                      'input', COALESCE(tool_input, ''),
                                      'content', COALESCE(tool_output, ''),
                                      'is_error', CASE WHEN is_error = 0 THEN json('false') ELSE json('true') END
                                    ) AS block_json
                               FROM message_blocks
                              WHERE message_blocks.message_id = messages.id
                              ORDER BY block_order ASC
                           )
                       ),
                       content_json
                   ),
                   blocks_count = COALESCE(
                       (
                         SELECT COUNT(*)
                           FROM message_blocks
                          WHERE message_blocks.message_id = messages.id
                       ),
                       blocks_count
                   ),
                   tool_use_id = COALESCE(
                       (
                         SELECT tool_id
                           FROM message_blocks
                          WHERE message_blocks.message_id = messages.id
                            AND tool_id IS NOT NULL
                          ORDER BY block_order ASC
                          LIMIT 1
                       ),
                       tool_use_id
                   ),
                   tool_name = COALESCE(
                       (
                         SELECT tool_name
                           FROM message_blocks
                          WHERE message_blocks.message_id = messages.id
                            AND tool_name IS NOT NULL
                          ORDER BY block_order ASC
                          LIMIT 1
                       ),
                       tool_name
                   )
             WHERE EXISTS (
                       SELECT 1
                         FROM message_blocks
                        WHERE message_blocks.message_id = messages.id
                   );
            "#,
        )
        .map_err(sql_err)?;
    }

    conn.execute_batch(
        r#"
        CREATE UNIQUE INDEX IF NOT EXISTS uq_messages_stable_message_id
            ON messages(stable_message_id);
        CREATE TRIGGER IF NOT EXISTS messages_stable_id_required_insert
        BEFORE INSERT ON messages
        WHEN NEW.stable_message_id IS NULL OR NEW.stable_message_id = ''
        BEGIN
            SELECT RAISE(ABORT, 'messages.stable_message_id is required');
        END;
        CREATE TRIGGER IF NOT EXISTS messages_stable_id_required_update
        BEFORE UPDATE OF stable_message_id ON messages
        WHEN NEW.stable_message_id IS NULL OR NEW.stable_message_id = ''
        BEGIN
            SELECT RAISE(ABORT, 'messages.stable_message_id is required');
        END;
        DROP TRIGGER IF EXISTS messages_fts_ai;
        DROP TRIGGER IF EXISTS messages_fts_ad;
        DROP TRIGGER IF EXISTS messages_fts_au;
        DROP TABLE IF EXISTS messages_fts;
        CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
            session_id UNINDEXED,
            role,
            content_text,
            tool_name,
            content=messages,
            content_rowid=id
        );
        CREATE TRIGGER IF NOT EXISTS messages_fts_ai AFTER INSERT ON messages BEGIN
            INSERT INTO messages_fts(rowid, session_id, role, content_text, tool_name)
            VALUES (new.id, new.session_id, new.role,
                    (SELECT group_concat(json_extract(value,'$.text'),' ') FROM json_each(new.content_json) WHERE json_extract(value,'$.type')='text'),
                    new.tool_name);
        END;
        CREATE TRIGGER IF NOT EXISTS messages_fts_ad AFTER DELETE ON messages BEGIN
            INSERT INTO messages_fts(messages_fts, rowid, session_id, role, content_text, tool_name)
            VALUES ('delete', old.id, old.session_id, old.role, NULL, old.tool_name);
        END;
        CREATE TRIGGER IF NOT EXISTS messages_fts_au AFTER UPDATE ON messages BEGIN
            INSERT INTO messages_fts(messages_fts, rowid, session_id, role, content_text, tool_name)
            VALUES ('delete', old.id, old.session_id, old.role, NULL, old.tool_name);
            INSERT INTO messages_fts(rowid, session_id, role, content_text, tool_name)
            VALUES (new.id, new.session_id, new.role,
                    (SELECT group_concat(json_extract(value,'$.text'),' ') FROM json_each(new.content_json) WHERE json_extract(value,'$.type')='text'),
                    new.tool_name);
        END;
        INSERT INTO messages_fts(rowid, session_id, role, content_text, tool_name)
        SELECT id,
               session_id,
               role,
               (SELECT group_concat(json_extract(value,'$.text'),' ')
                  FROM json_each(messages.content_json)
                 WHERE json_extract(value,'$.type')='text'),
               tool_name
          FROM messages;
        "#,
    )
    .map_err(sql_err)?;

    Ok(())
}

fn ensure_session_runtime_outbox_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS session_runtime_outbox (
            request_id TEXT PRIMARY KEY,
            turn_id TEXT NOT NULL UNIQUE,
            message_id TEXT NOT NULL UNIQUE,
            session_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            status TEXT NOT NULL,
            runtime_commit_cursor INTEGER,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at_ms INTEGER NOT NULL,
            claim_owner TEXT,
            claim_expires_at_ms INTEGER,
            failure_class TEXT,
            last_error TEXT,
            revision INTEGER NOT NULL DEFAULT 0,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            runtime_options_json TEXT,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
            FOREIGN KEY (message_id) REFERENCES messages(stable_message_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_session_runtime_outbox_claim
            ON session_runtime_outbox(status, next_attempt_at_ms, claim_expires_at_ms, sequence);
        CREATE INDEX IF NOT EXISTS idx_session_runtime_outbox_session
            ON session_runtime_outbox(session_id, sequence);
        CREATE TABLE IF NOT EXISTS session_runtime_outbox_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL,
            action TEXT NOT NULL,
            actor TEXT,
            reason TEXT,
            from_status TEXT NOT NULL,
            to_status TEXT NOT NULL,
            attempts INTEGER NOT NULL,
            created_at_ms INTEGER NOT NULL,
            FOREIGN KEY (request_id) REFERENCES session_runtime_outbox(request_id) ON DELETE CASCADE
        );
        "#,
    )
    .map_err(sql_err)?;
    let columns = table_columns(conn, "session_runtime_outbox")?;
    if !columns.contains("runtime_options_json") {
        conn.execute(
            "ALTER TABLE session_runtime_outbox ADD COLUMN runtime_options_json TEXT",
            [],
        )
        .map_err(sql_err)?;
    }
    Ok(())
}

fn ensure_session_mission_outbox_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS session_mission_outbox (
            request_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            title TEXT NOT NULL,
            workspace_key TEXT NOT NULL,
            operation TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at_ms INTEGER NOT NULL,
            claim_owner TEXT,
            claim_expires_at_ms INTEGER,
            failure_class TEXT,
            last_error TEXT,
            revision INTEGER NOT NULL DEFAULT 0,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_session_mission_outbox_claim
            ON session_mission_outbox(workspace_key, status, next_attempt_at_ms, claim_expires_at_ms, created_at_ms);
        CREATE INDEX IF NOT EXISTS idx_session_mission_outbox_session
            ON session_mission_outbox(workspace_key, session_id, created_at_ms);
        CREATE TABLE IF NOT EXISTS session_mission_outbox_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL,
            action TEXT NOT NULL,
            actor TEXT,
            reason TEXT,
            from_status TEXT NOT NULL,
            to_status TEXT NOT NULL,
            attempts INTEGER NOT NULL,
            created_at_ms INTEGER NOT NULL,
            FOREIGN KEY (request_id) REFERENCES session_mission_outbox(request_id) ON DELETE CASCADE
        );
        "#,
    )
    .map_err(sql_err)
}

fn migrate_legacy_session_domain_events(conn: &Connection) -> Result<()> {
    conn.execute(
        r#"
        UPDATE session_events
           SET event_type = ?1,
               event_json = json_set(
                   event_json,
                   '$.scope',
                   CASE json_extract(event_json, '$.scope')
                       WHEN 'task' THEN 'application_task'
                       ELSE json_extract(event_json, '$.scope')
                   END
               )
         WHERE event_type = 'RuntimeEvent'
           AND json_valid(event_json)
           AND json_extract(event_json, '$.scope') IN
               ('session', 'message', 'turn', 'context', 'tool', 'memory', 'policy', 'task', 'mfg')
        "#,
        params![SESSION_DOMAIN_EVENT_TYPE],
    )
    .map_err(sql_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// SessionRecord
// ---------------------------------------------------------------------------

/// A serialisable snapshot of a single session's metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionRecord {
    /// Unique session identifier (UUID string).
    pub session_id: String,
    /// Platform name (e.g. `"telegram"`, `"api_server"`).
    pub platform: String,
    /// Platform-native chat / room identifier.
    pub chat_id: String,
    /// Optional platform-native user identifier.
    pub user_id: Option<String>,
    /// Active model name for this session (if any).
    pub model: Option<String>,
    /// ISO 8601 creation timestamp.
    pub created_at: String,
    /// ISO 8601 timestamp of the last received message.
    pub last_activity: String,
    /// Number of messages processed in this session.
    pub message_count: i64,
    /// Name of the [`SessionResetPolicy`] variant (stored as text).
    pub reset_policy: String,
    /// Optional JSON blob for arbitrary extra metadata.
    pub metadata_json: Option<String>,
    /// Cumulative input tokens (prompt).
    pub input_tokens: i64,
    /// Cumulative output tokens (completion).
    pub output_tokens: i64,
    /// Estimated total cost in USD.
    pub estimated_cost_usd: f64,
    /// Lifecycle status (`active`, `closed`, etc.).
    pub status: String,
}

// ---------------------------------------------------------------------------
// Row → SessionRecord mapper
// ---------------------------------------------------------------------------

fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRecord> {
    Ok(SessionRecord {
        session_id: row.get(0)?,
        platform: row.get(1)?,
        chat_id: row.get(2)?,
        user_id: row.get(3)?,
        model: row.get(4)?,
        created_at: row.get(5)?,
        last_activity: row.get(6)?,
        message_count: row.get(7)?,
        reset_policy: row.get(8)?,
        metadata_json: row.get(9)?,
        input_tokens: row.get(10)?,
        output_tokens: row.get(11)?,
        estimated_cost_usd: row.get(12)?,
        status: row.get(13)?,
    })
}

fn row_to_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionMessage> {
    Ok(SessionMessage {
        stable_message_id: row.get(0)?,
        session_id: row.get(1)?,
        sequence: row.get::<_, i64>(2)? as usize,
        role: row.get(3)?,
        content_json: row.get(4)?,
        blocks_count: row.get::<_, i64>(5)? as usize,
        tool_use_id: row.get(6)?,
        tool_name: row.get(7)?,
        token_usage_json: row.get(8)?,
        created_at_ms: row.get::<_, i64>(9)? as u64,
    })
}

fn row_to_outbox(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRuntimeOutboxRecord> {
    Ok(SessionRuntimeOutboxRecord {
        request_id: row.get(0)?,
        turn_id: row.get(1)?,
        message_id: row.get(2)?,
        session_id: row.get(3)?,
        sequence: row.get::<_, i64>(4)? as usize,
        status: OutboxStatus::parse(&row.get::<_, String>(5)?)?,
        runtime_commit_cursor: row.get::<_, Option<i64>>(6)?.map(|value| value as u64),
        attempts: row.get::<_, i64>(7)? as u32,
        next_attempt_at_ms: row.get::<_, i64>(8)? as u64,
        claim_owner: row.get(9)?,
        claim_expires_at_ms: row.get::<_, Option<i64>>(10)?.map(|value| value as u64),
        failure_class: row
            .get::<_, Option<String>>(11)?
            .as_deref()
            .map(OutboxFailureClass::parse)
            .transpose()?,
        last_error: row.get(12)?,
        revision: row.get::<_, i64>(13)? as u64,
        created_at_ms: row.get::<_, i64>(14)? as u64,
        updated_at_ms: row.get::<_, i64>(15)? as u64,
        runtime_options_json: row.get(16)?,
    })
}

fn query_outbox(conn: &Connection, request_id: &str) -> Result<Option<SessionRuntimeOutboxRecord>> {
    conn.query_row(
        r"SELECT request_id, turn_id, message_id, session_id, sequence, status,
                  runtime_commit_cursor, attempts, next_attempt_at_ms, claim_owner,
                  claim_expires_at_ms, failure_class, last_error, revision,
                  created_at_ms, updated_at_ms, runtime_options_json
             FROM session_runtime_outbox WHERE request_id = ?1",
        params![request_id],
        row_to_outbox,
    )
    .optional()
    .map_err(sql_err)
}

fn refresh_session_message_summary_tx(
    tx: &rusqlite::Transaction<'_>,
    session_id: &str,
    activity_ms: u64,
) -> Result<()> {
    let activity_ms = i64::try_from(activity_ms).unwrap_or(i64::MAX);
    let activity = DateTime::<Utc>::from_timestamp_millis(activity_ms)
        .unwrap_or_else(Utc::now)
        .to_rfc3339();
    tx.execute(
        r"UPDATE sessions
              SET message_count = (
                      SELECT COUNT(*) FROM messages WHERE session_id = ?1
                  ),
                  last_activity = CASE
                      WHEN updated_at_ms <= ?3 THEN ?2
                      ELSE last_activity
                  END,
                  updated_at_ms = MAX(updated_at_ms, ?3)
            WHERE session_id = ?1",
        params![session_id, activity, activity_ms],
    )
    .map_err(sql_err)?;
    Ok(())
}

fn refresh_session_usage_summary_tx(
    tx: &rusqlite::Transaction<'_>,
    session_id: &str,
) -> Result<()> {
    tx.execute(
        r"UPDATE sessions
              SET input_tokens = COALESCE((
                      SELECT SUM(
                          CASE WHEN token_usage_json IS NOT NULL
                                    AND json_valid(token_usage_json)
                                    AND json_type(token_usage_json, '$.input_tokens') = 'integer'
                                    AND json_extract(token_usage_json, '$.input_tokens') >= 0
                               THEN COALESCE(json_extract(token_usage_json, '$.input_tokens'), 0)
                               ELSE 0 END
                      )
                        FROM messages WHERE session_id=?1
                  ), 0),
                  output_tokens = COALESCE((
                      SELECT SUM(
                          CASE WHEN token_usage_json IS NOT NULL
                                    AND json_valid(token_usage_json)
                                    AND json_type(token_usage_json, '$.output_tokens') = 'integer'
                                    AND json_extract(token_usage_json, '$.output_tokens') >= 0
                               THEN COALESCE(json_extract(token_usage_json, '$.output_tokens'), 0)
                               ELSE 0 END
                      )
                        FROM messages WHERE session_id=?1
                  ), 0)
            WHERE session_id=?1",
        params![session_id],
    )
    .map_err(sql_err)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_outbox_history(
    tx: &rusqlite::Transaction<'_>,
    record: &SessionRuntimeOutboxRecord,
    action: &str,
    actor: Option<&str>,
    reason: Option<&str>,
    from_status: &str,
    to_status: &str,
    now_ms: u64,
) -> Result<()> {
    tx.execute(
        r"INSERT INTO session_runtime_outbox_history
            (request_id, action, actor, reason, from_status, to_status, attempts, created_at_ms)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            record.request_id,
            action,
            actor,
            reason,
            from_status,
            to_status,
            record.attempts as i64,
            now_ms as i64,
        ],
    )
    .map_err(sql_err)?;
    Ok(())
}

fn validate_outbox_identity(
    message: &SessionMessage,
    request: &SessionRuntimeOutboxRequest,
) -> Result<()> {
    if message.session_id.trim().is_empty()
        || request.request_id.trim().is_empty()
        || request.turn_id.trim().is_empty()
        || request.message_id.trim().is_empty()
    {
        return Err(MemoryError::Store(
            "session/runtime outbox identities must be non-empty".to_string(),
        ));
    }
    Ok(())
}

fn row_to_mission_outbox(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionMissionOutboxRecord> {
    Ok(SessionMissionOutboxRecord {
        request_id: row.get(0)?,
        session_id: row.get(1)?,
        title: row.get(2)?,
        workspace_key: row.get(3)?,
        operation: SessionMissionOutboxOperation::parse(&row.get::<_, String>(4)?)?,
        status: OutboxStatus::parse(&row.get::<_, String>(5)?)?,
        attempts: row.get::<_, i64>(6)? as u32,
        next_attempt_at_ms: row.get::<_, i64>(7)? as u64,
        claim_owner: row.get(8)?,
        claim_expires_at_ms: row.get::<_, Option<i64>>(9)?.map(|value| value as u64),
        failure_class: row
            .get::<_, Option<String>>(10)?
            .as_deref()
            .map(OutboxFailureClass::parse)
            .transpose()?,
        last_error: row.get(11)?,
        revision: row.get::<_, i64>(12)? as u64,
        created_at_ms: row.get::<_, i64>(13)? as u64,
        updated_at_ms: row.get::<_, i64>(14)? as u64,
    })
}

fn query_mission_outbox(
    conn: &Connection,
    request_id: &str,
) -> Result<Option<SessionMissionOutboxRecord>> {
    conn.query_row(
        r"SELECT request_id, session_id, title, workspace_key, operation, status,
                  attempts, next_attempt_at_ms, claim_owner, claim_expires_at_ms,
                  failure_class, last_error, revision, created_at_ms, updated_at_ms
             FROM session_mission_outbox WHERE request_id = ?1",
        params![request_id],
        row_to_mission_outbox,
    )
    .optional()
    .map_err(sql_err)
}

#[allow(clippy::too_many_arguments)]
fn append_mission_outbox_history(
    tx: &rusqlite::Transaction<'_>,
    record: &SessionMissionOutboxRecord,
    action: &str,
    actor: Option<&str>,
    reason: Option<&str>,
    from_status: &str,
    to_status: &str,
    now_ms: u64,
) -> Result<()> {
    tx.execute(
        r"INSERT INTO session_mission_outbox_history
            (request_id, action, actor, reason, from_status, to_status, attempts, created_at_ms)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            record.request_id,
            action,
            actor,
            reason,
            from_status,
            to_status,
            record.attempts as i64,
            now_ms as i64,
        ],
    )
    .map_err(sql_err)?;
    Ok(())
}

fn validate_mission_outbox_request(request: &SessionMissionOutboxRequest) -> Result<()> {
    if request.request_id.trim().is_empty()
        || request.session_id.trim().is_empty()
        || request.title.trim().is_empty()
        || request.workspace_key.trim().is_empty()
    {
        return Err(MemoryError::Store(
            "session/mission outbox identities must be non-empty".to_string(),
        ));
    }
    Ok(())
}

fn insert_mission_outbox(
    tx: &rusqlite::Transaction<'_>,
    request: &SessionMissionOutboxRequest,
) -> Result<SessionMissionOutboxRecord> {
    if let Some(existing) = query_mission_outbox(tx, &request.request_id)? {
        if existing.session_id == request.session_id
            && existing.workspace_key == request.workspace_key
            && existing.operation == request.operation
        {
            // A title is mutable presentation metadata, not part of the
            // Session -> Mission lifecycle identity. Surface registration and
            // Runtime hydration may legitimately supply different display
            // titles for the same durable register intent.
            return Ok(existing);
        }
        return Err(MemoryError::Store(format!(
            "mission outbox request_id `{}` is already bound to another lifecycle intent",
            request.request_id
        )));
    }
    tx.execute(
        r"INSERT INTO session_mission_outbox
            (request_id, session_id, title, workspace_key, operation, status, attempts,
             next_attempt_at_ms, revision, created_at_ms, updated_at_ms)
           VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6, 0, ?6, ?6)",
        params![
            request.request_id,
            request.session_id,
            request.title,
            request.workspace_key,
            request.operation.as_str(),
            request.created_at_ms as i64,
        ],
    )
    .map_err(sql_err)?;
    let record = query_mission_outbox(tx, &request.request_id)?.ok_or_else(|| {
        MemoryError::Store("mission outbox insert produced no readable row".to_string())
    })?;
    append_mission_outbox_history(
        tx,
        &record,
        "enqueue",
        None,
        None,
        "none",
        OutboxStatus::Pending.as_str(),
        request.created_at_ms,
    )?;
    Ok(record)
}

// ---------------------------------------------------------------------------
// SessionEvent / SessionSnapshot
// ---------------------------------------------------------------------------

/// A recorded mutation event for a session, enabling event-sourced
/// reconstruction and time-travel debugging.
///
/// Each event is associated with a monotonically-increasing `sequence`
/// that orders it within the session's event log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionEvent {
    pub session_id: String,
    pub event_type: String,
    pub event_json: String,
    pub sequence: usize,
    pub created_at_ms: u64,
}

/// A full-message-list snapshot taken at a specific event index, used
/// as a basis for fast replay from that point forward.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub session_id: String,
    /// Event sequence index this snapshot corresponds to.
    pub event_idx: usize,
    /// Full JSON array of all messages at that point in time.
    pub messages_json: String,
    pub created_at_ms: u64,
}

fn row_to_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionEvent> {
    Ok(SessionEvent {
        session_id: row.get(1)?,
        event_type: row.get(2)?,
        event_json: row.get(3)?,
        sequence: row.get::<_, i64>(4)? as usize,
        created_at_ms: row.get::<_, i64>(5)? as u64,
    })
}

fn row_to_snapshot(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionSnapshot> {
    Ok(SessionSnapshot {
        session_id: row.get(1)?,
        event_idx: row.get::<_, i64>(2)? as usize,
        messages_json: row.get(3)?,
        created_at_ms: row.get::<_, i64>(4)? as u64,
    })
}

fn escape_like_pattern(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '%' | '_' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

fn session_sort_expression(sort: &str) -> &'static str {
    match sort {
        "created_at" => "created_at",
        "message_count" => "message_count",
        "model" => "COALESCE(model, '') COLLATE NOCASE",
        "title" => "COALESCE(json_extract(metadata_json, '$.title'), '') COLLATE NOCASE",
        _ => "last_activity",
    }
}

fn session_sort_order(order: &str) -> &'static str {
    if order.eq_ignore_ascii_case("asc") {
        "ASC"
    } else {
        "DESC"
    }
}

fn iso_to_ms(value: &str) -> i64 {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or_else(|_| Utc::now().timestamp_millis())
}

fn session_list_where_clause(opts: &SessionListOptions<'_>) -> (String, Vec<Value>) {
    let mut where_parts: Vec<&'static str> = Vec::new();
    let mut values = Vec::new();

    if let Some(status) = opts.status.map(str::trim).filter(|s| !s.is_empty()) {
        where_parts.push("status = ? COLLATE NOCASE");
        values.push(Value::Text(status.to_string()));
    }
    if let Some(model) = opts.model.map(str::trim).filter(|s| !s.is_empty()) {
        where_parts.push("model = ? COLLATE NOCASE");
        values.push(Value::Text(model.to_string()));
    }
    if let Some(query) = opts.query.map(str::trim).filter(|s| !s.is_empty()) {
        where_parts.push(
            "(session_id LIKE ? ESCAPE '\\' COLLATE NOCASE
              OR platform LIKE ? ESCAPE '\\' COLLATE NOCASE
              OR chat_id LIKE ? ESCAPE '\\' COLLATE NOCASE
              OR COALESCE(user_id, '') LIKE ? ESCAPE '\\' COLLATE NOCASE
              OR COALESCE(model, '') LIKE ? ESCAPE '\\' COLLATE NOCASE
              OR status LIKE ? ESCAPE '\\' COLLATE NOCASE
              OR COALESCE(metadata_json, '') LIKE ? ESCAPE '\\' COLLATE NOCASE)",
        );
        let pattern = format!("%{}%", escape_like_pattern(query));
        for _ in 0..7 {
            values.push(Value::Text(pattern.clone()));
        }
    }

    let clause = if where_parts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_parts.join(" AND "))
    };
    (clause, values)
}

// ---------------------------------------------------------------------------
// SqliteSessionStore
// ---------------------------------------------------------------------------

/// Persistent, SQLite-backed session store.
///
/// Uses an r2d2 connection pool so that multiple concurrent operations can
/// share a bounded set of WAL-enabled connections.
///
/// # In-memory mode (tests)
///
/// Pass `":memory:"` as the path. Pool size is 1 for in-memory so that all
/// operations share the same database handle.
#[derive(Debug, Clone)]
pub struct SqliteSessionStore {
    pool: Pool<SqliteConnectionManager>,
}

impl SqliteSessionStore {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Open (or create) a session database at `path`.
    ///
    /// Creates any missing parent directories and initialises the schema if
    /// the database is new.
    pub fn open(path: &Path) -> Result<Self> {
        let handle = storage::StorageHandle::sqlite(
            "session",
            path.to_path_buf(),
            "memory",
            "session_store_path_adapter_since_0.9.315",
        );
        Self::open_storage_handle(&handle)
    }

    /// Open a session database through a typed storage handle.
    pub fn open_storage_handle(handle: &storage::StorageHandle) -> Result<Self> {
        if handle.backend != storage::StorageBackendKind::Sqlite {
            return Err(MemoryError::Store(format!(
                "storage handle `{}` is not sqlite-backed",
                handle.domain
            )));
        }
        let path = &handle.path;
        let db_path = path
            .to_str()
            .ok_or_else(|| MemoryError::Store("non-UTF-8 session db path".to_string()))?
            .to_owned();
        // Create parent directories if needed (skip for ":memory:").
        if db_path != IN_MEMORY_PATH {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    MemoryError::Store(format!("cannot create session db dir: {e}"))
                })?;
            }
        }
        let pool = new_pool(&db_path, 10)?;
        let store = Self { pool };
        let conn = store.conn()?;
        init_schema(&conn)?;
        Ok(store)
    }

    /// Open an in-memory session database (useful for testing).
    pub fn open_in_memory() -> Result<Self> {
        let pool = new_pool(IN_MEMORY_PATH, 1)?;
        let store = Self { pool };
        let conn = store.conn()?;
        init_schema(&conn)?;
        Ok(store)
    }

    pub fn conn(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        let conn = self
            .pool
            .get()
            .map_err(|e| MemoryError::Store(e.to_string()))?;
        set_conn_pragmas(&conn)?;
        Ok(conn)
    }

    // -----------------------------------------------------------------------
    // CRUD
    // -----------------------------------------------------------------------

    /// Insert a new session record.
    ///
    /// Uses `INSERT OR IGNORE` so calling this for an already-existing session
    /// is a harmless no-op.
    pub fn create_session(&self, session: &SessionRecord) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            r"INSERT OR IGNORE INTO sessions
               (session_id, platform, chat_id, user_id, model,
                created_at, last_activity, message_count, reset_policy, metadata_json,
                input_tokens, output_tokens, estimated_cost_usd, status,
                created_at_ms, updated_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                session.session_id,
                session.platform,
                session.chat_id,
                session.user_id,
                session.model,
                session.created_at,
                session.last_activity,
                session.message_count,
                session.reset_policy,
                session.metadata_json,
                session.input_tokens,
                session.output_tokens,
                session.estimated_cost_usd,
                session.status,
                iso_to_ms(&session.created_at),
                iso_to_ms(&session.last_activity),
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Retrieve a session record by its ID, or `None` if not found.
    pub fn get_session(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT session_id, platform, chat_id, user_id, model,
                      created_at, last_activity, message_count, reset_policy, metadata_json,
                      input_tokens, output_tokens, estimated_cost_usd, status
               FROM sessions WHERE session_id = ?1",
            params![session_id],
            row_to_record,
        )
        .optional()
        .map_err(sql_err)
    }

    /// Overwrite all mutable fields of an existing session record.
    ///
    /// `session_id` is used as the lookup key; the row is silently unchanged
    /// if it does not exist.
    pub fn update_session(&self, session: &SessionRecord) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            r"UPDATE sessions SET
               platform      = ?2,
               chat_id       = ?3,
               user_id       = ?4,
               model         = ?5,
               last_activity = ?6,
               message_count = ?7,
               reset_policy  = ?8,
               metadata_json = ?9,
               input_tokens  = ?10,
               output_tokens = ?11,
               estimated_cost_usd = ?12,
               status = ?13,
               updated_at_ms = ?14
               WHERE session_id = ?1",
            params![
                session.session_id,
                session.platform,
                session.chat_id,
                session.user_id,
                session.model,
                session.last_activity,
                session.message_count,
                session.reset_policy,
                session.metadata_json,
                session.input_tokens,
                session.output_tokens,
                session.estimated_cost_usd,
                session.status,
                iso_to_ms(&session.last_activity),
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Upsert a session record (insert or replace all fields).
    ///
    /// Equivalent to calling [`create_session`] then [`update_session`].  Use
    /// this when you don't know whether the row already exists.
    pub fn upsert_session(&self, session: &SessionRecord) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            r"INSERT INTO sessions
               (session_id, platform, chat_id, user_id, model,
                created_at, last_activity, message_count, reset_policy, metadata_json,
                input_tokens, output_tokens, estimated_cost_usd, status,
                created_at_ms, updated_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
               ON CONFLICT(session_id) DO UPDATE SET
                 platform = excluded.platform,
                 chat_id = excluded.chat_id,
                 user_id = excluded.user_id,
                 model = excluded.model,
                 created_at = excluded.created_at,
                 last_activity = excluded.last_activity,
                 message_count = excluded.message_count,
                 reset_policy = excluded.reset_policy,
                 metadata_json = excluded.metadata_json,
                 input_tokens = excluded.input_tokens,
                 output_tokens = excluded.output_tokens,
                 estimated_cost_usd = excluded.estimated_cost_usd,
                 status = excluded.status,
                 created_at_ms = excluded.created_at_ms,
                 updated_at_ms = excluded.updated_at_ms",
            params![
                session.session_id,
                session.platform,
                session.chat_id,
                session.user_id,
                session.model,
                session.created_at,
                session.last_activity,
                session.message_count,
                session.reset_policy,
                session.metadata_json,
                session.input_tokens,
                session.output_tokens,
                session.estimated_cost_usd,
                session.status,
                iso_to_ms(&session.created_at),
                iso_to_ms(&session.last_activity),
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Atomically persist a Session record and enqueue its Mission lifecycle
    /// registration. The Runtime bridge is the only consumer of this intent;
    /// callers never need to dual-write the Session and Mission stores.
    pub fn upsert_session_with_mission_outbox(
        &self,
        session: &SessionRecord,
        request: &SessionMissionOutboxRequest,
    ) -> Result<SessionMissionOutboxRecord> {
        validate_mission_outbox_request(request)?;
        if request.session_id != session.session_id {
            return Err(MemoryError::Store(
                "session/mission outbox session identity does not match record".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        tx.execute(
            r"INSERT INTO sessions
               (session_id, platform, chat_id, user_id, model,
                created_at, last_activity, message_count, reset_policy, metadata_json,
                input_tokens, output_tokens, estimated_cost_usd, status,
                created_at_ms, updated_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
               ON CONFLICT(session_id) DO UPDATE SET
                 platform = excluded.platform,
                 chat_id = excluded.chat_id,
                 user_id = excluded.user_id,
                 model = excluded.model,
                 created_at = excluded.created_at,
                 last_activity = excluded.last_activity,
                 message_count = excluded.message_count,
                 reset_policy = excluded.reset_policy,
                 metadata_json = excluded.metadata_json,
                 input_tokens = excluded.input_tokens,
                 output_tokens = excluded.output_tokens,
                 estimated_cost_usd = excluded.estimated_cost_usd,
                 status = excluded.status,
                 created_at_ms = excluded.created_at_ms,
                 updated_at_ms = excluded.updated_at_ms",
            params![
                session.session_id,
                session.platform,
                session.chat_id,
                session.user_id,
                session.model,
                session.created_at,
                session.last_activity,
                session.message_count,
                session.reset_policy,
                session.metadata_json,
                session.input_tokens,
                session.output_tokens,
                session.estimated_cost_usd,
                session.status,
                iso_to_ms(&session.created_at),
                iso_to_ms(&session.last_activity),
            ],
        )
        .map_err(sql_err)?;
        let record = insert_mission_outbox(&tx, request)?;
        tx.commit().map_err(sql_err)?;
        Ok(record)
    }

    /// Permanently remove a session and all its memory associations.
    pub fn delete_session(&self, session_id: &str) -> Result<()> {
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(sql_err)?;
        // FK ON DELETE CASCADE handles cleanup; manual delete is belt-and-suspenders
        tx.execute(
            "DELETE FROM session_memories WHERE session_id = ?1",
            params![session_id],
        )
        .map_err(sql_err)?;
        tx.execute(
            "DELETE FROM sessions WHERE session_id = ?1",
            params![session_id],
        )
        .map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        Ok(())
    }

    /// Atomically delete the Session authority record and enqueue a Mission
    /// close intent. The outbox deliberately has no foreign key to `sessions`:
    /// deleting the record must not erase the close command before Runtime has
    /// materialized it.
    pub fn delete_session_with_mission_outbox(
        &self,
        request: &SessionMissionOutboxRequest,
    ) -> Result<bool> {
        validate_mission_outbox_request(request)?;
        if request.operation != SessionMissionOutboxOperation::Close {
            return Err(MemoryError::Store(
                "session deletion requires a close mission outbox operation".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let exists = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE session_id = ?1)",
                params![request.session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)?
            != 0;
        if !exists {
            tx.commit().map_err(sql_err)?;
            return Ok(false);
        }
        tx.execute(
            "DELETE FROM session_memories WHERE session_id = ?1",
            params![request.session_id],
        )
        .map_err(sql_err)?;
        tx.execute(
            "DELETE FROM sessions WHERE session_id = ?1",
            params![request.session_id],
        )
        .map_err(sql_err)?;
        insert_mission_outbox(&tx, request)?;
        tx.commit().map_err(sql_err)?;
        Ok(true)
    }

    /// List all session records ordered by `last_activity DESC`.
    pub fn list_sessions(&self) -> Result<Vec<SessionRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT session_id, platform, chat_id, user_id, model,
                          created_at, last_activity, message_count, reset_policy, metadata_json,
                          input_tokens, output_tokens, estimated_cost_usd, status
                   FROM sessions ORDER BY last_activity DESC",
            )
            .map_err(sql_err)?;
        let rows = stmt.query_map([], row_to_record).map_err(sql_err)?;
        let mut records = Vec::new();
        for r in rows {
            records.push(r.map_err(sql_err)?);
        }
        Ok(records)
    }

    /// List a filtered, sorted page of sessions directly in SQLite.
    ///
    /// This is the API-facing path for large workspaces. It avoids loading all
    /// sessions into memory before filtering and paginating.
    pub fn list_sessions_page(&self, opts: &SessionListOptions<'_>) -> Result<SessionListPage> {
        let conn = self.conn()?;
        let limit = opts.limit.max(1).min(500);
        let offset = opts.offset;
        let (where_sql, mut values) = session_list_where_clause(opts);

        let count_sql = format!("SELECT COUNT(*) FROM sessions{where_sql}");
        let total: i64 = conn
            .query_row(&count_sql, params_from_iter(values.iter()), |row| {
                row.get(0)
            })
            .map_err(sql_err)?;

        let sort_expr = session_sort_expression(opts.sort);
        let sort_order = session_sort_order(opts.order);
        let page_sql = format!(
            r"SELECT session_id, platform, chat_id, user_id, model,
                      created_at, last_activity, message_count, reset_policy, metadata_json,
                      input_tokens, output_tokens, estimated_cost_usd, status
                 FROM sessions{where_sql}
                ORDER BY {sort_expr} {sort_order}, session_id ASC
                LIMIT ? OFFSET ?"
        );
        values.push(Value::Integer(limit as i64));
        values.push(Value::Integer(offset as i64));

        let mut stmt = conn.prepare(&page_sql).map_err(sql_err)?;
        let rows = stmt
            .query_map(params_from_iter(values.iter()), row_to_record)
            .map_err(sql_err)?;
        let mut records = Vec::new();
        for r in rows {
            records.push(r.map_err(sql_err)?);
        }
        Ok(SessionListPage {
            records,
            total: total as usize,
        })
    }

    /// List all sessions for a given platform, ordered by `last_activity DESC`.
    pub fn list_sessions_by_platform(&self, platform: &str) -> Result<Vec<SessionRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT session_id, platform, chat_id, user_id, model,
                          created_at, last_activity, message_count, reset_policy, metadata_json,
                          input_tokens, output_tokens, estimated_cost_usd, status
                   FROM sessions WHERE platform = ?1 ORDER BY last_activity DESC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![platform], row_to_record)
            .map_err(sql_err)?;
        let mut records = Vec::new();
        for r in rows {
            records.push(r.map_err(sql_err)?);
        }
        Ok(records)
    }

    /// List all sessions bound to a workspace root through metadata_json.
    ///
    /// This is the DB-backed replacement for the deprecated filesystem
    /// `SessionStore` workspace namespace. Records without a
    /// `metadata_json.workspace_root` value are intentionally excluded.
    pub fn list_sessions_by_workspace_root(
        &self,
        workspace_root: &str,
    ) -> Result<Vec<SessionRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT session_id, platform, chat_id, user_id, model,
                          created_at, last_activity, message_count, reset_policy, metadata_json,
                          input_tokens, output_tokens, estimated_cost_usd, status
                   FROM sessions
                  WHERE json_extract(metadata_json, '$.workspace_root') = ?1
                  ORDER BY last_activity DESC, session_id ASC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![workspace_root], row_to_record)
            .map_err(sql_err)?;
        let mut records = Vec::new();
        for r in rows {
            records.push(r.map_err(sql_err)?);
        }
        Ok(records)
    }

    /// Search sessions using FTS5 full-text search.
    ///
    /// Searches across platform, chat_id, user_id, and metadata_json.
    /// Returns results with highlighted snippets from metadata.
    pub fn search_sessions(&self, query: &str, limit: usize) -> Result<Vec<SessionSearchResult>> {
        let conn = self.conn()?;

        // Join sessions with FTS5 and get snippets
        let sql = r"
            SELECT s.session_id, s.platform, s.chat_id, s.user_id,
                   s.created_at, s.last_activity, s.message_count,
                   snippet(sessions_fts, 4, '<mark>', '</mark>', '...', 32) as snippet
            FROM sessions s
            JOIN sessions_fts fts ON s.session_id = fts.session_id
            WHERE sessions_fts MATCH ?1
            ORDER BY rank
            LIMIT ?2
        ";

        let mut stmt = conn.prepare(sql).map_err(sql_err)?;
        let rows = stmt
            .query_map(params![query, limit as i64], |row| {
                Ok(SessionSearchResult {
                    session_id: row.get(0)?,
                    platform: row.get(1)?,
                    chat_id: row.get(2)?,
                    user_id: row.get(3)?,
                    created_at: row.get(4)?,
                    last_activity: row.get(5)?,
                    message_count: row.get(6)?,
                    snippet: row.get(7)?,
                })
            })
            .map_err(sql_err)?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r.map_err(sql_err)?);
        }
        Ok(results)
    }

    /// Search sessions with platform filter.
    pub fn search_sessions_by_platform(
        &self,
        query: &str,
        platform: &str,
        limit: usize,
    ) -> Result<Vec<SessionSearchResult>> {
        let conn = self.conn()?;

        let sql = r"
            SELECT s.session_id, s.platform, s.chat_id, s.user_id,
                   s.created_at, s.last_activity, s.message_count,
                   snippet(sessions_fts, 4, '<mark>', '</mark>', '...', 32) as snippet
            FROM sessions s
            JOIN sessions_fts fts ON s.session_id = fts.session_id
            WHERE sessions_fts MATCH ?1 AND s.platform = ?2
            ORDER BY rank
            LIMIT ?3
        ";

        let mut stmt = conn.prepare(sql).map_err(sql_err)?;
        let rows = stmt
            .query_map(params![query, platform, limit as i64], |row| {
                Ok(SessionSearchResult {
                    session_id: row.get(0)?,
                    platform: row.get(1)?,
                    chat_id: row.get(2)?,
                    user_id: row.get(3)?,
                    created_at: row.get(4)?,
                    last_activity: row.get(5)?,
                    message_count: row.get(6)?,
                    snippet: row.get(7)?,
                })
            })
            .map_err(sql_err)?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r.map_err(sql_err)?);
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Session ↔ Memory associations
    // -----------------------------------------------------------------------

    /// Link a memory ID to a session.
    ///
    /// `INSERT OR IGNORE` makes this idempotent.
    pub fn associate_memory(&self, session_id: &str, memory_id: &str) -> Result<()> {
        let conn = self.conn()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            r"INSERT OR IGNORE INTO session_memories (session_id, memory_id, created_at)
               VALUES (?1, ?2, ?3)",
            params![session_id, memory_id, now],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Return all memory IDs associated with `session_id`.
    pub fn get_session_memories(&self, session_id: &str) -> Result<Vec<String>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT memory_id FROM session_memories WHERE session_id = ?1 ORDER BY created_at",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![session_id], |row| row.get::<_, String>(0))
            .map_err(sql_err)?;
        let mut ids = Vec::new();
        for r in rows {
            ids.push(r.map_err(sql_err)?);
        }
        Ok(ids)
    }

    /// Remove the association between a session and a memory.
    pub fn disassociate_memory(&self, session_id: &str, memory_id: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "DELETE FROM session_memories WHERE session_id = ?1 AND memory_id = ?2",
            params![session_id, memory_id],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Message persistence
    // -----------------------------------------------------------------------

    /// Insert a single message (INSERT OR REPLACE on the (session_id, sequence)
    /// unique constraint).
    pub fn insert_message(&self, msg: &SessionMessage) -> Result<()> {
        let conn = self.conn()?;
        let message_id = if msg.stable_message_id.trim().is_empty() {
            legacy_message_id(&msg.session_id, msg.sequence)
        } else {
            msg.stable_message_id.clone()
        };
        conn.execute(
            r"INSERT INTO messages
                (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                 tool_use_id, tool_name, token_usage_json, created_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
               ON CONFLICT(session_id, sequence) DO UPDATE SET
                   role = excluded.role,
                   content_json = excluded.content_json,
                   blocks_count = excluded.blocks_count,
                   tool_use_id = excluded.tool_use_id,
                   tool_name = excluded.tool_name,
                   token_usage_json = excluded.token_usage_json,
                   created_at_ms = excluded.created_at_ms",
            params![
                message_id,
                msg.session_id,
                msg.sequence as i64,
                msg.role,
                msg.content_json,
                msg.blocks_count as i64,
                msg.tool_use_id,
                msg.tool_name,
                msg.token_usage_json,
                msg.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Materialize one Runtime terminal message with a stable cross-store ID.
    ///
    /// A repeated delivery is idempotent when the immutable message content
    /// matches. Reusing the ID for another message is rejected. Sequence
    /// allocation and insertion happen under one immediate transaction so two
    /// delivery workers cannot create duplicate assistant messages.
    pub fn append_terminal_message_idempotent(
        &self,
        message_id: &str,
        session_id: &str,
        content_json: &str,
        token_usage_json: Option<&str>,
        created_at_ms: u64,
    ) -> Result<(SessionMessage, bool)> {
        if message_id.trim().is_empty() || session_id.trim().is_empty() {
            return Err(MemoryError::Store(
                "terminal message requires stable message and session IDs".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let existing = tx
            .query_row(
                "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                        tool_use_id, tool_name, token_usage_json, created_at_ms
                 FROM messages WHERE stable_message_id = ?1",
                params![message_id],
                |row| {
                    Ok(SessionMessage {
                        stable_message_id: row.get(0)?,
                        session_id: row.get(1)?,
                        sequence: row.get::<_, i64>(2)? as usize,
                        role: row.get(3)?,
                        content_json: row.get(4)?,
                        blocks_count: row.get::<_, i64>(5)? as usize,
                        tool_use_id: row.get(6)?,
                        tool_name: row.get(7)?,
                        token_usage_json: row.get(8)?,
                        created_at_ms: row.get::<_, i64>(9)? as u64,
                    })
                },
            )
            .optional()
            .map_err(sql_err)?;
        if let Some(mut existing) = existing {
            if existing.session_id != session_id
                || existing.role != "assistant"
                || existing.content_json != content_json
                || matches!(
                    (existing.token_usage_json.as_deref(), token_usage_json),
                    (Some(existing), Some(requested)) if existing != requested
                )
            {
                return Err(MemoryError::Store(format!(
                    "terminal message_id `{message_id}` conflicts with committed content"
                )));
            }
            if existing.token_usage_json.is_none() && token_usage_json.is_some() {
                tx.execute(
                    "UPDATE messages SET token_usage_json=?2 WHERE stable_message_id=?1",
                    params![message_id, token_usage_json],
                )
                .map_err(sql_err)?;
                existing.token_usage_json = token_usage_json.map(ToOwned::to_owned);
                refresh_session_usage_summary_tx(&tx, session_id)?;
            }
            tx.commit().map_err(sql_err)?;
            return Ok((existing, false));
        }
        let sequence = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence), -1) + 1 FROM messages WHERE session_id = ?1",
                params![session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)? as usize;
        tx.execute(
            "INSERT INTO messages
             (stable_message_id, session_id, sequence, role, content_json, blocks_count,
              token_usage_json, created_at_ms)
              VALUES (?1, ?2, ?3, 'assistant', ?4, 1, ?5, ?6)",
            params![
                message_id,
                session_id,
                sequence as i64,
                content_json,
                token_usage_json,
                created_at_ms as i64
            ],
        )
        .map_err(sql_err)?;
        refresh_session_message_summary_tx(&tx, session_id, created_at_ms)?;
        refresh_session_usage_summary_tx(&tx, session_id)?;
        tx.commit().map_err(sql_err)?;
        Ok((
            SessionMessage {
                stable_message_id: message_id.to_string(),
                session_id: session_id.to_string(),
                sequence,
                role: "assistant".to_string(),
                content_json: content_json.to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: token_usage_json.map(ToOwned::to_owned),
                created_at_ms,
            },
            true,
        ))
    }

    /// Atomically append the complete Runtime transcript for one terminal.
    ///
    /// The caller supplies stable IDs for every row and the final row must use
    /// `terminal_message_id`. A retry either observes the complete identical
    /// batch or fails closed; a partially matching batch is never accepted.
    pub fn append_terminal_transcript_idempotent(
        &self,
        terminal_message_id: &str,
        ingress_message_id: &str,
        session_id: &str,
        messages: &[SessionMessage],
        created_at_ms: u64,
    ) -> Result<(Vec<SessionMessage>, bool)> {
        if terminal_message_id.trim().is_empty()
            || ingress_message_id.trim().is_empty()
            || session_id.trim().is_empty()
            || messages.is_empty()
            || messages
                .last()
                .is_none_or(|message| message.stable_message_id != terminal_message_id)
        {
            return Err(MemoryError::Store(
                "terminal transcript requires a non-empty session, terminal ID, and terminal final row"
                    .to_string(),
            ));
        }
        if messages.iter().any(|message| {
            message.stable_message_id.trim().is_empty()
                || message.session_id != session_id
                || message.role.trim().is_empty()
                || serde_json::from_str::<serde_json::Value>(&message.content_json)
                    .ok()
                    .and_then(|value| value.as_array().cloned())
                    .is_none()
        }) {
            return Err(MemoryError::Store(
                "terminal transcript contains an invalid message row".to_string(),
            ));
        }
        let unique_ids = messages
            .iter()
            .map(|message| message.stable_message_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if unique_ids.len() != messages.len() {
            return Err(MemoryError::Store(
                "terminal transcript contains duplicate stable message IDs".to_string(),
            ));
        }

        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let load = |message_id: &str| -> Result<Option<SessionMessage>> {
            tx.query_row(
                "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                        tool_use_id, tool_name, token_usage_json, created_at_ms
                   FROM messages WHERE stable_message_id = ?1",
                params![message_id],
                |row| {
                    Ok(SessionMessage {
                        stable_message_id: row.get(0)?,
                        session_id: row.get(1)?,
                        sequence: row.get::<_, i64>(2)? as usize,
                        role: row.get(3)?,
                        content_json: row.get(4)?,
                        blocks_count: row.get::<_, i64>(5)? as usize,
                        tool_use_id: row.get(6)?,
                        tool_name: row.get(7)?,
                        token_usage_json: row.get(8)?,
                        created_at_ms: row.get::<_, i64>(9)? as u64,
                    })
                },
            )
            .optional()
            .map_err(sql_err)
        };
        let terminal_exists = load(terminal_message_id)?.is_some();
        let mut existing = Vec::with_capacity(messages.len());
        for requested in messages {
            match load(&requested.stable_message_id)? {
                Some(committed) => {
                    if committed.session_id != requested.session_id
                        || committed.role != requested.role
                        || committed.content_json != requested.content_json
                        || committed.blocks_count != requested.blocks_count
                        || committed.tool_use_id != requested.tool_use_id
                        || committed.tool_name != requested.tool_name
                        || committed.token_usage_json != requested.token_usage_json
                    {
                        return Err(MemoryError::Store(format!(
                            "terminal transcript message_id `{}` conflicts with committed content",
                            requested.stable_message_id
                        )));
                    }
                    existing.push(committed);
                }
                None if terminal_exists => {
                    return Err(MemoryError::Store(format!(
                        "terminal transcript `{terminal_message_id}` is partially committed"
                    )));
                }
                None => {}
            }
        }
        if terminal_exists {
            if existing.len() != messages.len() {
                return Err(MemoryError::Store(format!(
                    "terminal transcript `{terminal_message_id}` is partially committed"
                )));
            }
            existing.sort_by_key(|message| message.sequence);
            tx.commit().map_err(sql_err)?;
            return Ok((existing, false));
        }
        if !existing.is_empty() {
            return Err(MemoryError::Store(format!(
                "terminal transcript `{terminal_message_id}` collides with existing intermediate rows"
            )));
        }

        let _ingress_sequence = tx
            .query_row(
                "SELECT sequence FROM messages
                  WHERE stable_message_id=?1 AND session_id=?2 AND role='user'",
                params![ingress_message_id, session_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(sql_err)?
            .ok_or_else(|| {
                MemoryError::Store(format!(
                    "terminal transcript ingress `{ingress_message_id}` is not committed"
                ))
            })?;
        // Durable sequence is an immutable append cursor. A later terminal may
        // causally belong before already accepted queued ingress rows, but
        // renumbering those published rows would make every Surface cursor
        // skip or replay data. Causal ordering is carried in block metadata
        // and reconstructed by Runtime/Surface reducers; physical storage
        // remains append-only.
        let first_sequence = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence), -1) + 1 FROM messages WHERE session_id=?1",
                params![session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)
            .and_then(|sequence| {
                usize::try_from(sequence).map_err(|_| {
                    MemoryError::Store("terminal transcript sequence overflow".to_string())
                })
            })?;
        let mut committed = Vec::with_capacity(messages.len());
        for (index, requested) in messages.iter().enumerate() {
            let mut message = requested.clone();
            message.sequence = first_sequence.saturating_add(index);
            message.created_at_ms = created_at_ms.saturating_add(index as u64);
            tx.execute(
                r"INSERT INTO messages
                    (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                     tool_use_id, tool_name, token_usage_json, created_at_ms)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    message.stable_message_id,
                    message.session_id,
                    message.sequence as i64,
                    message.role,
                    message.content_json,
                    message.blocks_count as i64,
                    message.tool_use_id,
                    message.tool_name,
                    message.token_usage_json,
                    message.created_at_ms as i64,
                ],
            )
            .map_err(sql_err)?;
            committed.push(message);
        }
        let last_created_at = committed
            .last()
            .map_or(created_at_ms, |message| message.created_at_ms);
        refresh_session_message_summary_tx(&tx, session_id, last_created_at)?;
        refresh_session_usage_summary_tx(&tx, session_id)?;
        tx.commit().map_err(sql_err)?;
        Ok((committed, true))
    }

    /// Insert multiple messages in a single transaction.
    pub fn insert_messages_batch(&self, messages: &[SessionMessage]) -> Result<()> {
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(sql_err)?;
        {
            let mut stmt = tx
                .prepare(
                    r"INSERT INTO messages
                       (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                        tool_use_id, tool_name, token_usage_json, created_at_ms)
                      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                      ON CONFLICT(session_id, sequence) DO UPDATE SET
                          role = excluded.role,
                          content_json = excluded.content_json,
                          blocks_count = excluded.blocks_count,
                          tool_use_id = excluded.tool_use_id,
                          tool_name = excluded.tool_name,
                          token_usage_json = excluded.token_usage_json,
                          created_at_ms = excluded.created_at_ms",
                )
                .map_err(sql_err)?;
            for msg in messages {
                stmt.execute(params![
                    if msg.stable_message_id.trim().is_empty() {
                        legacy_message_id(&msg.session_id, msg.sequence)
                    } else {
                        msg.stable_message_id.clone()
                    },
                    msg.session_id,
                    msg.sequence as i64,
                    msg.role,
                    msg.content_json,
                    msg.blocks_count as i64,
                    msg.tool_use_id,
                    msg.tool_name,
                    msg.token_usage_json,
                    msg.created_at_ms as i64,
                ])
                .map_err(sql_err)?;
            }
        }
        tx.commit().map_err(sql_err)?;
        Ok(())
    }

    /// Atomically persist a stable user message and its Runtime ingress outbox row.
    ///
    /// Reusing `request_id` with identical identities is idempotent. Reusing it
    /// for another turn/message is rejected rather than silently overwriting data.
    pub fn append_message_with_runtime_outbox(
        &self,
        message: &SessionMessage,
        request: &SessionRuntimeOutboxRequest,
    ) -> Result<SessionRuntimeOutboxRecord> {
        validate_outbox_identity(message, request)?;
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;

        if let Some(existing) = query_outbox(&tx, &request.request_id)? {
            if existing.turn_id == request.turn_id
                && existing.message_id == request.message_id
                && existing.session_id == message.session_id
                && existing.sequence == message.sequence
            {
                tx.commit().map_err(sql_err)?;
                return Ok(existing);
            }
            return Err(MemoryError::Store(format!(
                "outbox request_id `{}` is already bound to another message",
                request.request_id
            )));
        }

        tx.execute(
            r"INSERT INTO messages
                (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                 tool_use_id, tool_name, token_usage_json, created_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                request.message_id,
                message.session_id,
                message.sequence as i64,
                message.role,
                message.content_json,
                message.blocks_count as i64,
                message.tool_use_id,
                message.tool_name,
                message.token_usage_json,
                message.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        refresh_session_message_summary_tx(&tx, &message.session_id, message.created_at_ms)?;
        tx.execute(
            r"INSERT INTO session_runtime_outbox
                (request_id, turn_id, message_id, session_id, sequence, status,
                 attempts, next_attempt_at_ms, revision, created_at_ms, updated_at_ms,
                 runtime_options_json)
               VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6, 0, ?6, ?6, ?7)",
            params![
                request.request_id,
                request.turn_id,
                request.message_id,
                message.session_id,
                message.sequence as i64,
                request.created_at_ms as i64,
                request.runtime_options_json,
            ],
        )
        .map_err(sql_err)?;
        let stored = query_outbox(&tx, &request.request_id)?.ok_or_else(|| {
            MemoryError::Store("outbox insert committed without a readable row".to_string())
        })?;
        tx.commit().map_err(sql_err)?;
        Ok(stored)
    }

    /// Persist an ingress message and Runtime request while allocating the
    /// session-local message sequence inside the same write transaction.
    ///
    /// Surface and Gateway callers must use this entry point for live input;
    /// accepting a caller-computed sequence would create a race between
    /// concurrent surfaces writing to the same session.
    pub fn append_ingress_with_runtime_outbox(
        &self,
        session_id: &str,
        role: &str,
        content_json: Option<&str>,
        created_at_ms: u64,
        request: &SessionRuntimeOutboxRequest,
    ) -> Result<SessionRuntimeOutboxRecord> {
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        if let Some(existing) = query_outbox(&tx, &request.request_id)? {
            if existing.session_id == session_id
                && existing.message_id == request.message_id
                && existing.turn_id == request.turn_id
            {
                tx.commit().map_err(sql_err)?;
                return Ok(existing);
            }
            return Err(MemoryError::Store(format!(
                "outbox request `{}` conflicts with its committed ingress",
                request.request_id
            )));
        }
        let sequence = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence), -1) + 1 FROM messages WHERE session_id = ?1",
                params![session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)? as usize;
        tx.execute(
            r"INSERT INTO messages
                (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                 created_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6)",
            params![
                request.message_id,
                session_id,
                sequence as i64,
                role,
                content_json,
                created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        refresh_session_message_summary_tx(&tx, session_id, created_at_ms)?;
        tx.execute(
            r"INSERT INTO session_runtime_outbox
                (request_id, turn_id, message_id, session_id, sequence, status,
                 attempts, next_attempt_at_ms, revision, created_at_ms, updated_at_ms,
                 runtime_options_json)
               VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6, 0, ?6, ?6, ?7)",
            params![
                request.request_id,
                request.turn_id,
                request.message_id,
                session_id,
                sequence as i64,
                request.created_at_ms as i64,
                request.runtime_options_json,
            ],
        )
        .map_err(sql_err)?;
        let stored = query_outbox(&tx, &request.request_id)?.ok_or_else(|| {
            MemoryError::Store("ingress outbox insert produced no readable row".to_string())
        })?;
        tx.commit().map_err(sql_err)?;
        Ok(stored)
    }

    /// Claim due ingress rows under a renewable lease.
    pub fn claim_session_runtime_outbox(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        if worker_id.trim().is_empty() || lease_ms == 0 || limit == 0 {
            return Err(MemoryError::Store(
                "outbox claim requires worker_id, positive lease and positive limit".to_string(),
            ));
        }
        let claim_expires_at_ms = now_ms.saturating_add(lease_ms);
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let candidates = {
            let mut stmt = tx
                .prepare(
                    r"SELECT request_id, revision, status
                        FROM session_runtime_outbox
                       WHERE ((status IN ('pending', 'retry_scheduled') AND next_attempt_at_ms <= ?1)
                          OR (status = 'claimed' AND claim_expires_at_ms <= ?1))
                       ORDER BY next_attempt_at_ms ASC, sequence ASC, request_id ASC
                       LIMIT ?2",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![now_ms as i64, limit as i64], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)? as u64,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(sql_err)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_err)?
        };

        let mut claimed = Vec::with_capacity(candidates.len());
        for (request_id, revision, from_status) in candidates {
            let changed = tx
                .execute(
                    r"UPDATE session_runtime_outbox
                          SET status = 'claimed',
                              attempts = attempts + 1,
                              claim_owner = ?1,
                              claim_expires_at_ms = ?2,
                              updated_at_ms = ?3,
                              revision = revision + 1
                        WHERE request_id = ?4 AND revision = ?5
                          AND ((status IN ('pending', 'retry_scheduled') AND next_attempt_at_ms <= ?3)
                           OR (status = 'claimed' AND claim_expires_at_ms <= ?3))",
                    params![
                        worker_id,
                        claim_expires_at_ms as i64,
                        now_ms as i64,
                        request_id,
                        revision as i64,
                    ],
                )
                .map_err(sql_err)?;
            if changed == 1 {
                let record = query_outbox(&tx, &request_id)?.ok_or_else(|| {
                    MemoryError::Store(format!("claimed outbox `{request_id}` disappeared"))
                })?;
                append_outbox_history(
                    &tx,
                    &record,
                    if from_status == "claimed" {
                        "reclaim"
                    } else {
                        "claim"
                    },
                    Some(worker_id),
                    None,
                    &from_status,
                    OutboxStatus::Claimed.as_str(),
                    now_ms,
                )?;
                claimed.push(record);
            }
        }
        tx.commit().map_err(sql_err)?;
        Ok(claimed)
    }

    /// Ack a claimed ingress row after Runtime has durably committed it.
    pub fn ack_session_runtime_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        runtime_commit_cursor: u64,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        self.transition_claimed_outbox(
            request_id,
            worker_id,
            expected_revision,
            now_ms,
            |tx, current| {
                tx.execute(
                    r"UPDATE session_runtime_outbox
                          SET status = 'materialized', runtime_commit_cursor = ?1,
                              claim_owner = NULL, claim_expires_at_ms = NULL,
                              failure_class = NULL, last_error = NULL,
                              updated_at_ms = ?2, revision = revision + 1
                        WHERE request_id = ?3 AND status = 'claimed'
                          AND claim_owner = ?4 AND revision = ?5",
                    params![
                        runtime_commit_cursor as i64,
                        now_ms as i64,
                        request_id,
                        worker_id,
                        expected_revision as i64,
                    ],
                )
                .map_err(sql_err)?;
                Ok(("ack", OutboxStatus::Materialized, current.status))
            },
        )
    }

    /// Extend a live ingress claim. The revision is advanced so stale workers
    /// can no longer acknowledge or fail work after ownership has moved.
    pub fn renew_session_runtime_outbox_lease(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        if lease_ms == 0 {
            return Err(MemoryError::Store(
                "outbox lease renewal requires a positive lease".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_outbox(&tx, request_id)?.ok_or_else(|| {
            MemoryError::Store(format!("session runtime outbox `{request_id}` not found"))
        })?;
        if current.status != OutboxStatus::Claimed
            || current.claim_owner.as_deref() != Some(worker_id)
            || current.revision != expected_revision
        {
            return Err(MemoryError::Store(format!(
                "stale outbox lease renewal for `{request_id}`"
            )));
        }
        let expires_at = now_ms.saturating_add(lease_ms);
        let changed = tx
            .execute(
                r"UPDATE session_runtime_outbox
                      SET claim_expires_at_ms = ?1, updated_at_ms = ?2,
                          revision = revision + 1
                    WHERE request_id = ?3 AND status = 'claimed'
                      AND claim_owner = ?4 AND revision = ?5",
                params![
                    expires_at as i64,
                    now_ms as i64,
                    request_id,
                    worker_id,
                    expected_revision as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(MemoryError::Store(format!(
                "outbox lease for `{request_id}` changed during renewal"
            )));
        }
        let renewed = query_outbox(&tx, request_id)?.ok_or_else(|| {
            MemoryError::Store(format!("renewed outbox `{request_id}` disappeared"))
        })?;
        append_outbox_history(
            &tx,
            &renewed,
            "renew_lease",
            Some(worker_id),
            None,
            OutboxStatus::Claimed.as_str(),
            OutboxStatus::Claimed.as_str(),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(renewed)
    }

    /// Classify a failed claim and either schedule retry or block it.
    #[allow(clippy::too_many_arguments)]
    pub fn fail_session_runtime_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        failure_class: OutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        self.transition_claimed_outbox(
            request_id,
            worker_id,
            expected_revision,
            now_ms,
            |tx, current| {
                let retry = failure_class == OutboxFailureClass::Retryable
                    && current.attempts < max_attempts.max(1);
                let next_status = if retry {
                    OutboxStatus::RetryScheduled
                } else {
                    OutboxStatus::BlockedMaterialization
                };
                tx.execute(
                    r"UPDATE session_runtime_outbox
                          SET status = ?1, next_attempt_at_ms = ?2,
                              claim_owner = NULL, claim_expires_at_ms = NULL,
                              failure_class = ?3, last_error = ?4,
                              updated_at_ms = ?5, revision = revision + 1
                        WHERE request_id = ?6 AND status = 'claimed'
                          AND claim_owner = ?7 AND revision = ?8",
                    params![
                        next_status.as_str(),
                        if retry { retry_at_ms } else { now_ms } as i64,
                        failure_class.as_str(),
                        error,
                        now_ms as i64,
                        request_id,
                        worker_id,
                        expected_revision as i64,
                    ],
                )
                .map_err(sql_err)?;
                Ok((
                    if retry { "retry" } else { "block" },
                    next_status,
                    current.status,
                ))
            },
        )
    }

    /// Manually release a blocked row while retaining attempts and audit history.
    pub fn retry_blocked_session_runtime_outbox(
        &self,
        request_id: &str,
        expected_revision: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(MemoryError::Store(
                "manual outbox retry requires actor and reason".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_outbox(&tx, request_id)?
            .ok_or_else(|| MemoryError::Store(format!("outbox `{request_id}` not found")))?;
        if current.status != OutboxStatus::BlockedMaterialization
            || current.revision != expected_revision
        {
            return Err(MemoryError::Store(format!(
                "outbox `{request_id}` is not blocked at revision {expected_revision}"
            )));
        }
        let changed = tx
            .execute(
                r"UPDATE session_runtime_outbox
                      SET status = 'pending', next_attempt_at_ms = ?1,
                          claim_owner = NULL, claim_expires_at_ms = NULL,
                          failure_class = NULL, updated_at_ms = ?1,
                          revision = revision + 1
                    WHERE request_id = ?2 AND status = 'blocked_materialization'
                      AND revision = ?3",
                params![now_ms as i64, request_id, expected_revision as i64],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(MemoryError::Store(format!(
                "outbox `{request_id}` changed during manual retry"
            )));
        }
        let updated = query_outbox(&tx, request_id)?.ok_or_else(|| {
            MemoryError::Store(format!("retried outbox `{request_id}` disappeared"))
        })?;
        append_outbox_history(
            &tx,
            &updated,
            "manual_retry",
            Some(actor),
            Some(reason),
            OutboxStatus::BlockedMaterialization.as_str(),
            OutboxStatus::Pending.as_str(),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(updated)
    }

    pub fn get_session_runtime_outbox(
        &self,
        request_id: &str,
    ) -> Result<Option<SessionRuntimeOutboxRecord>> {
        let conn = self.conn()?;
        query_outbox(&conn, request_id)
    }

    /// Bounded durable ingress history for one Session.  Runtime/Suface
    /// observers use it only to recover execution identity and ingress state;
    /// detailed execution facts remain owned by Runtime's graph projection.
    pub fn session_runtime_outbox_for_session(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT request_id, turn_id, message_id, session_id, sequence, status,
                         runtime_commit_cursor, attempts, next_attempt_at_ms, claim_owner,
                         claim_expires_at_ms, failure_class, last_error, revision,
                         created_at_ms, updated_at_ms, runtime_options_json
                    FROM session_runtime_outbox
                   WHERE session_id = ?1
                   ORDER BY updated_at_ms DESC, sequence DESC, request_id DESC
                   LIMIT ?2",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, limit.clamp(1, 500) as i64],
                row_to_outbox,
            )
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    /// Bounded durable work that may still need observer recovery after a
    /// Gateway restart.  Materialized ingress is terminal for this carrier;
    /// the terminal transcript/outbox remains the source for reply recovery.
    pub fn active_session_runtime_outbox(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT request_id, turn_id, message_id, session_id, sequence, status,
                         runtime_commit_cursor, attempts, next_attempt_at_ms, claim_owner,
                         claim_expires_at_ms, failure_class, last_error, revision,
                         created_at_ms, updated_at_ms, runtime_options_json
                    FROM session_runtime_outbox
                   WHERE status != 'materialized'
                   ORDER BY updated_at_ms DESC, sequence DESC, request_id DESC
                   LIMIT ?1",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![limit.clamp(1, 500) as i64], row_to_outbox)
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    pub fn session_runtime_outbox_health(&self) -> Result<SessionRuntimeOutboxHealth> {
        let conn = self.conn()?;
        let mut health = SessionRuntimeOutboxHealth::default();
        let mut stmt = conn
            .prepare("SELECT status, COUNT(*) FROM session_runtime_outbox GROUP BY status")
            .map_err(sql_err)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(sql_err)?;
        for row in rows {
            let (status, count) = row.map_err(sql_err)?;
            let count = count as usize;
            match OutboxStatus::parse(&status).map_err(sql_err)? {
                OutboxStatus::Pending => health.pending = count,
                OutboxStatus::Claimed => health.claimed = count,
                OutboxStatus::RetryScheduled => health.retry_scheduled = count,
                OutboxStatus::Materialized => health.materialized = count,
                OutboxStatus::BlockedMaterialization => health.blocked = count,
            }
        }
        Ok(health)
    }

    /// Return blocked ingress rows for operational inspection. The bounded
    /// result is ordered deterministically so operators can retry the oldest
    /// poison item first.
    pub fn blocked_session_runtime_outbox(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT request_id, turn_id, message_id, session_id, sequence, status,
                         runtime_commit_cursor, attempts, next_attempt_at_ms, claim_owner,
                         claim_expires_at_ms, failure_class, last_error, revision,
                         created_at_ms, updated_at_ms, runtime_options_json
                    FROM session_runtime_outbox
                   WHERE status = 'blocked_materialization'
                   ORDER BY updated_at_ms ASC, sequence ASC, request_id ASC
                   LIMIT ?1",
            )
            .map_err(sql_err)?;
        let records = stmt
            .query_map(params![limit.clamp(1, 500) as i64], row_to_outbox)
            .map_err(sql_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_err)?;
        Ok(records)
    }

    /// Claim due Session -> Mission lifecycle intents for one Runtime
    /// workspace. A gateway serving another workspace can never materialize
    /// these rows because the workspace key is part of the claim predicate.
    pub fn claim_session_mission_outbox(
        &self,
        workspace_key: &str,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<SessionMissionOutboxRecord>> {
        if workspace_key.trim().is_empty()
            || worker_id.trim().is_empty()
            || lease_ms == 0
            || limit == 0
        {
            return Err(MemoryError::Store(
                "mission outbox claim requires workspace, worker, positive lease and positive limit"
                    .to_string(),
            ));
        }
        let expires_at = now_ms.saturating_add(lease_ms);
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let candidates = {
            let mut stmt = tx
                .prepare(
                    r"SELECT request_id, revision, status
                        FROM session_mission_outbox
                       WHERE workspace_key = ?1
                         AND ((status IN ('pending', 'retry_scheduled') AND next_attempt_at_ms <= ?2)
                           OR (status = 'claimed' AND claim_expires_at_ms <= ?2))
                       ORDER BY next_attempt_at_ms ASC, created_at_ms ASC, request_id ASC
                       LIMIT ?3",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![workspace_key, now_ms as i64, limit as i64], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)? as u64,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(sql_err)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_err)?
        };
        let mut claimed = Vec::with_capacity(candidates.len());
        for (request_id, revision, from_status) in candidates {
            let changed = tx
                .execute(
                    r"UPDATE session_mission_outbox
                          SET status = 'claimed', attempts = attempts + 1,
                              claim_owner = ?1, claim_expires_at_ms = ?2,
                              updated_at_ms = ?3, revision = revision + 1
                        WHERE request_id = ?4 AND workspace_key = ?5 AND revision = ?6
                          AND ((status IN ('pending', 'retry_scheduled') AND next_attempt_at_ms <= ?3)
                            OR (status = 'claimed' AND claim_expires_at_ms <= ?3))",
                    params![
                        worker_id,
                        expires_at as i64,
                        now_ms as i64,
                        request_id,
                        workspace_key,
                        revision as i64,
                    ],
                )
                .map_err(sql_err)?;
            if changed == 1 {
                let record = query_mission_outbox(&tx, &request_id)?.ok_or_else(|| {
                    MemoryError::Store(format!("claimed mission outbox `{request_id}` disappeared"))
                })?;
                append_mission_outbox_history(
                    &tx,
                    &record,
                    if from_status == "claimed" {
                        "reclaim"
                    } else {
                        "claim"
                    },
                    Some(worker_id),
                    None,
                    &from_status,
                    OutboxStatus::Claimed.as_str(),
                    now_ms,
                )?;
                claimed.push(record);
            }
        }
        tx.commit().map_err(sql_err)?;
        Ok(claimed)
    }

    /// Acknowledge an applied Mission lifecycle intent.
    pub fn ack_session_mission_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> Result<SessionMissionOutboxRecord> {
        self.transition_claimed_mission_outbox(
            request_id,
            worker_id,
            expected_revision,
            now_ms,
            |tx, current| {
                tx.execute(
                    r"UPDATE session_mission_outbox
                          SET status = 'materialized', claim_owner = NULL,
                              claim_expires_at_ms = NULL, failure_class = NULL,
                              last_error = NULL, updated_at_ms = ?1,
                              revision = revision + 1
                        WHERE request_id = ?2 AND status = 'claimed'
                          AND claim_owner = ?3 AND revision = ?4",
                    params![
                        now_ms as i64,
                        request_id,
                        worker_id,
                        expected_revision as i64
                    ],
                )
                .map_err(sql_err)?;
                Ok(("ack", OutboxStatus::Materialized, current.status))
            },
        )
    }

    /// Record a failed Session -> Mission application. Transient Runtime
    /// failures retry with bounded attempts; invalid payloads remain visible
    /// as blocked rows rather than spinning indefinitely.
    #[allow(clippy::too_many_arguments)]
    pub fn fail_session_mission_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        failure_class: OutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> Result<SessionMissionOutboxRecord> {
        self.transition_claimed_mission_outbox(
            request_id,
            worker_id,
            expected_revision,
            now_ms,
            |tx, current| {
                let retry = failure_class == OutboxFailureClass::Retryable
                    && current.attempts < max_attempts.max(1);
                let next = if retry {
                    OutboxStatus::RetryScheduled
                } else {
                    OutboxStatus::BlockedMaterialization
                };
                tx.execute(
                    r"UPDATE session_mission_outbox
                          SET status = ?1, next_attempt_at_ms = ?2,
                              claim_owner = NULL, claim_expires_at_ms = NULL,
                              failure_class = ?3, last_error = ?4,
                              updated_at_ms = ?5, revision = revision + 1
                        WHERE request_id = ?6 AND status = 'claimed'
                          AND claim_owner = ?7 AND revision = ?8",
                    params![
                        next.as_str(),
                        if retry { retry_at_ms } else { now_ms } as i64,
                        failure_class.as_str(),
                        error,
                        now_ms as i64,
                        request_id,
                        worker_id,
                        expected_revision as i64,
                    ],
                )
                .map_err(sql_err)?;
                Ok((if retry { "retry" } else { "block" }, next, current.status))
            },
        )
    }

    pub fn get_session_mission_outbox(
        &self,
        request_id: &str,
    ) -> Result<Option<SessionMissionOutboxRecord>> {
        let conn = self.conn()?;
        query_mission_outbox(&conn, request_id)
    }

    fn transition_claimed_mission_outbox<F>(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
        transition: F,
    ) -> Result<SessionMissionOutboxRecord>
    where
        F: FnOnce(
            &rusqlite::Transaction<'_>,
            &SessionMissionOutboxRecord,
        ) -> Result<(&'static str, OutboxStatus, OutboxStatus)>,
    {
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_mission_outbox(&tx, request_id)?.ok_or_else(|| {
            MemoryError::Store(format!("mission outbox `{request_id}` not found"))
        })?;
        if current.status != OutboxStatus::Claimed
            || current.claim_owner.as_deref() != Some(worker_id)
            || current.revision != expected_revision
        {
            return Err(MemoryError::Store(format!(
                "mission outbox `{request_id}` claim owner/status/revision mismatch"
            )));
        }
        let (action, to_status, from_status) = transition(&tx, &current)?;
        let updated = query_mission_outbox(&tx, request_id)?.ok_or_else(|| {
            MemoryError::Store(format!(
                "transitioned mission outbox `{request_id}` disappeared"
            ))
        })?;
        if updated.revision != expected_revision + 1 || updated.status != to_status {
            return Err(MemoryError::Store(format!(
                "mission outbox `{request_id}` transition lost an optimistic update"
            )));
        }
        append_mission_outbox_history(
            &tx,
            &updated,
            action,
            Some(worker_id),
            updated.last_error.as_deref(),
            from_status.as_str(),
            to_status.as_str(),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(updated)
    }

    fn transition_claimed_outbox<F>(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
        transition: F,
    ) -> Result<SessionRuntimeOutboxRecord>
    where
        F: FnOnce(
            &rusqlite::Transaction<'_>,
            &SessionRuntimeOutboxRecord,
        ) -> Result<(&'static str, OutboxStatus, OutboxStatus)>,
    {
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_outbox(&tx, request_id)?
            .ok_or_else(|| MemoryError::Store(format!("outbox `{request_id}` not found")))?;
        if current.status != OutboxStatus::Claimed
            || current.claim_owner.as_deref() != Some(worker_id)
            || current.revision != expected_revision
        {
            return Err(MemoryError::Store(format!(
                "outbox `{request_id}` claim owner/status/revision mismatch"
            )));
        }
        let (action, to_status, from_status) = transition(&tx, &current)?;
        let updated = query_outbox(&tx, request_id)?.ok_or_else(|| {
            MemoryError::Store(format!("transitioned outbox `{request_id}` disappeared"))
        })?;
        if updated.revision != expected_revision + 1 || updated.status != to_status {
            return Err(MemoryError::Store(format!(
                "outbox `{request_id}` transition lost an optimistic update"
            )));
        }
        append_outbox_history(
            &tx,
            &updated,
            action,
            Some(worker_id),
            updated.last_error.as_deref(),
            from_status.as_str(),
            to_status.as_str(),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(updated)
    }

    /// Retrieve messages for a session with pagination.
    pub fn get_messages(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT stable_message_id, session_id, sequence, role, content_json,
                          blocks_count, tool_use_id, tool_name,
                          token_usage_json, created_at_ms
                   FROM messages
                  WHERE session_id = ?1
                  ORDER BY sequence ASC
                  LIMIT ?2 OFFSET ?3",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, limit as i64, offset as i64],
                row_to_message,
            )
            .map_err(sql_err)?;
        let mut msgs = Vec::new();
        for r in rows {
            msgs.push(r.map_err(sql_err)?);
        }
        Ok(msgs)
    }

    /// Retrieve messages for a session starting at `from_sequence`.
    ///
    /// This keyset-style path is stable for deep history paging because it
    /// uses the `(session_id, sequence)` index instead of scanning through a
    /// large OFFSET window.
    pub fn get_messages_from_sequence(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT stable_message_id, session_id, sequence, role, content_json,
                          blocks_count, tool_use_id, tool_name,
                          token_usage_json, created_at_ms
                   FROM messages
                  WHERE session_id = ?1 AND sequence >= ?2
                  ORDER BY sequence ASC
                  LIMIT ?3",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, from_sequence as i64, limit as i64],
                row_to_message,
            )
            .map_err(sql_err)?;
        let mut msgs = Vec::new();
        for r in rows {
            msgs.push(r.map_err(sql_err)?);
        }
        Ok(msgs)
    }

    /// Retrieve ALL messages for a session (unbounded, no pagination).
    pub fn get_all_messages(&self, session_id: &str) -> Result<Vec<SessionMessage>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                        tool_use_id, tool_name, token_usage_json, created_at_ms
                 FROM messages WHERE session_id = ?1 ORDER BY sequence ASC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![session_id], row_to_message)
            .map_err(sql_err)?;
        let mut messages = Vec::new();
        for row in rows {
            messages.push(row.map_err(sql_err)?);
        }
        if messages.len() > 1000 {
            tracing::warn!(
                session_id,
                count = messages.len(),
                "get_all_messages: large session, consider pagination"
            );
        }
        Ok(messages)
    }

    /// Count the number of messages in a session.
    pub fn get_message_count(&self, session_id: &str) -> Result<usize> {
        let conn = self.conn()?;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        Ok(count as usize)
    }

    /// Delete all messages in a session starting from `from_sequence` (inclusive).
    ///
    /// Returns the number of rows deleted.
    pub fn delete_messages_from(&self, session_id: &str, from_sequence: usize) -> Result<usize> {
        let conn = self.conn()?;
        let removed = conn
            .execute(
                "DELETE FROM messages WHERE session_id = ?1 AND sequence >= ?2",
                params![session_id, from_sequence as i64],
            )
            .map_err(sql_err)?;
        Ok(removed)
    }

    /// Search messages using FTS5 full-text search.
    ///
    /// Optionally filter by `session_id`. Searches across role and
    /// extracted text content from `content_json`.
    pub fn search_messages(
        &self,
        query: &str,
        session_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let conn = self.conn()?;
        if let Some(sid) = session_id {
            let mut stmt = conn
                .prepare(
                    r"SELECT m.stable_message_id, m.session_id, m.sequence, m.role, m.content_json,
                              m.blocks_count, m.tool_use_id, m.tool_name,
                              m.token_usage_json, m.created_at_ms
                       FROM messages m
                       JOIN messages_fts fts ON m.id = fts.rowid
                      WHERE messages_fts MATCH ?1 AND m.session_id = ?2
                      ORDER BY rank
                      LIMIT ?3",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![query, sid, limit as i64], row_to_message)
                .map_err(sql_err)?;
            let mut msgs = Vec::new();
            for r in rows {
                msgs.push(r.map_err(sql_err)?);
            }
            Ok(msgs)
        } else {
            let mut stmt = conn
                .prepare(
                    r"SELECT m.stable_message_id, m.session_id, m.sequence, m.role, m.content_json,
                              m.blocks_count, m.tool_use_id, m.tool_name,
                              m.token_usage_json, m.created_at_ms
                       FROM messages m
                       JOIN messages_fts fts ON m.id = fts.rowid
                      WHERE messages_fts MATCH ?1
                      ORDER BY rank
                      LIMIT ?2",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![query, limit as i64], row_to_message)
                .map_err(sql_err)?;
            let mut msgs = Vec::new();
            for r in rows {
                msgs.push(r.map_err(sql_err)?);
            }
            Ok(msgs)
        }
    }

    /// Search only the supplied session authority set.  Gateway resolves that
    /// set before issuing the query so an unauthorised high-ranked FTS row can
    /// neither displace an authorised result nor be exposed to the caller.
    pub fn search_messages_in_sessions(
        &self,
        query: &str,
        session_ids: &[String],
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        if session_ids.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let conn = self.conn()?;
        let scope_json = serde_json::to_string(session_ids)
            .map_err(|error| MemoryError::Store(format!("encode search session scope: {error}")))?;
        let mut stmt = conn
            .prepare(
                r"SELECT m.stable_message_id, m.session_id, m.sequence, m.role, m.content_json,
                          m.blocks_count, m.tool_use_id, m.tool_name,
                          m.token_usage_json, m.created_at_ms
                     FROM messages m
                     JOIN messages_fts fts ON m.id = fts.rowid
                    WHERE messages_fts MATCH ?1
                      AND m.session_id IN (SELECT value FROM json_each(?2))
                    ORDER BY rank
                    LIMIT ?3",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![query, scope_json, limit as i64], row_to_message)
            .map_err(sql_err)?;
        let mut messages = Vec::new();
        for row in rows {
            messages.push(row.map_err(sql_err)?);
        }
        Ok(messages)
    }

    // -----------------------------------------------------------------------
    // Event log
    // -----------------------------------------------------------------------

    /// Append a mutation event to the session's event log.
    pub fn append_event(&self, event: &SessionEvent) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            r"INSERT INTO session_events
               (session_id, event_type, event_json, sequence, created_at_ms)
              VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                event.session_id,
                event.event_type,
                event.event_json,
                event.sequence as i64,
                event.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Allocate the next session-local sequence and append one event in the
    /// same SQLite transaction. The input sequence is treated as a placeholder.
    pub fn append_event_allocating_sequence(&self, event: &SessionEvent) -> Result<SessionEvent> {
        let mut appended = self.append_events_allocating_sequence(std::slice::from_ref(event))?;
        appended
            .pop()
            .ok_or_else(|| MemoryError::Store("event allocation returned no row".to_string()))
    }

    /// Allocate contiguous sequences and append a same-session event batch in
    /// one `BEGIN IMMEDIATE` transaction.
    pub fn append_events_allocating_sequence(
        &self,
        events: &[SessionEvent],
    ) -> Result<Vec<SessionEvent>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let session_id = events[0].session_id.as_str();
        if session_id.trim().is_empty() || events.iter().any(|event| event.session_id != session_id)
        {
            return Err(MemoryError::Store(
                "atomic session event batch must contain one non-empty session_id".to_string(),
            ));
        }

        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let first_sequence: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;

        let mut appended = Vec::with_capacity(events.len());
        for (offset, event) in events.iter().enumerate() {
            let offset = i64::try_from(offset).map_err(|_| {
                MemoryError::Store("session event batch offset exceeds i64 range".to_string())
            })?;
            let sequence = first_sequence
                .checked_add(offset)
                .ok_or_else(|| MemoryError::Store("session event sequence overflow".to_string()))?;
            let stored_sequence = usize::try_from(sequence).map_err(|_| {
                MemoryError::Store(
                    "allocated session event sequence is negative or too large".to_string(),
                )
            })?;
            let event_json = event_json_with_allocated_sequence(event, stored_sequence)?;
            let created_at_ms = i64::try_from(event.created_at_ms).map_err(|_| {
                MemoryError::Store("session event timestamp exceeds SQLite i64 range".to_string())
            })?;
            tx.execute(
                r"INSERT INTO session_events
                   (session_id, event_type, event_json, sequence, created_at_ms)
                  VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    event.session_id,
                    event.event_type,
                    event_json,
                    sequence,
                    created_at_ms,
                ],
            )
            .map_err(sql_err)?;
            let mut stored = event.clone();
            stored.sequence = stored_sequence;
            stored.event_json = event_json;
            appended.push(stored);
        }
        tx.commit().map_err(sql_err)?;
        Ok(appended)
    }

    /// Atomically append a compaction event bundle unless its semantic
    /// checkpoint has already committed for this session. `None` means a
    /// previous attempt committed the exact checkpoint and the caller must
    /// reuse it instead of emitting duplicate facts/events.
    pub fn append_events_allocating_sequence_if_checkpoint_absent(
        &self,
        events: &[SessionEvent],
        checkpoint_id: &str,
    ) -> Result<Option<Vec<SessionEvent>>> {
        if events.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let session_id = events[0].session_id.as_str();
        if session_id.trim().is_empty() || events.iter().any(|event| event.session_id != session_id)
        {
            return Err(MemoryError::Store(
                "atomic session event batch must contain one non-empty session_id".to_string(),
            ));
        }
        if checkpoint_id.trim().is_empty() {
            return Err(MemoryError::Store(
                "checkpoint-aware event batch requires a non-empty checkpoint_id".to_string(),
            ));
        }

        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let exists: i64 = tx
            .query_row(
                r"SELECT COUNT(*) FROM session_events
                    WHERE session_id = ?1
                      AND event_type = ?2
                      AND json_extract(event_json, '$.kind') = 'memory.semantic_checkpoint.created'
                      AND json_extract(event_json, '$.payload.checkpoint.checkpoint_id') = ?3",
                params![session_id, SESSION_DOMAIN_EVENT_TYPE, checkpoint_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        if exists > 0 {
            tx.commit().map_err(sql_err)?;
            return Ok(None);
        }

        let first_sequence: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        let mut appended = Vec::with_capacity(events.len());
        for (offset, event) in events.iter().enumerate() {
            let offset = i64::try_from(offset).map_err(|_| {
                MemoryError::Store("session event batch offset exceeds i64 range".to_string())
            })?;
            let sequence = first_sequence
                .checked_add(offset)
                .ok_or_else(|| MemoryError::Store("session event sequence overflow".to_string()))?;
            let stored_sequence = usize::try_from(sequence).map_err(|_| {
                MemoryError::Store(
                    "allocated session event sequence is negative or too large".to_string(),
                )
            })?;
            let event_json = event_json_with_allocated_sequence(event, stored_sequence)?;
            let created_at_ms = i64::try_from(event.created_at_ms).map_err(|_| {
                MemoryError::Store("session event timestamp exceeds SQLite i64 range".to_string())
            })?;
            tx.execute(
                r"INSERT INTO session_events
                   (session_id, event_type, event_json, sequence, created_at_ms)
                  VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    event.session_id,
                    event.event_type,
                    event_json,
                    sequence,
                    created_at_ms,
                ],
            )
            .map_err(sql_err)?;
            let mut stored = event.clone();
            stored.sequence = stored_sequence;
            stored.event_json = event_json;
            appended.push(stored);
        }
        tx.commit().map_err(sql_err)?;
        Ok(Some(appended))
    }

    /// Atomically de-duplicate a context envelope and allocate its sequence.
    pub fn append_context_envelope_event_if_absent_allocating_sequence(
        &self,
        event: &SessionEvent,
    ) -> Result<Option<SessionEvent>> {
        if event.event_type != "ContextEnvelope" {
            return self.append_event_allocating_sequence(event).map(Some);
        }
        let envelope_id = serde_json::from_str::<serde_json::Value>(&event.event_json)
            .ok()
            .and_then(|payload| {
                payload
                    .pointer("/envelope/id")
                    .or_else(|| payload.get("envelope_id"))
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            })
            .ok_or_else(|| {
                MemoryError::Store(
                    "ContextEnvelope append requires envelope.id or envelope_id".to_string(),
                )
            })?;

        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let exists: i64 = tx
            .query_row(
                r"SELECT COUNT(*) FROM session_events
                  WHERE event_type = 'ContextEnvelope'
                    AND COALESCE(
                        json_extract(event_json, '$.envelope.id'),
                        json_extract(event_json, '$.envelope_id')
                    ) = ?1",
                params![envelope_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        if exists > 0 {
            tx.commit().map_err(sql_err)?;
            return Ok(None);
        }
        let sequence: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                params![event.session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        tx.execute(
            r"INSERT INTO session_events
               (session_id, event_type, event_json, sequence, created_at_ms)
              VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                event.session_id,
                event.event_type,
                event.event_json,
                sequence,
                event.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        let mut stored = event.clone();
        stored.sequence = sequence as usize;
        Ok(Some(stored))
    }

    /// Append a context envelope event only if this envelope id is not already present.
    ///
    /// Returns `true` when a row was inserted and `false` when an existing
    /// `ContextEnvelope` row with the same `envelope.id` already exists.
    pub fn append_context_envelope_event_if_absent(&self, event: &SessionEvent) -> Result<bool> {
        self.append_context_envelope_event_if_absent_allocating_sequence(event)
            .map(|stored| stored.is_some())
    }

    /// Retrieve events for a session starting from `from_seq` (inclusive).
    /// Ordered by sequence ascending.
    pub fn get_events(&self, session_id: &str, from_seq: usize) -> Result<Vec<SessionEvent>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                 FROM session_events
                 WHERE session_id = ?1 AND sequence >= ?2
                 ORDER BY sequence ASC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![session_id, from_seq as i64], row_to_event)
            .map_err(sql_err)?;
        let mut events = Vec::new();
        for r in rows {
            events.push(r.map_err(sql_err)?);
        }
        Ok(events)
    }

    /// Retrieve at most `limit` events for a session starting from `from_seq`.
    /// Ordered by sequence ascending.
    pub fn get_events_limited(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                 FROM session_events
                 WHERE session_id = ?1 AND sequence >= ?2
                 ORDER BY sequence ASC
                 LIMIT ?3",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, from_seq as i64, limit as i64],
                row_to_event,
            )
            .map_err(sql_err)?;
        let mut events = Vec::new();
        for r in rows {
            events.push(r.map_err(sql_err)?);
        }
        Ok(events)
    }

    /// Retrieve the session-domain timeline while excluding legacy Runtime
    /// lifecycle rows awaiting migration into RuntimeEventStore.
    pub fn get_session_domain_timeline_limited(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                    FROM session_events
                   WHERE session_id = ?1 AND sequence >= ?2
                     AND event_type != 'RuntimeEvent'
                   ORDER BY sequence ASC
                   LIMIT ?3",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, from_seq as i64, limit as i64],
                row_to_event,
            )
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    pub fn count_session_domain_timeline_from(
        &self,
        session_id: &str,
        from_seq: usize,
    ) -> Result<usize> {
        let conn = self.conn()?;
        let count = conn
            .query_row(
                r"SELECT COUNT(*) FROM session_events
                   WHERE session_id = ?1 AND sequence >= ?2
                     AND event_type != 'RuntimeEvent'",
                params![session_id, from_seq as i64],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)?;
        Ok(count as usize)
    }

    /// Retrieve at most `limit` events of one type for a session.
    /// Ordered by sequence ascending.
    pub fn get_events_by_type_limited(
        &self,
        session_id: &str,
        event_type: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                 FROM session_events
                 WHERE session_id = ?1 AND event_type = ?2 AND sequence >= ?3
                 ORDER BY sequence ASC
                 LIMIT ?4",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, event_type, from_seq as i64, limit as i64],
                row_to_event,
            )
            .map_err(sql_err)?;
        let mut events = Vec::new();
        for r in rows {
            events.push(r.map_err(sql_err)?);
        }
        Ok(events)
    }

    /// Count events for a session starting from `from_seq`.
    pub fn count_events_from(&self, session_id: &str, from_seq: usize) -> Result<usize> {
        let conn = self.conn()?;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id = ?1 AND sequence >= ?2",
                params![session_id, from_seq as i64],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        Ok(count as usize)
    }

    /// Count events of one type for a session starting from `from_seq`.
    pub fn count_events_by_type_from(
        &self,
        session_id: &str,
        event_type: &str,
        from_seq: usize,
    ) -> Result<usize> {
        let conn = self.conn()?;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id = ?1 AND event_type = ?2 AND sequence >= ?3",
                params![session_id, event_type, from_seq as i64],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        Ok(count as usize)
    }

    /// Retrieve a context envelope event by its envelope id.
    pub fn get_context_event_by_envelope_id(
        &self,
        envelope_id: &str,
    ) -> Result<Option<SessionEvent>> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT id, session_id, event_type, event_json, sequence, created_at_ms
              FROM session_events
              WHERE event_type = 'ContextEnvelope'
                AND json_extract(event_json, '$.envelope.id') = ?1
              ORDER BY created_at_ms DESC
              LIMIT 1",
            params![envelope_id],
            row_to_event,
        )
        .optional()
        .map_err(sql_err)
    }

    /// Return the next append sequence for a session event.
    pub fn next_event_sequence(&self, session_id: &str) -> Result<usize> {
        let conn = self.conn()?;
        let next: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        Ok(next.max(0) as usize)
    }

    /// Delete all events from `from_sequence` onward in a session.
    pub fn delete_events_from(&self, session_id: &str, from_sequence: usize) -> Result<usize> {
        let conn = self.conn()?;
        let deleted = conn
            .execute(
                "DELETE FROM session_events WHERE session_id = ?1 AND sequence >= ?2",
                params![session_id, from_sequence as i64],
            )
            .map_err(sql_err)?;
        Ok(deleted)
    }

    /// Delete events of one type from `from_sequence` onward in a session.
    pub fn delete_events_by_type_from(
        &self,
        session_id: &str,
        event_type: &str,
        from_sequence: usize,
    ) -> Result<usize> {
        let conn = self.conn()?;
        let deleted = conn
            .execute(
                "DELETE FROM session_events WHERE session_id = ?1 AND event_type = ?2 AND sequence >= ?3",
                params![session_id, event_type, from_sequence as i64],
            )
            .map_err(sql_err)?;
        Ok(deleted)
    }

    /// Save a full-message-list snapshot at a given event index.
    pub fn save_snapshot(&self, snapshot: &SessionSnapshot) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            r"INSERT INTO session_snapshots
               (session_id, event_idx, messages_json, created_at_ms)
              VALUES (?1, ?2, ?3, ?4)",
            params![
                snapshot.session_id,
                snapshot.event_idx as i64,
                snapshot.messages_json,
                snapshot.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Return the most recent snapshot for a session, or `None`.
    pub fn get_latest_snapshot(&self, session_id: &str) -> Result<Option<SessionSnapshot>> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT id, session_id, event_idx, messages_json, created_at_ms
             FROM session_snapshots
             WHERE session_id = ?1
             ORDER BY event_idx DESC
             LIMIT 1",
            params![session_id],
            row_to_snapshot,
        )
        .optional()
        .map_err(sql_err)
    }

    // -----------------------------------------------------------------------
    // Maintenance
    // -----------------------------------------------------------------------

    /// Delete sessions whose `last_activity` is older than `cutoff_iso8601`.
    ///
    /// Returns the number of sessions that were removed.
    pub fn prune_before(&self, cutoff_iso8601: &str) -> Result<usize> {
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(sql_err)?;
        // Remove associated memories first.
        tx.execute(
            r"DELETE FROM session_memories WHERE session_id IN (
                SELECT session_id FROM sessions WHERE last_activity < ?1
              )",
            params![cutoff_iso8601],
        )
        .map_err(sql_err)?;
        let removed = tx
            .execute(
                "DELETE FROM sessions WHERE last_activity < ?1",
                params![cutoff_iso8601],
            )
            .map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        Ok(removed)
    }

    /// Delete sessions whose `last_activity` is older than `cutoff_iso8601`,
    /// cleaning up both the SQLite records and any corresponding JSONL/JSON
    /// files on disk under `sessions_dir`.
    ///
    /// Returns the number of sessions that were removed.
    pub fn prune_with_files(&self, cutoff_iso8601: &str, sessions_dir: &Path) -> Result<usize> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare("SELECT session_id FROM sessions WHERE last_activity < ?1")
            .map_err(sql_err)?;
        let ids: Vec<String> = stmt
            .query_map(params![cutoff_iso8601], |row| row.get::<_, String>(0))
            .map_err(sql_err)?
            .filter_map(|r| r.ok())
            .collect();
        let count = ids.len();
        for id in &ids {
            self.delete_session(id)?;
            for ext in &["jsonl", "json"] {
                let path = sessions_dir.join(format!("{id}.{ext}"));
                let _ = std::fs::remove_file(&path);
                if *ext == "jsonl" {
                    if let Ok(entries) = std::fs::read_dir(sessions_dir) {
                        for entry in entries.flatten() {
                            let name = entry.file_name();
                            let name_str = name.to_string_lossy();
                            if name_str.starts_with(&format!("{id}.rot-"))
                                && name_str.ends_with(".jsonl")
                            {
                                let _ = std::fs::remove_file(entry.path());
                            }
                        }
                    }
                }
            }
        }
        Ok(count)
    }

    /// Mark a session as closed.
    ///
    /// Updates the session's status to `'closed'` and refreshes
    /// `last_activity`.  Messages are preserved for auditing.
    pub fn mark_session_closed(&self, session_id: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE sessions SET status = 'closed', last_activity = ?1 WHERE session_id = ?2",
            params![chrono::Utc::now().to_rfc3339(), session_id],
        )
        .map_err(sql_err)?;
        Ok(())
    }
}

fn legacy_message_id(session_id: &str, sequence: usize) -> String {
    let mut encoded = String::with_capacity(session_id.len() * 2);
    for byte in session_id.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    format!("legacy:{encoded}:{sequence}")
}

fn event_json_with_allocated_sequence(event: &SessionEvent, sequence: usize) -> Result<String> {
    if event.event_type != SESSION_DOMAIN_EVENT_TYPE {
        return Ok(event.event_json.clone());
    }
    let mut payload =
        serde_json::from_str::<serde_json::Value>(&event.event_json).map_err(|error| {
            MemoryError::Store(format!(
                "session domain event JSON must be valid before sequence allocation: {error}"
            ))
        })?;
    let object = payload.as_object_mut().ok_or_else(|| {
        MemoryError::Store("session domain event JSON must be an object".to_string())
    })?;
    object.insert("sequence".to_string(), serde_json::json!(sequence));
    serde_json::to_string(&payload).map_err(|error| {
        MemoryError::Store(format!("session domain event JSON encode failed: {error}"))
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_store() -> (SqliteSessionStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        let store = SqliteSessionStore::open(&path).expect("open session store");
        (store, dir)
    }

    fn make_record(id: &str) -> SessionRecord {
        SessionRecord {
            session_id: id.to_string(),
            platform: "test".to_string(),
            chat_id: "chat-1".to_string(),
            user_id: Some("user-1".to_string()),
            model: None,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            last_activity: "2024-01-01T00:01:00Z".to_string(),
            message_count: 1,
            reset_policy: "None".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        }
    }

    #[test]
    fn test_create_and_get() {
        let (store, _dir) = make_store();
        let rec = make_record("session-001");
        store.create_session(&rec).unwrap();
        let loaded = store.get_session("session-001").unwrap().unwrap();
        assert_eq!(loaded.session_id, "session-001");
        assert_eq!(loaded.platform, "test");
        assert_eq!(loaded.message_count, 1);
    }

    #[test]
    fn create_session_populates_millisecond_timestamps() {
        let (store, _dir) = make_store();
        let rec = make_record("session-ms");
        store.create_session(&rec).unwrap();
        let conn = store.conn().unwrap();
        let (created_at_ms, updated_at_ms): (i64, i64) = conn
            .query_row(
                "SELECT created_at_ms, updated_at_ms FROM sessions WHERE session_id = ?1",
                params!["session-ms"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(created_at_ms, 1_704_067_200_000);
        assert_eq!(updated_at_ms, 1_704_067_260_000);
    }

    #[test]
    fn test_update_session() {
        let (store, _dir) = make_store();
        let mut rec = make_record("session-002");
        store.create_session(&rec).unwrap();
        rec.message_count = 42;
        rec.last_activity = "2024-01-02T00:00:00Z".to_string();
        store.update_session(&rec).unwrap();
        let loaded = store.get_session("session-002").unwrap().unwrap();
        assert_eq!(loaded.message_count, 42);
    }

    #[test]
    fn test_upsert_session() {
        let (store, _dir) = make_store();
        let mut rec = make_record("session-003");
        store.upsert_session(&rec).unwrap();
        rec.message_count = 99;
        store.upsert_session(&rec).unwrap();
        let loaded = store.get_session("session-003").unwrap().unwrap();
        assert_eq!(loaded.message_count, 99);
    }

    #[test]
    fn test_delete_session() {
        let (store, _dir) = make_store();
        let rec = make_record("session-004");
        store.create_session(&rec).unwrap();
        store.delete_session("session-004").unwrap();
        assert!(store.get_session("session-004").unwrap().is_none());
    }

    #[test]
    fn test_list_sessions() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s1")).unwrap();
        store.create_session(&make_record("s2")).unwrap();
        let list = store.list_sessions().unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn scoped_message_search_preserves_authorized_results_when_other_sessions_rank_first() {
        let (store, _dir) = make_store();
        for session_id in ["foreign", "authorized"] {
            store.create_session(&make_record(session_id)).unwrap();
        }
        for (session_id, sequence) in [("foreign", 0), ("authorized", 0)] {
            store
                .insert_message(&SessionMessage {
                    stable_message_id: format!("{session_id}:{sequence}"),
                    session_id: session_id.to_string(),
                    sequence,
                    role: "user".to_string(),
                    content_json: r#"[{"type":"text","text":"tenant ranked search phrase"}]"#
                        .to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: 1,
                })
                .unwrap();
        }

        let results = store
            .search_messages_in_sessions("tenant", &["authorized".to_string()], 1)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].session_id, "authorized");
    }

    #[test]
    fn terminal_transcript_preserves_published_cursor_and_is_idempotent() {
        let (store, _dir) = make_store();
        let session_id = "causal-terminal";
        store.create_session(&make_record(session_id)).unwrap();
        for (sequence, id, text, turn_id) in [
            (0, "user-1", "first", "turn-1"),
            (1, "user-2", "second", "turn-2"),
        ] {
            store
                .insert_message(&SessionMessage {
                    stable_message_id: id.to_string(),
                    session_id: session_id.to_string(),
                    sequence,
                    role: "user".to_string(),
                    content_json: serde_json::json!([{
                        "type": "text",
                        "text": text,
                        "cowd_turn_id": turn_id,
                        "cowd_turn_ingress_message_id": id,
                    }])
                    .to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: sequence as u64 + 1,
                })
                .unwrap();
        }
        let transcript = vec![SessionMessage {
            stable_message_id: "assistant-1".to_string(),
            session_id: session_id.to_string(),
            sequence: usize::MAX,
            role: "assistant".to_string(),
            content_json: serde_json::json!([{
                "type": "text",
                "text": "first answer",
                "cowd_turn_id": "turn-1",
                "cowd_turn_ingress_message_id": "user-1",
            }])
            .to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: Some(
                serde_json::json!({"input_tokens": 3, "output_tokens": 2}).to_string(),
            ),
            created_at_ms: 3,
        }];

        let (committed, inserted) = store
            .append_terminal_transcript_idempotent(
                "assistant-1",
                "user-1",
                session_id,
                &transcript,
                3,
            )
            .unwrap();
        assert!(inserted);
        assert_eq!(committed[0].sequence, 2);
        let physical = store.get_all_messages(session_id).unwrap();
        assert_eq!(
            physical
                .iter()
                .map(|message| (message.stable_message_id.as_str(), message.sequence))
                .collect::<Vec<_>>(),
            vec![("user-1", 0), ("user-2", 1), ("assistant-1", 2)],
            "already-published ingress cursors must never be renumbered"
        );

        let (replayed, inserted) = store
            .append_terminal_transcript_idempotent(
                "assistant-1",
                "user-1",
                session_id,
                &transcript,
                99,
            )
            .unwrap();
        assert!(!inserted);
        assert_eq!(replayed, committed);
        assert_eq!(store.get_all_messages(session_id).unwrap(), physical);
    }

    #[test]
    fn list_sessions_by_workspace_root_filters_and_orders_by_activity() {
        let (store, _dir) = make_store();
        let workspace_a = "/tmp/cowd-workspace-a";
        let workspace_b = "/tmp/cowd-workspace-b";

        let mut older = make_record("workspace-a-older");
        older.last_activity = "2024-01-01T00:00:00Z".to_string();
        older.metadata_json = Some(serde_json::json!({"workspace_root": workspace_a}).to_string());
        store.create_session(&older).unwrap();

        let mut newer = make_record("workspace-a-newer");
        newer.last_activity = "2024-01-02T00:00:00Z".to_string();
        newer.metadata_json = Some(serde_json::json!({"workspace_root": workspace_a}).to_string());
        store.create_session(&newer).unwrap();

        let mut other_workspace = make_record("workspace-b");
        other_workspace.last_activity = "2024-01-03T00:00:00Z".to_string();
        other_workspace.metadata_json =
            Some(serde_json::json!({"workspace_root": workspace_b}).to_string());
        store.create_session(&other_workspace).unwrap();

        let records = store
            .list_sessions_by_workspace_root(workspace_a)
            .expect("workspace sessions should list");

        assert_eq!(
            records
                .iter()
                .map(|record| record.session_id.as_str())
                .collect::<Vec<_>>(),
            vec!["workspace-a-newer", "workspace-a-older"]
        );
    }

    #[test]
    fn list_sessions_page_filters_sorts_and_counts_at_scale() {
        let (store, _dir) = make_store();
        {
            let mut conn = store.conn().unwrap();
            let tx = conn.transaction().unwrap();
            {
                let mut stmt = tx
                    .prepare(
                        r"INSERT INTO sessions
                           (session_id, platform, chat_id, user_id, model,
                            created_at, last_activity, message_count, reset_policy, metadata_json,
                            input_tokens, output_tokens, estimated_cost_usd, status)
                           VALUES (?1, 'api_server', ?1, NULL, ?2, ?3, ?3, ?4, 'none', ?5, 0, 0, 0.0, ?6)",
                    )
                    .unwrap();
                for i in 0..10_000 {
                    let model = if i % 2 == 0 {
                        "claude-sonnet-4-6"
                    } else {
                        "claude-haiku-4-5"
                    };
                    let status = if i % 3 == 0 { "active" } else { "closed" };
                    let ts = format!(
                        "2026-06-04T{:02}:{:02}:{:02}Z",
                        (i / 3600) % 24,
                        (i / 60) % 60,
                        i % 60
                    );
                    let title =
                        serde_json::json!({"title": format!("Perf Session {i:05}")}).to_string();
                    stmt.execute(params![
                        format!("perf-{i:05}"),
                        model,
                        ts,
                        i as i64,
                        title,
                        status
                    ])
                    .unwrap();
                }
            }
            tx.commit().unwrap();
        }

        let page = store
            .list_sessions_page(&SessionListOptions {
                model: Some("claude-sonnet-4-6"),
                status: Some("active"),
                sort: "last_activity",
                order: "desc",
                limit: 7,
                offset: 0,
                ..SessionListOptions::default()
            })
            .unwrap();

        assert_eq!(page.total, 1667);
        assert_eq!(page.records.len(), 7);
        assert!(page
            .records
            .windows(2)
            .all(|pair| pair[0].last_activity >= pair[1].last_activity));
        assert!(page
            .records
            .iter()
            .all(|r| r.model.as_deref() == Some("claude-sonnet-4-6") && r.status == "active"));
    }

    #[test]
    fn list_sessions_page_escapes_like_wildcards() {
        let (store, _dir) = make_store();
        let mut literal = make_record("literal-percent");
        literal.metadata_json = Some(serde_json::json!({"title":"Auth% Literal"}).to_string());
        store.create_session(&literal).unwrap();

        let mut wildcard = make_record("wildcard-match");
        wildcard.metadata_json = Some(serde_json::json!({"title":"Auth Wildcard"}).to_string());
        store.create_session(&wildcard).unwrap();

        let page = store
            .list_sessions_page(&SessionListOptions {
                query: Some("Auth%"),
                limit: 20,
                ..SessionListOptions::default()
            })
            .unwrap();

        assert_eq!(page.total, 1);
        assert_eq!(page.records[0].session_id, "literal-percent");
    }

    #[test]
    fn status_model_recent_session_query_uses_composite_index() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-index")).unwrap();
        let conn = store.conn().unwrap();
        let mut stmt = conn
            .prepare(
                r"EXPLAIN QUERY PLAN
                  SELECT session_id FROM sessions
                  WHERE status = ?1 COLLATE NOCASE AND model = ?2 COLLATE NOCASE
                  ORDER BY last_activity DESC
                  LIMIT 20 OFFSET 0",
            )
            .unwrap();
        let plan: Vec<String> = stmt
            .query_map(params!["active", "claude-sonnet-4-6"], |row| row.get(3))
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        let plan_text = plan.join(" | ");
        assert!(
            plan_text.contains("idx_sessions_status_model_last_activity"),
            "expected composite index in query plan, got: {plan_text}"
        );
    }

    #[test]
    fn get_events_limited_pages_from_sequence_and_counts_total() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-events")).unwrap();
        for i in 0..1000 {
            store
                .append_event(&SessionEvent {
                    session_id: "s-events".to_string(),
                    event_type: "message_appended".to_string(),
                    event_json: serde_json::json!({"sequence": i}).to_string(),
                    sequence: i,
                    created_at_ms: i as u64,
                })
                .unwrap();
        }

        let events = store.get_events_limited("s-events", 990, 5).unwrap();
        assert_eq!(events.len(), 5);
        assert_eq!(events[0].sequence, 990);
        assert_eq!(events[4].sequence, 994);
        assert_eq!(store.count_events_from("s-events", 990).unwrap(), 10);
    }

    #[test]
    fn get_events_by_type_pages_context_envelopes_only() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-context-events"))
            .unwrap();
        for (sequence, event_type) in [
            (0, "TextDelta"),
            (1, "ContextEnvelope"),
            (2, "ToolStart"),
            (3, "ContextEnvelope"),
        ] {
            store
                .append_event(&SessionEvent {
                    session_id: "s-context-events".to_string(),
                    event_type: event_type.to_string(),
                    event_json: serde_json::json!({
                        "envelope_id": format!("env-{sequence}"),
                        "envelope": {"id": format!("env-{sequence}")}
                    })
                    .to_string(),
                    sequence,
                    created_at_ms: sequence as u64,
                })
                .unwrap();
        }

        let events = store
            .get_events_by_type_limited("s-context-events", "ContextEnvelope", 0, 10)
            .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[1].sequence, 3);
        assert_eq!(
            store
                .count_events_by_type_from("s-context-events", "ContextEnvelope", 0)
                .unwrap(),
            2
        );
    }

    #[test]
    fn get_context_event_by_envelope_id_reads_json_payload() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-context-id")).unwrap();
        store
            .append_event(&SessionEvent {
                session_id: "s-context-id".to_string(),
                event_type: "ContextEnvelope".to_string(),
                event_json: serde_json::json!({
                    "envelope_id": "env-target",
                    "envelope": {"id": "env-target", "intent": "ship"}
                })
                .to_string(),
                sequence: 7,
                created_at_ms: 7,
            })
            .unwrap();

        let event = store
            .get_context_event_by_envelope_id("env-target")
            .unwrap()
            .expect("context event");
        assert_eq!(event.session_id, "s-context-id");
        assert_eq!(event.sequence, 7);
        assert!(event.event_json.contains("ship"));
    }

    #[test]
    fn append_context_envelope_event_if_absent_skips_duplicate_envelope_id() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-context-once"))
            .unwrap();
        let first = SessionEvent {
            session_id: "s-context-once".to_string(),
            event_type: "ContextEnvelope".to_string(),
            event_json: serde_json::json!({
                "envelope_id": "env-once",
                "envelope": {"id": "env-once", "intent": "first"}
            })
            .to_string(),
            sequence: 1,
            created_at_ms: 1,
        };
        let duplicate = SessionEvent {
            sequence: 2,
            created_at_ms: 2,
            event_json: serde_json::json!({
                "envelope_id": "env-once",
                "envelope": {"id": "env-once", "intent": "duplicate"}
            })
            .to_string(),
            ..first.clone()
        };

        assert!(store
            .append_context_envelope_event_if_absent(&first)
            .unwrap());
        assert!(!store
            .append_context_envelope_event_if_absent(&duplicate)
            .unwrap());

        let events = store
            .get_events_by_type_limited("s-context-once", "ContextEnvelope", 0, 10)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 0);
        assert!(events[0].event_json.contains("first"));
    }

    #[test]
    fn delete_events_from_removes_tail_only() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-events-delete"))
            .unwrap();
        for i in 0..5 {
            store
                .append_event(&SessionEvent {
                    session_id: "s-events-delete".to_string(),
                    event_type: "message_appended".to_string(),
                    event_json: serde_json::json!({"sequence": i}).to_string(),
                    sequence: i,
                    created_at_ms: i as u64,
                })
                .unwrap();
        }

        assert_eq!(store.delete_events_from("s-events-delete", 3).unwrap(), 2);
        let events = store.get_events("s-events-delete", 0).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].sequence, 0);
        assert_eq!(events[2].sequence, 2);
    }

    #[test]
    fn delete_events_by_type_from_preserves_other_event_types() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-events-delete-type"))
            .unwrap();
        for (sequence, event_type) in [
            (0, "message_appended"),
            (1, "TextDelta"),
            (2, "message_appended"),
            (3, "ToolStart"),
        ] {
            store
                .append_event(&SessionEvent {
                    session_id: "s-events-delete-type".to_string(),
                    event_type: event_type.to_string(),
                    event_json: serde_json::json!({"sequence": sequence}).to_string(),
                    sequence,
                    created_at_ms: sequence as u64,
                })
                .unwrap();
        }

        assert_eq!(
            store
                .delete_events_by_type_from("s-events-delete-type", "message_appended", 0)
                .unwrap(),
            2
        );
        let events = store.get_events("s-events-delete-type", 0).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "TextDelta");
        assert_eq!(events[1].event_type, "ToolStart");
    }

    #[test]
    fn next_event_sequence_uses_max_sequence_plus_one() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-next-event")).unwrap();
        assert_eq!(store.next_event_sequence("s-next-event").unwrap(), 0);

        for sequence in [0, 5, 2] {
            store
                .append_event(&SessionEvent {
                    session_id: "s-next-event".to_string(),
                    event_type: "TextDelta".to_string(),
                    event_json: serde_json::json!({"sequence": sequence}).to_string(),
                    sequence,
                    created_at_ms: sequence as u64,
                })
                .unwrap();
        }

        assert_eq!(store.next_event_sequence("s-next-event").unwrap(), 6);
    }

    #[test]
    fn allocating_sequence_appends_contiguous_batch_atomically() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-atomic-batch"))
            .unwrap();
        let events = ["first", "second", "third"].map(|event_type| SessionEvent {
            session_id: "s-atomic-batch".to_string(),
            event_type: event_type.to_string(),
            event_json: "{}".to_string(),
            sequence: usize::MAX,
            created_at_ms: 1,
        });

        let appended = store
            .append_events_allocating_sequence(&events)
            .expect("atomic batch should append");
        assert_eq!(
            appended
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(store.get_events("s-atomic-batch", 0).unwrap().len(), 3);
    }

    #[test]
    fn allocating_sequence_is_atomic_across_parallel_sqlite_connections() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-parallel-sqlite"))
            .unwrap();
        let store = std::sync::Arc::new(store);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(100));
        let mut workers = Vec::new();
        for index in 0..100usize {
            let store = std::sync::Arc::clone(&store);
            let barrier = std::sync::Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                store
                    .append_event_allocating_sequence(&SessionEvent {
                        session_id: "s-parallel-sqlite".to_string(),
                        event_type: "parallel".to_string(),
                        event_json: format!(r#"{{"index":{index}}}"#),
                        sequence: usize::MAX,
                        created_at_ms: index as u64,
                    })
                    .unwrap()
                    .sequence
            }));
        }
        let mut sequences = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        sequences.sort_unstable();
        assert_eq!(sequences, (0..100).collect::<Vec<_>>());
    }

    #[test]
    fn session_event_sequence_constraint_rejects_duplicate() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-unique-event"))
            .unwrap();
        let event = SessionEvent {
            session_id: "s-unique-event".to_string(),
            event_type: "first".to_string(),
            event_json: "{}".to_string(),
            sequence: 0,
            created_at_ms: 1,
        };
        store.append_event(&event).unwrap();
        let mut duplicate = event;
        duplicate.event_type = "duplicate".to_string();
        assert!(store.append_event(&duplicate).is_err());
        assert_eq!(store.get_events("s-unique-event", 0).unwrap().len(), 1);
    }

    #[test]
    fn allocating_batch_rolls_back_when_runtime_envelope_is_invalid() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-batch-rollback"))
            .unwrap();
        let events = vec![
            SessionEvent {
                session_id: "s-batch-rollback".to_string(),
                event_type: "normal".to_string(),
                event_json: "{}".to_string(),
                sequence: usize::MAX,
                created_at_ms: 1,
            },
            SessionEvent {
                session_id: "s-batch-rollback".to_string(),
                event_type: SESSION_DOMAIN_EVENT_TYPE.to_string(),
                event_json: "not-json".to_string(),
                sequence: usize::MAX,
                created_at_ms: 2,
            },
        ];
        assert!(store.append_events_allocating_sequence(&events).is_err());
        assert!(store.get_events("s-batch-rollback", 0).unwrap().is_empty());
    }

    #[test]
    fn checkpoint_batch_timestamp_overflow_rolls_back_without_partial_event() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-checkpoint-timestamp-overflow"))
            .unwrap();
        let checkpoint_id = "checkpoint-timestamp-overflow";
        let event = SessionEvent {
            session_id: "s-checkpoint-timestamp-overflow".to_string(),
            event_type: SESSION_DOMAIN_EVENT_TYPE.to_string(),
            event_json: serde_json::json!({
                "kind": "memory.semantic_checkpoint.created",
                "payload": {"checkpoint": {"checkpoint_id": checkpoint_id}},
            })
            .to_string(),
            sequence: usize::MAX,
            created_at_ms: u64::MAX,
        };

        assert!(store
            .append_events_allocating_sequence_if_checkpoint_absent(&[event], checkpoint_id)
            .is_err());
        assert!(store
            .get_events("s-checkpoint-timestamp-overflow", 0)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn event_page_query_uses_session_sequence_index() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-event-index")).unwrap();
        let conn = store.conn().unwrap();
        let mut stmt = conn
            .prepare(
                r"EXPLAIN QUERY PLAN
                  SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                  FROM session_events
                  WHERE session_id = ?1 AND sequence >= ?2
                  ORDER BY sequence ASC
                  LIMIT ?3",
            )
            .unwrap();
        let plan: Vec<String> = stmt
            .query_map(params!["s-event-index", 100_i64, 20_i64], |row| row.get(3))
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        let plan_text = plan.join(" | ");
        assert!(
            plan_text.contains("idx_session_events_session_seq")
                || plan_text.contains("uq_session_events_session_sequence"),
            "expected event sequence index in query plan, got: {plan_text}"
        );
    }

    #[test]
    fn event_type_page_query_uses_session_type_sequence_index() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-context-event-index"))
            .unwrap();
        let conn = store.conn().unwrap();
        let mut stmt = conn
            .prepare(
                r"EXPLAIN QUERY PLAN
                  SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                  FROM session_events
                  WHERE session_id = ?1 AND event_type = ?2 AND sequence >= ?3
                  ORDER BY sequence ASC
                  LIMIT ?4",
            )
            .unwrap();
        let plan: Vec<String> = stmt
            .query_map(
                params!["s-context-event-index", "ContextEnvelope", 100_i64, 20_i64],
                |row| row.get(3),
            )
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        let plan_text = plan.join(" | ");
        assert!(
            plan_text.contains("idx_session_events_session_type_seq"),
            "expected context event type index in query plan, got: {plan_text}"
        );
    }

    #[test]
    fn context_envelope_lookup_uses_envelope_id_index() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-context-envelope-index"))
            .unwrap();
        let conn = store.conn().unwrap();
        let mut stmt = conn
            .prepare(
                r"EXPLAIN QUERY PLAN
                  SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                  FROM session_events
                  WHERE event_type = 'ContextEnvelope'
                    AND json_extract(event_json, '$.envelope.id') = ?1
                  ORDER BY created_at_ms DESC
                  LIMIT 1",
            )
            .unwrap();
        let plan: Vec<String> = stmt
            .query_map(params!["env-indexed"], |row| row.get(3))
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        let plan_text = plan.join(" | ");
        assert!(
            plan_text.contains("idx_session_events_context_envelope_id"),
            "expected context envelope id index in query plan, got: {plan_text}"
        );
    }

    #[test]
    fn get_messages_from_sequence_pages_100k_history() {
        let (store, _dir) = make_store();
        let mut record = make_record("s-100k");
        record.message_count = 100_000;
        store.create_session(&record).unwrap();
        {
            let mut conn = store.conn().unwrap();
            let tx = conn.transaction().unwrap();
            {
                let mut stmt = tx
                    .prepare(
                        r"INSERT INTO messages
                           (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                            tool_use_id, tool_name, token_usage_json, created_at_ms)
                           VALUES (printf('bulk:%d', ?1), 's-100k', ?1, ?2, ?3, 1, NULL, NULL, NULL, ?4)",
                    )
                    .unwrap();
                for i in 0..100_000 {
                    let role = if i % 2 == 0 { "user" } else { "assistant" };
                    let content =
                        serde_json::json!([{"type":"text","text":format!("message {i}")}])
                            .to_string();
                    stmt.execute(params![i as i64, role, content, i as i64])
                        .unwrap();
                }
            }
            tx.commit().unwrap();
        }

        let page = store
            .get_messages_from_sequence("s-100k", 99_950, 50)
            .unwrap();
        assert_eq!(page.len(), 50);
        assert_eq!(page[0].sequence, 99_950);
        assert_eq!(page[49].sequence, 99_999);
    }

    #[test]
    fn message_sequence_page_query_uses_session_sequence_index() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-message-index"))
            .unwrap();
        let conn = store.conn().unwrap();
        let mut stmt = conn
            .prepare(
                r"EXPLAIN QUERY PLAN
                  SELECT session_id, sequence, role, content_json,
                         blocks_count, tool_use_id, tool_name,
                         token_usage_json, created_at_ms
                  FROM messages
                  WHERE session_id = ?1 AND sequence >= ?2
                  ORDER BY sequence ASC
                  LIMIT ?3",
            )
            .unwrap();
        let plan: Vec<String> = stmt
            .query_map(params!["s-message-index", 99_950_i64, 50_i64], |row| {
                row.get(3)
            })
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        let plan_text = plan.join(" | ");
        assert!(
            plan_text.contains("idx_messages_session_seq"),
            "expected message sequence index in query plan, got: {plan_text}"
        );
    }

    #[test]
    fn test_list_by_platform() {
        let (store, _dir) = make_store();
        let mut rec = make_record("s-tg");
        rec.platform = "telegram".to_string();
        store.create_session(&rec).unwrap();
        store.create_session(&make_record("s-test")).unwrap();
        let tg = store.list_sessions_by_platform("telegram").unwrap();
        assert_eq!(tg.len(), 1);
        assert_eq!(tg[0].session_id, "s-tg");
    }

    #[test]
    fn test_memory_associations() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-mem")).unwrap();
        store.associate_memory("s-mem", "mem-1").unwrap();
        store.associate_memory("s-mem", "mem-2").unwrap();
        // Idempotent
        store.associate_memory("s-mem", "mem-1").unwrap();
        let mems = store.get_session_memories("s-mem").unwrap();
        assert_eq!(mems.len(), 2);
        store.disassociate_memory("s-mem", "mem-1").unwrap();
        let mems = store.get_session_memories("s-mem").unwrap();
        assert_eq!(mems.len(), 1);
        assert_eq!(mems[0], "mem-2");
    }

    #[test]
    fn conn_sets_busy_timeout() {
        let store = SqliteSessionStore::open_in_memory().unwrap();
        let conn = store.conn().unwrap();
        let timeout: i32 = conn
            .pragma_query_value(None, "busy_timeout", |row| row.get(0))
            .unwrap();
        assert!(timeout > 0, "busy_timeout should be > 0, got {}", timeout);
    }

    #[test]
    fn open_migrates_legacy_message_block_schema() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("legacy-sessions.db");
        {
            let conn = Connection::open(&db).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE sessions (
                    session_id TEXT PRIMARY KEY,
                    platform TEXT DEFAULT '',
                    chat_id TEXT DEFAULT '',
                    user_id TEXT DEFAULT '',
                    model TEXT,
                    created_at TEXT NOT NULL DEFAULT '',
                    last_activity TEXT NOT NULL DEFAULT '',
                    message_count INTEGER DEFAULT 0,
                    reset_policy TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT DEFAULT '{}',
                    created_at_ms INTEGER NOT NULL DEFAULT 0,
                    updated_at_ms INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                    sequence INTEGER NOT NULL,
                    role TEXT NOT NULL,
                    usage_input INTEGER DEFAULT 0,
                    usage_output INTEGER DEFAULT 0,
                    created_at_ms INTEGER NOT NULL,
                    UNIQUE(session_id, sequence)
                );
                CREATE TABLE message_blocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                    session_id TEXT NOT NULL,
                    block_order INTEGER NOT NULL,
                    block_type TEXT NOT NULL,
                    text TEXT,
                    signature TEXT,
                    tool_id TEXT,
                    tool_name TEXT,
                    tool_input TEXT,
                    tool_output TEXT,
                    is_error INTEGER DEFAULT 0,
                    created_at_ms INTEGER NOT NULL
                );
                INSERT INTO sessions(session_id, message_count) VALUES ('legacy', 1);
                INSERT INTO messages(session_id, sequence, role, created_at_ms)
                    VALUES ('legacy', 0, 'user', 1);
                INSERT INTO message_blocks(message_id, session_id, block_order, block_type, text, created_at_ms)
                    VALUES (1, 'legacy', 0, 'text', 'resume survives migration', 1);
                "#,
            )
            .unwrap();
        }

        let store = SqliteSessionStore::open(&db).unwrap();
        let messages = store.get_all_messages("legacy").unwrap();
        assert_eq!(messages.len(), 1);
        assert!(messages[0]
            .content_json
            .contains("resume survives migration"));
        store
            .insert_message(&SessionMessage {
                stable_message_id: "legacy:new-write".to_string(),
                session_id: "legacy".to_string(),
                sequence: 1,
                role: "assistant".to_string(),
                content_json: r#"[{"type":"text","text":"new write works"}]"#.to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: 2,
            })
            .unwrap();
        assert_eq!(store.get_message_count("legacy").unwrap(), 2);
    }

    #[test]
    fn open_repairs_legacy_duplicate_event_sequences_without_dropping_events() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("duplicate-events.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                user_id TEXT,
                model TEXT,
                created_at TEXT NOT NULL,
                last_activity TEXT NOT NULL,
                message_count INTEGER NOT NULL DEFAULT 0,
                reset_policy TEXT NOT NULL,
                metadata_json TEXT,
                input_tokens INTEGER NOT NULL DEFAULT 0,
                output_tokens INTEGER NOT NULL DEFAULT 0,
                estimated_cost_usd REAL NOT NULL DEFAULT 0.0,
                status TEXT NOT NULL DEFAULT 'active',
                created_at_ms INTEGER NOT NULL DEFAULT 0,
                updated_at_ms INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE session_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_json TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL
            );
            INSERT INTO sessions(session_id, platform, chat_id, created_at, last_activity, reset_policy)
            VALUES ('duplicate-session', 'test', 'chat', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z', 'None');
            INSERT INTO session_events(session_id, event_type, event_json, sequence, created_at_ms)
            VALUES ('duplicate-session', 'one', '{}', 0, 1),
                   ('duplicate-session', 'two', '{}', 0, 2);
            "#,
        )
        .unwrap();
        drop(conn);

        let store = SqliteSessionStore::open(&path).expect("legacy events are resequenced");
        let events = store.get_events("duplicate-session", 0).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            events
                .iter()
                .map(|event| {
                    serde_json::from_str::<serde_json::Value>(&event.event_json)
                        .ok()
                        .and_then(|value| value["sequence"].as_u64())
                })
                .collect::<Vec<_>>(),
            vec![Some(0), Some(1)]
        );
    }

    #[test]
    fn test_prune_before() {
        let (store, _dir) = make_store();
        let mut old = make_record("old-session");
        old.last_activity = "2020-01-01T00:00:00Z".to_string();
        store.create_session(&old).unwrap();
        store.create_session(&make_record("new-session")).unwrap();
        let removed = store.prune_before("2021-01-01T00:00:00Z").unwrap();
        assert_eq!(removed, 1);
        assert!(store.get_session("old-session").unwrap().is_none());
        assert!(store.get_session("new-session").unwrap().is_some());
    }

    fn outbox_message(session_id: &str) -> SessionMessage {
        SessionMessage {
            stable_message_id: format!("outbox:{session_id}:0"),
            session_id: session_id.to_string(),
            sequence: 0,
            role: "user".to_string(),
            content_json: r#"[{"type":"text","text":"run this"}]"#.to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: 100,
        }
    }

    fn outbox_request() -> SessionRuntimeOutboxRequest {
        SessionRuntimeOutboxRequest {
            request_id: "request-1".to_string(),
            turn_id: "turn-1".to_string(),
            message_id: "message-1".to_string(),
            created_at_ms: 100,
            runtime_options_json: None,
        }
    }

    #[test]
    fn source_message_and_outbox_are_atomic_and_idempotent() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-outbox")).unwrap();
        let message = outbox_message("s-outbox");
        let request = outbox_request();

        let first = store
            .append_message_with_runtime_outbox(&message, &request)
            .unwrap();
        let duplicate = store
            .append_message_with_runtime_outbox(&message, &request)
            .unwrap();
        assert_eq!(first, duplicate);
        assert_eq!(first.status, OutboxStatus::Pending);
        assert_eq!(store.get_message_count("s-outbox").unwrap(), 1);
        assert_eq!(
            store
                .get_session("s-outbox")
                .unwrap()
                .unwrap()
                .message_count,
            1
        );

        let mut conflicting = request;
        conflicting.turn_id = "turn-other".to_string();
        assert!(store
            .append_message_with_runtime_outbox(&message, &conflicting)
            .is_err());
        assert_eq!(store.get_message_count("s-outbox").unwrap(), 1);
    }

    #[test]
    fn runtime_options_remain_opaque_and_durable_with_session_ingress() {
        let (store, _dir) = make_store();
        store
            .create_session(&make_record("s-runtime-options"))
            .unwrap();
        let message = outbox_message("s-runtime-options");
        let mut request = outbox_request();
        request.request_id = "request-runtime-options".to_string();
        request.runtime_options_json = Some(
            r#"{"profile":"surface_quick_reply","pre_messages":[{"role":"user","blocks":[]}]}"#
                .to_string(),
        );

        let first = store
            .append_message_with_runtime_outbox(&message, &request)
            .unwrap();
        assert_eq!(first.runtime_options_json, request.runtime_options_json);
        let reloaded = store
            .get_session_runtime_outbox(&request.request_id)
            .unwrap()
            .expect("outbox record must persist");
        assert_eq!(reloaded.runtime_options_json, request.runtime_options_json);
    }

    #[test]
    fn source_transaction_rolls_back_when_outbox_identity_conflicts() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-rollback")).unwrap();
        let first = outbox_message("s-rollback");
        let request = outbox_request();
        store
            .append_message_with_runtime_outbox(&first, &request)
            .unwrap();

        let mut second = first;
        second.sequence = 1;
        let conflicting = SessionRuntimeOutboxRequest {
            request_id: "request-2".to_string(),
            turn_id: "turn-2".to_string(),
            message_id: "message-1".to_string(),
            created_at_ms: 101,
            runtime_options_json: None,
        };
        assert!(store
            .append_message_with_runtime_outbox(&second, &conflicting)
            .is_err());
        assert_eq!(store.get_message_count("s-rollback").unwrap(), 1);
        assert!(store
            .get_session_runtime_outbox("request-2")
            .unwrap()
            .is_none());
    }

    fn mission_outbox_request(
        session_id: &str,
        operation: SessionMissionOutboxOperation,
    ) -> SessionMissionOutboxRequest {
        SessionMissionOutboxRequest {
            request_id: format!("mission:workspace-a:{:?}:{session_id}", operation),
            session_id: session_id.to_string(),
            title: format!("Session {session_id}"),
            workspace_key: "workspace-a".to_string(),
            operation,
            created_at_ms: 100,
        }
    }

    #[test]
    fn session_and_mission_registration_are_atomic_idempotent_and_workspace_scoped() {
        let (store, _dir) = make_store();
        let record = make_record("s-mission-outbox");
        let request =
            mission_outbox_request("s-mission-outbox", SessionMissionOutboxOperation::Register);

        let first = store
            .upsert_session_with_mission_outbox(&record, &request)
            .unwrap();
        let duplicate = store
            .upsert_session_with_mission_outbox(&record, &request)
            .unwrap();
        assert_eq!(first, duplicate);
        assert!(store.get_session("s-mission-outbox").unwrap().is_some());
        assert!(store
            .claim_session_mission_outbox("workspace-other", "worker", 100, 50, 10)
            .unwrap()
            .is_empty());
        let claimed = store
            .claim_session_mission_outbox("workspace-a", "worker", 100, 50, 10)
            .unwrap();
        assert_eq!(claimed.len(), 1);
        let done = store
            .ack_session_mission_outbox(&request.request_id, "worker", claimed[0].revision, 101)
            .unwrap();
        assert_eq!(done.status, OutboxStatus::Materialized);
    }

    #[test]
    fn mission_registration_ignores_display_title_drift_for_the_same_lifecycle_intent() {
        let (store, _dir) = make_store();
        let record = make_record("s-mission-title");
        let request =
            mission_outbox_request("s-mission-title", SessionMissionOutboxOperation::Register);
        let first = store
            .upsert_session_with_mission_outbox(&record, &request)
            .unwrap();

        let mut hydrated_record = record;
        hydrated_record.metadata_json = Some(r#"{"title":"Runtime hydrated title"}"#.to_string());
        let mut hydrated_request = request;
        hydrated_request.title = "Runtime hydrated title".to_string();
        let duplicate = store
            .upsert_session_with_mission_outbox(&hydrated_record, &hydrated_request)
            .unwrap();

        assert_eq!(first.request_id, duplicate.request_id);
        assert_eq!(first.title, duplicate.title);
    }

    #[test]
    fn session_mission_upsert_preserves_existing_ingress_message_and_runtime_outbox() {
        let (store, _dir) = make_store();
        let record = make_record("s-mission-preserve-ingress");
        let mission = mission_outbox_request(
            "s-mission-preserve-ingress",
            SessionMissionOutboxOperation::Register,
        );
        store
            .upsert_session_with_mission_outbox(&record, &mission)
            .unwrap();
        let ingress = SessionRuntimeOutboxRequest {
            request_id: "ingress-preserve".to_string(),
            turn_id: "turn-preserve".to_string(),
            message_id: "message-preserve".to_string(),
            created_at_ms: 101,
            runtime_options_json: None,
        };
        store
            .append_ingress_with_runtime_outbox(
                &record.session_id,
                "user",
                Some(r#"[{"type":"text","text":"preserve this turn"}]"#),
                101,
                &ingress,
            )
            .unwrap();

        let mut refreshed = record;
        refreshed.metadata_json = Some(r#"{"title":"Refreshed session"}"#.to_string());
        let mut refreshed_mission = mission;
        refreshed_mission.title = "Refreshed session".to_string();
        store
            .upsert_session_with_mission_outbox(&refreshed, &refreshed_mission)
            .unwrap();

        assert_eq!(store.get_message_count(&refreshed.session_id).unwrap(), 1);
        assert!(store
            .get_session_runtime_outbox(&ingress.request_id)
            .unwrap()
            .is_some());
    }

    #[test]
    fn session_delete_queues_close_without_foreign_key_loss() {
        let (store, _dir) = make_store();
        let record = make_record("s-mission-close");
        store
            .upsert_session_with_mission_outbox(
                &record,
                &mission_outbox_request("s-mission-close", SessionMissionOutboxOperation::Register),
            )
            .unwrap();
        let close = mission_outbox_request("s-mission-close", SessionMissionOutboxOperation::Close);
        assert!(store.delete_session_with_mission_outbox(&close).unwrap());
        assert!(store.get_session("s-mission-close").unwrap().is_none());
        let claimed = store
            .claim_session_mission_outbox("workspace-a", "worker", 100, 50, 10)
            .unwrap();
        assert!(claimed.iter().any(|record| {
            record.request_id == close.request_id
                && record.operation == SessionMissionOutboxOperation::Close
        }));
    }

    #[test]
    fn outbox_claim_lease_retry_block_manual_retry_and_ack_are_guarded() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-lifecycle")).unwrap();
        store
            .append_message_with_runtime_outbox(&outbox_message("s-lifecycle"), &outbox_request())
            .unwrap();

        let first = store
            .claim_session_runtime_outbox("worker-a", 100, 50, 10)
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].attempts, 1);
        assert!(store
            .claim_session_runtime_outbox("worker-b", 149, 50, 10)
            .unwrap()
            .is_empty());
        let reclaimed = store
            .claim_session_runtime_outbox("worker-b", 150, 50, 10)
            .unwrap();
        assert_eq!(reclaimed[0].attempts, 2);

        let retry = store
            .fail_session_runtime_outbox(
                "request-1",
                "worker-b",
                reclaimed[0].revision,
                OutboxFailureClass::Retryable,
                "runtime unavailable",
                250,
                3,
                151,
            )
            .unwrap();
        assert_eq!(retry.status, OutboxStatus::RetryScheduled);
        assert!(store
            .claim_session_runtime_outbox("worker-c", 249, 50, 10)
            .unwrap()
            .is_empty());
        let final_claim = store
            .claim_session_runtime_outbox("worker-c", 250, 50, 10)
            .unwrap();
        assert_eq!(final_claim[0].attempts, 3);
        let blocked = store
            .fail_session_runtime_outbox(
                "request-1",
                "worker-c",
                final_claim[0].revision,
                OutboxFailureClass::Retryable,
                "retry exhausted",
                500,
                3,
                251,
            )
            .unwrap();
        assert_eq!(blocked.status, OutboxStatus::BlockedMaterialization);

        let pending = store
            .retry_blocked_session_runtime_outbox(
                "request-1",
                blocked.revision,
                "operator-1",
                "runtime recovered",
                300,
            )
            .unwrap();
        assert_eq!(pending.status, OutboxStatus::Pending);
        assert_eq!(pending.attempts, 3);
        let claimed = store
            .claim_session_runtime_outbox("worker-d", 300, 50, 10)
            .unwrap();
        let done = store
            .ack_session_runtime_outbox("request-1", "worker-d", claimed[0].revision, 42, 301)
            .unwrap();
        assert_eq!(done.status, OutboxStatus::Materialized);
        assert_eq!(done.runtime_commit_cursor, Some(42));
        assert_eq!(
            store.session_runtime_outbox_health().unwrap().materialized,
            1
        );
    }

    #[test]
    fn outbox_lease_renewal_rejects_stale_ack_and_prevents_reclaim() {
        let (store, _dir) = make_store();
        store.create_session(&make_record("s-renew")).unwrap();
        store
            .append_message_with_runtime_outbox(&outbox_message("s-renew"), &outbox_request())
            .unwrap();
        let claimed = store
            .claim_session_runtime_outbox("worker-a", 100, 50, 1)
            .unwrap()
            .remove(0);
        let renewed = store
            .renew_session_runtime_outbox_lease("request-1", "worker-a", claimed.revision, 140, 50)
            .unwrap();
        assert!(store
            .claim_session_runtime_outbox("worker-b", 151, 50, 1)
            .unwrap()
            .is_empty());
        assert!(store
            .ack_session_runtime_outbox("request-1", "worker-a", claimed.revision, 7, 152,)
            .is_err());
        let done = store
            .ack_session_runtime_outbox("request-1", "worker-a", renewed.revision, 7, 153)
            .unwrap();
        assert_eq!(done.status, OutboxStatus::Materialized);
    }

    #[test]
    fn legacy_domain_events_migrate_but_execution_lifecycle_does_not() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("domain-migration.db");
        {
            let store = SqliteSessionStore::open(&path).unwrap();
            store.create_session(&make_record("s-migration")).unwrap();
            for (sequence, scope, kind) in [
                (0, "context", "context.prepared"),
                (1, "agent", "agent.started"),
            ] {
                store
                    .append_event(&SessionEvent {
                        session_id: "s-migration".to_string(),
                        event_type: "RuntimeEvent".to_string(),
                        event_json: serde_json::json!({
                            "event_id": format!("event-{sequence}"),
                            "session_id": "s-migration",
                            "sequence": sequence,
                            "scope": scope,
                            "kind": kind,
                            "refs": [],
                            "payload": {},
                            "created_at_ms": sequence + 1
                        })
                        .to_string(),
                        sequence,
                        created_at_ms: sequence as u64 + 1,
                    })
                    .unwrap();
            }
        }

        let migrated = SqliteSessionStore::open(&path).unwrap();
        assert_eq!(
            migrated
                .count_events_by_type_from("s-migration", SESSION_DOMAIN_EVENT_TYPE, 0)
                .unwrap(),
            1
        );
        assert_eq!(
            migrated
                .count_events_by_type_from("s-migration", "RuntimeEvent", 0)
                .unwrap(),
            1,
            "execution lifecycle remains outside the session-domain protocol"
        );
    }
}
