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

use chrono::Utc;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};

use crate::{error::MemoryError, store::Result};

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
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
pub struct SessionListPage {
    pub records: Vec<SessionRecord>,
    pub total: usize,
}

/// A single message within a conversation session.
///
/// Each message belongs to a session and is ordered by `sequence`.
/// The `content_json` field stores the message blocks as a JSON array
/// of `ContentBlock` objects (text, tool_use, tool_result, etc.).
#[derive(Debug, Clone)]
pub struct SessionMessage {
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
            status TEXT NOT NULL DEFAULT 'active'
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
        r"CREATE INDEX IF NOT EXISTS idx_sessions_status_model_last_activity
            ON sessions(status COLLATE NOCASE, model COLLATE NOCASE, last_activity DESC)",
        r"CREATE INDEX IF NOT EXISTS idx_sessions_model_last_activity
            ON sessions(model COLLATE NOCASE, last_activity DESC)",
        r"CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions(created_at)",
        r"CREATE INDEX IF NOT EXISTS idx_sessions_message_count ON sessions(message_count)",
        r"CREATE TABLE IF NOT EXISTS messages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
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
            UNIQUE(session_id, sequence)
        )",
        r"CREATE INDEX IF NOT EXISTS idx_messages_session     ON messages(session_id)",
        r"CREATE INDEX IF NOT EXISTS idx_messages_session_seq ON messages(session_id, sequence)",
        r"CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
            session_id UNINDEXED,
            role,
            content_text,
            tool_name,
            content=messages,
            content_rowid=id
        )",
        r"CREATE TRIGGER IF NOT EXISTS messages_fts_ai AFTER INSERT ON messages BEGIN
            INSERT INTO messages_fts(rowid, session_id, role, content_text, tool_name)
            VALUES (new.id, new.session_id, new.role,
                    (SELECT group_concat(json_extract(value,'$.text'),' ') FROM json_each(new.content_json) WHERE json_extract(value,'$.type')='text'),
                    new.tool_name);
        END",
        r"CREATE TRIGGER IF NOT EXISTS messages_fts_ad AFTER DELETE ON messages BEGIN
            INSERT INTO messages_fts(messages_fts, rowid, session_id, role, content_text, tool_name)
            VALUES ('delete', old.rowid, old.session_id, old.role, NULL, old.tool_name);
        END",
        r"CREATE TRIGGER IF NOT EXISTS messages_fts_au AFTER UPDATE ON messages BEGIN
            INSERT INTO messages_fts(messages_fts, rowid, session_id, role, content_text, tool_name)
            VALUES ('delete', old.rowid, old.session_id, old.role, NULL, old.tool_name);
            INSERT INTO messages_fts(rowid, session_id, role, content_text, tool_name)
            VALUES (new.rowid, new.session_id, new.role,
                    (SELECT group_concat(json_extract(value,'$.text'),' ') FROM json_each(new.content_json) WHERE json_extract(value,'$.type')='text'),
                    new.tool_name);
        END",
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

    conn.execute_batch("COMMIT;").map_err(sql_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// SessionRecord
// ---------------------------------------------------------------------------

/// A serialisable snapshot of a single session's metadata.
#[derive(Debug, Clone)]
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
        session_id: row.get(0)?,
        sequence: row.get::<_, i64>(1)? as usize,
        role: row.get(2)?,
        content_json: row.get(3)?,
        blocks_count: row.get::<_, i64>(4)? as usize,
        tool_use_id: row.get(5)?,
        tool_name: row.get(6)?,
        token_usage_json: row.get(7)?,
        created_at_ms: row.get::<_, i64>(8)? as u64,
    })
}

// ---------------------------------------------------------------------------
// SessionEvent / SessionSnapshot
// ---------------------------------------------------------------------------

/// A recorded mutation event for a session, enabling event-sourced
/// reconstruction and time-travel debugging.
///
/// Each event is associated with a monotonically-increasing `sequence`
/// that orders it within the session's event log.
#[derive(Debug, Clone)]
pub struct SessionEvent {
    pub session_id: String,
    pub event_type: String,
    pub event_json: String,
    pub sequence: usize,
    pub created_at_ms: u64,
}

/// A full-message-list snapshot taken at a specific event index, used
/// as a basis for fast replay from that point forward.
#[derive(Debug, Clone)]
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
                input_tokens, output_tokens, estimated_cost_usd, status)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
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
               status = ?13
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
            r"INSERT OR REPLACE INTO sessions
               (session_id, platform, chat_id, user_id, model,
                created_at, last_activity, message_count, reset_policy, metadata_json,
                input_tokens, output_tokens, estimated_cost_usd, status)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
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
            ],
        )
        .map_err(sql_err)?;
        Ok(())
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
        conn.execute(
            r"INSERT OR REPLACE INTO messages
                (session_id, sequence, role, content_json, blocks_count,
                 tool_use_id, tool_name, token_usage_json, created_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
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

    /// Insert multiple messages in a single transaction.
    pub fn insert_messages_batch(&self, messages: &[SessionMessage]) -> Result<()> {
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(sql_err)?;
        {
            let mut stmt = tx
                .prepare(
                    r"INSERT OR REPLACE INTO messages
                       (session_id, sequence, role, content_json, blocks_count,
                        tool_use_id, tool_name, token_usage_json, created_at_ms)
                      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                )
                .map_err(sql_err)?;
            for msg in messages {
                stmt.execute(params![
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
                r"SELECT session_id, sequence, role, content_json,
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
                r"SELECT session_id, sequence, role, content_json,
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
                "SELECT session_id, sequence, role, content_json, blocks_count,
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
                    r"SELECT m.session_id, m.sequence, m.role, m.content_json,
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
                    r"SELECT m.session_id, m.sequence, m.role, m.content_json,
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
        assert!(
            page.records
                .windows(2)
                .all(|pair| pair[0].last_activity >= pair[1].last_activity)
        );
        assert!(
            page.records
                .iter()
                .all(|r| r.model.as_deref() == Some("claude-sonnet-4-6") && r.status == "active")
        );
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
            plan_text.contains("idx_session_events_session_seq"),
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
                           (session_id, sequence, role, content_json, blocks_count,
                            tool_use_id, tool_name, token_usage_json, created_at_ms)
                           VALUES ('s-100k', ?1, ?2, ?3, 1, NULL, NULL, NULL, ?4)",
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
}
