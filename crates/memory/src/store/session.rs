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
use rusqlite::{params, Connection, OptionalExtension};

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
fn set_conn_pragmas(conn: &Connection) -> Result<()> {
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
        .map_err(sql_err)?;
    conn.query_row("PRAGMA busy_timeout=5000", [], |_| Ok(()))
        .map_err(sql_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Schema DDL
// ---------------------------------------------------------------------------

const SCHEMA_SQL: &str = r"
CREATE TABLE IF NOT EXISTS sessions (
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
    estimated_cost_usd REAL NOT NULL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS session_memories (
    session_id TEXT NOT NULL,
    memory_id  TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (session_id, memory_id)
);

CREATE VIRTUAL TABLE IF NOT EXISTS sessions_fts USING fts5(
    session_id UNINDEXED,
    platform,
    chat_id,
    user_id,
    metadata_json,
    content=sessions,
    content_rowid=rowid
);

CREATE INDEX IF NOT EXISTS idx_sessions_platform      ON sessions(platform);
CREATE INDEX IF NOT EXISTS idx_sessions_last_activity ON sessions(last_activity);

CREATE TABLE IF NOT EXISTS messages (
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
);

CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_messages_session_seq ON messages(session_id, sequence);

CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    session_id UNINDEXED,
    role,
    content_text,
    tool_name,
    content=messages,
    content_rowid=id
);
";

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
    conn.execute_batch(SCHEMA_SQL).map_err(sql_err)?;

    // Create FTS5 triggers for sessions (must be separate from batch)
    let triggers: &[&str] = &[
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
    ];
    for trigger in triggers {
        let _ = conn.execute_batch(trigger); // Ignore if already exists
    }
    // Create FTS5 triggers for messages (must be separate from batch)
    let msg_triggers: &[&str] = &[
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
    ];
    for trigger in msg_triggers {
        let _ = conn.execute_batch(trigger); // Ignore if already exists
    }
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

    fn conn(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        let conn = self.pool.get().map_err(|e| MemoryError::Store(e.to_string()))?;
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
                input_tokens, output_tokens, estimated_cost_usd)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
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
                      input_tokens, output_tokens, estimated_cost_usd
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
               estimated_cost_usd = ?12
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
                input_tokens, output_tokens, estimated_cost_usd)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
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
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Permanently remove a session and all its memory associations.
    pub fn delete_session(&self, session_id: &str) -> Result<()> {
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(sql_err)?;
        // session_memories has no FK cascade so delete manually.
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
                          input_tokens, output_tokens, estimated_cost_usd
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

    /// List all sessions for a given platform, ordered by `last_activity DESC`.
    pub fn list_sessions_by_platform(&self, platform: &str) -> Result<Vec<SessionRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT session_id, platform, chat_id, user_id, model,
                          created_at, last_activity, message_count, reset_policy, metadata_json,
                          input_tokens, output_tokens, estimated_cost_usd
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
        let rows = stmt.query_map(params![session_id], row_to_message).map_err(sql_err)?;
        let mut messages = Vec::new();
        for row in rows {
            messages.push(row.map_err(sql_err)?);
        }
        if messages.len() > 1000 {
            tracing::warn!(session_id, count = messages.len(), "get_all_messages: large session, consider pagination");
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
    pub fn delete_messages_from(
        &self,
        session_id: &str,
        from_sequence: usize,
    ) -> Result<usize> {
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
        assert!(
            timeout > 0,
            "busy_timeout should be > 0, got {}",
            timeout
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
}
