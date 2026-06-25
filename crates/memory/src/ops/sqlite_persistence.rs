//! SQLite-backed persistence store — low-level SQL operations with internal
//! data types.  A newtype wrapper in the `runtime` crate bridges this to
//! [`runtime::persistence::PersistenceProtocol`] so the crate graph stays
//! acyclic (runtime → memory, never memory → runtime).
//!
//! Uses two r2d2 connection pools:
//!   - read_pool  (4 connections) — queries
//!   - write_pool (2 connections) — inserts/updates
//!
//! All blocking DB operations run inside `tokio::task::spawn_blocking`
//! to keep the async runtime responsive.

use std::path::{Path, PathBuf};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection};

// ---------------------------------------------------------------------------
// Internal data types (no dependency on `runtime::session`)
// ---------------------------------------------------------------------------

/// Raw representation of a single content block, stored column-wise in
/// `message_blocks` (zero JSON overhead).
#[derive(Debug, Clone)]
pub struct BlockData {
    pub block_type: String,  // "text" | "thinking" | "tool_use" | "tool_result"
    pub text: Option<String>,
    pub signature: Option<String>,
    pub tool_id: Option<String>,
    pub tool_name: Option<String>,
    pub tool_input: Option<String>,
    pub tool_output: Option<String>,
    pub is_error: bool,
}

/// Raw representation of a single conversation message.
#[derive(Debug, Clone)]
pub struct MessageData {
    pub role: String,
    pub blocks: Vec<BlockData>,
    pub usage_input: i64,
    pub usage_output: i64,
}

/// Raw session metadata record.
#[derive(Debug, Clone)]
pub struct SessionRecordData {
    pub session_id: String,
    pub title: Option<String>,
    pub model: Option<String>,
    pub message_count: usize,
    pub created_at_ms: u64,
    pub last_activity: u64,
}

// ---------------------------------------------------------------------------
// Cleanup configuration / store statistics
// ---------------------------------------------------------------------------

/// Cleanup policy — mirrors `runtime::persistence::CleanupConfig`.
#[derive(Debug, Clone)]
pub struct CleanupConfig {
    pub max_sessions: Option<usize>,
    pub max_days: Option<u32>,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            max_sessions: Some(2000),
            max_days: Some(60),
        }
    }
}

/// Store statistics — mirrors `runtime::persistence::StoreStats`.
#[derive(Debug, Clone, Default)]
pub struct StoreStats {
    pub session_count: usize,
    pub message_count: usize,
    pub db_size_bytes: u64,
}

// ---------------------------------------------------------------------------
// SqlitePersistence
// ---------------------------------------------------------------------------

pub struct SqlitePersistence {
    pub read_pool: Pool<SqliteConnectionManager>,
    pub write_pool: Pool<SqliteConnectionManager>,
    db_path: PathBuf,
    pub cleanup_config: CleanupConfig,
}

impl std::fmt::Debug for SqlitePersistence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlitePersistence")
            .field("db_path", &self.db_path)
            .field("cleanup_config", &self.cleanup_config)
            .finish()
    }
}

impl SqlitePersistence {
    /// Open (or create) a persistent database at `path`.
    pub fn open(
        path: &Path,
        cleanup_config: CleanupConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let read_pool = Pool::builder()
            .max_size(4)
            .build(SqliteConnectionManager::file(path))?;
        let write_pool = Pool::builder()
            .max_size(2)
            .build(SqliteConnectionManager::file(path))?;
        // Init schema on first connection
        {
            let conn = write_pool.get()?;
            init_schema(&conn)?;
        }
        Ok(Self {
            read_pool,
            write_pool,
            db_path: path.to_path_buf(),
            cleanup_config,
        })
    }

    /// Open an in-memory database (useful for testing).
    pub fn open_in_memory(
        cleanup_config: CleanupConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::open(Path::new(":memory:"), cleanup_config)
    }

    /// Return a copy of the database path (for stats).
    pub fn db_path(&self) -> PathBuf {
        self.db_path.clone()
    }

    // ── Session CRUD (raw, used by adapter) ──────────────────────────

    pub fn insert_session(
        conn: &Connection,
        session_id: &str,
        record: &SessionRecordData,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let now = record.last_activity.max(record.created_at_ms) as i64;
        conn.execute(
            "INSERT OR REPLACE INTO sessions (session_id, title, model, message_count, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                session_id,
                record.title,
                record.model,
                record.message_count as i64,
                record.created_at_ms as i64,
                now,
            ],
        )?;
        Ok(())
    }

    pub fn get_session(
        conn: &Connection,
        session_id: &str,
    ) -> Result<Option<SessionRecordData>, Box<dyn std::error::Error + Send + Sync>> {
        let mut stmt = conn.prepare_cached(
            "SELECT session_id, title, model, message_count, created_at_ms, updated_at_ms
             FROM sessions WHERE session_id=?1",
        )?;
        let row = stmt
            .query_row(params![session_id], |row| {
                Ok(SessionRecordData {
                    session_id: row.get(0)?,
                    title: row.get(1)?,
                    model: row.get(2)?,
                    message_count: row.get::<_, i64>(3)? as usize,
                    created_at_ms: row.get::<_, i64>(4)? as u64,
                    last_activity: row.get::<_, i64>(5)? as u64,
                })
            })
            .ok();
        Ok(row)
    }

    pub fn list_sessions(
        conn: &Connection,
    ) -> Result<Vec<SessionRecordData>, Box<dyn std::error::Error + Send + Sync>> {
        let mut stmt = conn.prepare_cached(
            "SELECT session_id, title, model, message_count, created_at_ms, updated_at_ms
             FROM sessions ORDER BY updated_at_ms DESC LIMIT 500",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(SessionRecordData {
                session_id: row.get(0)?,
                title: row.get(1)?,
                model: row.get(2)?,
                message_count: row.get::<_, i64>(3)? as usize,
                created_at_ms: row.get::<_, i64>(4)? as u64,
                last_activity: row.get::<_, i64>(5)? as u64,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    pub fn update_session(
        conn: &Connection,
        session_id: &str,
        record: &SessionRecordData,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        conn.execute(
            "UPDATE sessions SET title=?1, model=?2, message_count=?3, updated_at_ms=?4 WHERE session_id=?5",
            params![
                record.title,
                record.model,
                record.message_count as i64,
                current_time_millis(),
                session_id,
            ],
        )?;
        Ok(())
    }

    pub fn delete_session(
        conn: &Connection,
        session_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        conn.execute("DELETE FROM sessions WHERE session_id=?1", params![session_id])?;
        Ok(())
    }

    // ── Message operations ──────────────────────────────────────────

    /// Insert one message (no transaction — caller wraps if needed).
    /// Returns the new message row id.
    pub fn insert_message(
        conn: &Connection,
        session_id: &str,
        msg: &MessageData,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let now = current_time_millis();

        let mut msg_stmt = conn.prepare_cached(
            "INSERT INTO messages (session_id, sequence, role, usage_input, usage_output, created_at_ms)
             VALUES (?1, COALESCE((SELECT MAX(sequence) FROM messages WHERE session_id=?1), -1) + 1,
                     ?2, ?3, ?4, ?5) RETURNING id",
        )?;
        let msg_id: i64 = msg_stmt.query_row(
            params![session_id, msg.role, msg.usage_input, msg.usage_output, now],
            |row| row.get(0),
        )?;

        let mut block_stmt = conn.prepare_cached(
            "INSERT INTO message_blocks (message_id, session_id, block_order, block_type,
             text, signature, tool_id, tool_name, tool_input, tool_output, is_error, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        )?;

        for (order, block) in msg.blocks.iter().enumerate() {
            block_stmt.execute(params![
                msg_id,
                session_id,
                order as i64,
                block.block_type,
                block.text,
                block.signature,
                block.tool_id,
                block.tool_name,
                block.tool_input,
                block.tool_output,
                block.is_error as i32,
                now,
            ])?;
        }
        Ok(msg_id)
    }

    /// Read all messages for a session, ordered by sequence.
    pub fn get_messages(
        conn: &Connection,
        session_id: &str,
    ) -> Result<Vec<MessageData>, Box<dyn std::error::Error + Send + Sync>> {
        let mut stmt = conn.prepare_cached(
            "SELECT m.id as msg_id, m.sequence, m.role, m.usage_input, m.usage_output,
                    b.block_order, b.block_type, b.text, b.signature,
                    b.tool_id, b.tool_name, b.tool_input, b.tool_output, b.is_error
             FROM messages m
             LEFT JOIN message_blocks b ON b.message_id = m.id
             WHERE m.session_id = ?1
             ORDER BY m.sequence, b.block_order",
        )?;

        use std::collections::BTreeMap;
        let mut msgs: BTreeMap<
            i64,
            (String, i64, i64, Vec<BlockData>),
        > = BTreeMap::new();

        let rows = stmt.query_map(params![session_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,   // msg_id
                row.get::<_, String>(2)?, // role
                row.get::<_, i64>(3)?,   // usage_input
                row.get::<_, i64>(4)?,   // usage_output
                row.get::<_, i64>(5)?,   // block_order
                row.get::<_, String>(6)?, // block_type
                row.get::<_, Option<String>>(7)?, // text
                row.get::<_, Option<String>>(8)?, // signature
                row.get::<_, Option<String>>(9)?, // tool_id
                row.get::<_, Option<String>>(10)?, // tool_name
                row.get::<_, Option<String>>(11)?, // tool_input
                row.get::<_, Option<String>>(12)?, // tool_output
                row.get::<_, i32>(13)?,  // is_error
            ))
        })?;

        for row in rows {
            let (
                msg_id, role, usage_in, usage_out, _block_order,
                block_type, text, sig, tid, tn, ti, to, is_err,
            ) = row?;
            let entry = msgs.entry(msg_id).or_insert_with(|| {
                (role, usage_in, usage_out, Vec::new())
            });
            entry.3.push(BlockData {
                block_type,
                text,
                signature: sig,
                tool_id: tid,
                tool_name: tn,
                tool_input: ti,
                tool_output: to,
                is_error: is_err != 0,
            });
        }

        let mut result = Vec::new();
        for (_msg_id, (role, usage_in, usage_out, blocks)) in msgs {
            result.push(MessageData {
                role,
                blocks,
                usage_input: usage_in,
                usage_output: usage_out,
            });
        }
        Ok(result)
    }

    pub fn get_message_count(
        conn: &Connection,
        session_id: &str,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM messages WHERE session_id=?1",
            params![session_id],
            |r| r.get(0),
        )?;
        Ok(count as usize)
    }

    pub fn delete_messages_from(
        conn: &Connection,
        session_id: &str,
        sequence: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        conn.execute(
            "DELETE FROM messages WHERE session_id=?1 AND sequence >= ?2",
            params![session_id, sequence as i64],
        )?;
        Ok(())
    }

    // ── Search ──────────────────────────────────────────────────────

    pub fn search_sessions(
        conn: &Connection,
        query: &str,
    ) -> Result<Vec<SessionRecordData>, Box<dyn std::error::Error + Send + Sync>> {
        let mut stmt = conn.prepare_cached(
            "SELECT session_id, title, model, message_count, created_at_ms, updated_at_ms
             FROM sessions WHERE title LIKE ?1 OR session_id LIKE ?1
             ORDER BY updated_at_ms DESC LIMIT 50",
        )?;
        let pattern = format!("%{}%", query);
        let rows = stmt.query_map(params![pattern], |row| {
            Ok(SessionRecordData {
                session_id: row.get(0)?,
                title: row.get(1)?,
                model: row.get(2)?,
                message_count: row.get::<_, i64>(3)? as usize,
                created_at_ms: row.get::<_, i64>(4)? as u64,
                last_activity: row.get::<_, i64>(5)? as u64,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    pub fn search_messages_session_ids(
        conn: &Connection,
        query: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let mut stmt = conn.prepare_cached(
            "SELECT DISTINCT m.session_id FROM messages_fts f
             JOIN message_blocks b ON b.id = f.rowid
             JOIN messages m ON m.id = b.message_id
             WHERE messages_fts MATCH ?1",
        )?;
        let ids: Vec<String> = stmt
            .query_map(params![query], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(ids)
    }

    // ── Cleanup ─────────────────────────────────────────────────────

    /// Run cleanup based on the configured policy.
    /// Returns the number of sessions deleted.
    pub fn cleanup_with_config(
        conn: &Connection,
        config: &CleanupConfig,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        if config.max_sessions.is_none() && config.max_days.is_none() {
            return Ok(0);
        }
        // Only count-based
        if config.max_days.is_none() {
            let max = config.max_sessions.unwrap() as i64;
            let count: i64 =
                conn.query_row("SELECT COUNT(*) FROM sessions", [], |r| r.get(0))?;
            if count <= max {
                return Ok(0);
            }
            return Ok(conn.execute(
                "DELETE FROM sessions WHERE session_id NOT IN \
                 (SELECT session_id FROM sessions ORDER BY updated_at_ms DESC LIMIT ?1)",
                params![max],
            )?);
        }
        // Age-based (possibly + count OR)
        let cutoff = current_time_millis() - (config.max_days.unwrap() as i64 * 86_400_000);
        let deleted = if config.max_sessions.is_none() {
            conn.execute("DELETE FROM sessions WHERE updated_at_ms < ?1", params![cutoff])?
        } else {
            conn.execute(
                "DELETE FROM sessions WHERE updated_at_ms < ?1 OR session_id NOT IN \
                 (SELECT session_id FROM sessions ORDER BY updated_at_ms DESC LIMIT ?2)",
                params![cutoff, config.max_sessions.unwrap() as i64],
            )?
        };
        Ok(deleted)
    }

    // ── Flush / Stats ───────────────────────────────────────────────

    pub fn wal_checkpoint(
        conn: &Connection,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        Ok(())
    }

    pub fn compute_stats(
        conn: &Connection,
        db_path: &Path,
    ) -> Result<StoreStats, Box<dyn std::error::Error + Send + Sync>> {
        let session_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM sessions", [], |r| r.get(0))?;
        let message_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM messages", [], |r| r.get(0))?;
        let db_size = std::fs::metadata(db_path).map(|m| m.len()).unwrap_or(0);
        Ok(StoreStats {
            session_count: session_count as usize,
            message_count: message_count as usize,
            db_size_bytes: db_size,
        })
    }
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

fn init_schema(conn: &Connection) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    exec_ddl(conn, "PRAGMA journal_mode=WAL")?;
    exec_ddl(conn, "PRAGMA foreign_keys=ON")?;
    exec_ddl(conn, "PRAGMA busy_timeout=5000")?;

    exec_ddl(conn,
        "CREATE TABLE IF NOT EXISTS sessions (
            session_id    TEXT PRIMARY KEY,
            title         TEXT,
            model         TEXT,
            platform      TEXT DEFAULT '',
            chat_id       TEXT DEFAULT '',
            user_id       TEXT DEFAULT '',
            metadata_json TEXT DEFAULT '{}',
            message_count INTEGER DEFAULT 0,
            status        TEXT DEFAULT 'active',
            reset_policy  TEXT NOT NULL DEFAULT '',
            created_at    TEXT NOT NULL DEFAULT '',
            last_activity TEXT NOT NULL DEFAULT '',
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL
        )",
    )?;
    exec_ddl(conn,
        "CREATE TABLE IF NOT EXISTS messages (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id    TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
            sequence      INTEGER NOT NULL,
            role          TEXT NOT NULL,
            content_json  TEXT NOT NULL DEFAULT '[{\"type\":\"text\",\"text\":\"\"}]',
            blocks_count  INTEGER NOT NULL DEFAULT 1,
            tool_use_id   TEXT,
            tool_name     TEXT,
            token_usage_json TEXT,
            usage_input   INTEGER DEFAULT 0,
            usage_output  INTEGER DEFAULT 0,
            created_at_ms INTEGER NOT NULL,
            UNIQUE(session_id, sequence)
        )",
    )?;
    exec_ddl(conn,
        "CREATE INDEX IF NOT EXISTS idx_messages_session_seq ON messages(session_id, sequence)",
    )?;
    exec_ddl(conn,
        "CREATE TABLE IF NOT EXISTS message_blocks (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id    INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
            session_id    TEXT NOT NULL,
            block_order   INTEGER NOT NULL,
            block_type    TEXT NOT NULL,
            text          TEXT,
            signature     TEXT,
            tool_id       TEXT,
            tool_name     TEXT,
            tool_input    TEXT,
            tool_output   TEXT,
            is_error      INTEGER DEFAULT 0,
            created_at_ms INTEGER NOT NULL
        )",
    )?;
    exec_ddl(conn,
        "CREATE INDEX IF NOT EXISTS idx_blocks_msg ON message_blocks(message_id)",
    )?;
    exec_ddl(conn,
        "CREATE INDEX IF NOT EXISTS idx_blocks_session_order ON message_blocks(session_id, block_order)",
    )?;

    exec_ddl(conn,
        "CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(\
            session_id, role, content_text, tool_name,\
            content=message_blocks, content_rowid=id\
        )",
    )?;

    exec_ddl(conn,
        "CREATE TRIGGER IF NOT EXISTS messages_fts_ai AFTER INSERT ON message_blocks BEGIN
            INSERT INTO messages_fts(rowid, session_id, role, content_text, tool_name)
            VALUES (new.id, new.session_id,
                    (SELECT role FROM messages WHERE id=new.message_id),
                    COALESCE(new.text, new.tool_output, ''),
                    COALESCE(new.tool_name, ''));
        END",
    )?;
    exec_ddl(conn,
        "CREATE TRIGGER IF NOT EXISTS messages_fts_ad AFTER DELETE ON message_blocks BEGIN
            INSERT INTO messages_fts(messages_fts, rowid, session_id, role, content_text, tool_name)
            VALUES ('delete', old.id, old.session_id, '', '', '');
        END",
    )?;
    exec_ddl(conn,
        "CREATE TRIGGER IF NOT EXISTS messages_fts_au AFTER UPDATE ON message_blocks BEGIN
            INSERT INTO messages_fts(messages_fts, rowid, session_id, role, content_text, tool_name)
            VALUES ('delete', old.id, old.session_id, '', '', '');
            INSERT INTO messages_fts(rowid, session_id, role, content_text, tool_name)
            VALUES (new.id, new.session_id,
                    (SELECT role FROM messages WHERE id=new.message_id),
                    COALESCE(new.text, new.tool_output, ''),
                    COALESCE(new.tool_name, ''));
        END",
    )?;
    Ok(())
}

fn exec_ddl(conn: &Connection, sql: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match conn.execute_batch(sql) {
        Ok(()) => Ok(()),
        Err(rusqlite::Error::ExecuteReturnedResults) => {
            let mut stmt = conn.prepare(sql)?;
            let mut rows = stmt.query([])?;
            while rows.next()?.is_some() {}
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn current_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
