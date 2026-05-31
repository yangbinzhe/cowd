//! VerbatimSink – zero-loss raw storage layer (mempalace philosophy).
//!
//! Entries stored here are preserved in their exact original form and never
//! pass through the compression pipeline.  The sink is backed by a dedicated
//! `verbatim_entries` table in the same SQLite database used by [`SqliteStore`].
//!
//! # Design
//!
//! The [`VerbatimSink`] is a lightweight handle that opens short-lived
//! connections on each call, sharing the WAL-journaled database file with
//! [`SqliteStore`](super::sqlite::SqliteStore).  This keeps the sink decoupled
//! while still benefiting from `SQLite`'s built-in concurrency via `PRAGMA
//! journal_mode=WAL`.

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::error::MemoryError;

/// Result alias used throughout the verbatim module.
pub type Result<T> = std::result::Result<T, MemoryError>;

// ---------------------------------------------------------------------------
// Internal helpers
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

/// Execute a pragma that may return rows (rusqlite 0.31+ treats this as an error).
fn exec_pragma(conn: &Connection, sql: &str) -> Result<()> {
    match conn.execute(sql, []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::ExecuteReturnedResults) => Ok(()),
        Err(e) => Err(sql_err(e)),
    }
}

// ---------------------------------------------------------------------------
// VerbatimEntry — the unit of storage
// ---------------------------------------------------------------------------

/// A single verbatim entry stored in the raw sink.
///
/// Each entry carries its own identity, the original content, a semantic
/// *source* label, the originating memory layer, and an ISO-8601 timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerbatimEntry {
    /// Unique identifier (typically the UUID of the parent [`MemoryEntry`]).
    pub id: String,
    /// The raw, uncompressed content — exactly as the user or system provided it.
    pub content: String,
    /// Semantic source label (e.g. "UserExplicit", "AutoExtracted", "Import").
    pub source: String,
    /// Originating memory layer (2 = L2, 3 = L3, 4 = L4).
    pub layer: i32,
    /// ISO-8601 timestamp of when the entry was stored.
    pub timestamp: String,
}

// ---------------------------------------------------------------------------
// VerbatimSink
// ---------------------------------------------------------------------------

/// Zero-loss raw storage sink backed by a SQLite database file with an r2d2
/// connection pool.
///
/// # Persistence
///
/// The sink reuses the same `sqlite_path` as [`SqliteStore`]
/// (or `":memory:"` for testing).
#[derive(Debug, Clone)]
pub struct VerbatimSink {
    pool: Pool<SqliteConnectionManager>,
}

impl VerbatimSink {
    /// Create a new sink pointing at the given SQLite database.
    ///
    /// The database must already exist and contain the `verbatim_entries`
    /// table (typically created by [`SqliteStore::open`] during schema init).
    pub fn new(db_path: &str) -> Result<Self> {
        let max_size = if db_path == IN_MEMORY_PATH { 1 } else { 4 };
        let pool = new_pool(db_path, max_size)?;
        Ok(Self { pool })
    }

    /// Get a connection from the pool with `PRAGMA foreign_keys=ON`.
    fn conn(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        let conn = self.pool.get().map_err(|e| MemoryError::Store(e.to_string()))?;
        exec_pragma(&conn, "PRAGMA foreign_keys=ON")?;
        Ok(conn)
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /// Persist a single verbatim entry.
    ///
    /// Uses `INSERT OR REPLACE` so that re-storing an entry with the same
    /// `id` is idempotent.
    pub fn store_raw(&self, entry: &VerbatimEntry) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "INSERT OR REPLACE INTO verbatim_entries (id, content, source, layer, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![entry.id, entry.content, entry.source, entry.layer, entry.timestamp],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Retrieve a verbatim entry by its `id`.
    pub fn retrieve_by_id(&self, id: &str) -> Result<Option<VerbatimEntry>> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT id, content, source, layer, timestamp FROM verbatim_entries WHERE id = ?1",
            params![id],
            |row| {
                Ok(VerbatimEntry {
                    id: row.get(0)?,
                    content: row.get(1)?,
                    source: row.get(2)?,
                    layer: row.get(3)?,
                    timestamp: row.get(4)?,
                })
            },
        )
        .optional()
        .map_err(sql_err)
    }

    /// Search verbatim entries whose content matches the given SQL `LIKE` pattern.
    ///
    /// The pattern should include `%` wildcards (e.g. `"%keyword%"`).
    /// Results are ordered by timestamp descending.
    pub fn search_by_content(&self, pattern: &str) -> Result<Vec<VerbatimEntry>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, content, source, layer, timestamp
                 FROM verbatim_entries
                 WHERE content LIKE ?1
                 ORDER BY timestamp DESC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![pattern], |row| {
                Ok(VerbatimEntry {
                    id: row.get(0)?,
                    content: row.get(1)?,
                    source: row.get(2)?,
                    layer: row.get(3)?,
                    timestamp: row.get(4)?,
                })
            })
            .map_err(sql_err)?;
        let mut entries = Vec::new();
        for r in rows {
            entries.push(r.map_err(sql_err)?);
        }
        Ok(entries)
    }

    /// Search verbatim entries by their *source* label (exact match).
    pub fn search_by_entity(&self, source: &str) -> Result<Vec<VerbatimEntry>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, content, source, layer, timestamp
                 FROM verbatim_entries
                 WHERE source = ?1
                 ORDER BY timestamp DESC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![source], |row| {
                Ok(VerbatimEntry {
                    id: row.get(0)?,
                    content: row.get(1)?,
                    source: row.get(2)?,
                    layer: row.get(3)?,
                    timestamp: row.get(4)?,
                })
            })
            .map_err(sql_err)?;
        let mut entries = Vec::new();
        for r in rows {
            entries.push(r.map_err(sql_err)?);
        }
        Ok(entries)
    }
}
