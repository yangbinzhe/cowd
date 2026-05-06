use std::path::Path;

use chrono::Utc;
use rusqlite::{params, Connection};

use crate::error::SessionStoreError;
use crate::schema;

pub struct SessionStore {
    conn: Connection,
}

#[derive(Debug, Clone)]
pub struct CreateSessionOpts {
    pub source: String,
    pub model: Option<String>,
    pub workspace_root: Option<String>,
    pub title: Option<String>,
    pub parent_session_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SessionSummary {
    pub id: String,
    pub source: String,
    pub model: Option<String>,
    pub started_at: f64,
    pub message_count: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub title: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ManagedSession {
    pub id: String,
    pub source: String,
    pub model: Option<String>,
    pub parent_session_id: Option<String>,
    pub workspace_root: Option<String>,
    pub started_at: f64,
    pub ended_at: Option<f64>,
    pub end_reason: Option<String>,
    pub message_count: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub estimated_cost_usd: Option<f64>,
    pub title: Option<String>,
    pub messages: Vec<StoredMessage>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredMessage {
    pub id: i64,
    pub session_id: String,
    pub role: String,
    pub content: Option<String>,
    pub tool_call_id: Option<String>,
    pub tool_calls: Option<String>,
    pub tool_name: Option<String>,
    pub timestamp: f64,
    pub token_count: Option<i64>,
}

fn row_to_summary(row: &rusqlite::Row) -> rusqlite::Result<SessionSummary> {
    Ok(SessionSummary {
        id: row.get(0)?,
        source: row.get(1)?,
        model: row.get(2)?,
        started_at: row.get(3)?,
        message_count: row.get(4)?,
        input_tokens: row.get(5)?,
        output_tokens: row.get(6)?,
        title: row.get(7)?,
    })
}

fn row_to_message(row: &rusqlite::Row) -> rusqlite::Result<StoredMessage> {
    Ok(StoredMessage {
        id: row.get(0)?,
        session_id: row.get(1)?,
        role: row.get(2)?,
        content: row.get(3)?,
        tool_call_id: row.get(4)?,
        tool_calls: row.get(5)?,
        tool_name: row.get(6)?,
        timestamp: row.get(7)?,
        token_count: row.get(8)?,
    })
}

impl SessionStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SessionStoreError> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        schema::ensure_schema(&conn)?;
        Ok(Self { conn })
    }

    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    pub fn create_session(&self, opts: &CreateSessionOpts) -> Result<String, SessionStoreError> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now().timestamp() as f64;
        self.conn.execute(
            "INSERT INTO sessions (id, source, model, workspace_root, title, parent_session_id, started_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![id, opts.source, opts.model, opts.workspace_root, opts.title, opts.parent_session_id, now],
        )?;
        Ok(id)
    }

    pub fn get_session(&self, id: &str, load_messages: bool) -> Result<Option<ManagedSession>, SessionStoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source, model, parent_session_id, workspace_root,
                    started_at, ended_at, end_reason, message_count,
                    input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
                    estimated_cost_usd, title
             FROM sessions WHERE id = ?1"
        )?;
        let session = stmt.query_row(params![id], |row| {
            Ok(ManagedSession {
                id: row.get(0)?, source: row.get(1)?, model: row.get(2)?,
                parent_session_id: row.get(3)?, workspace_root: row.get(4)?,
                started_at: row.get(5)?, ended_at: row.get(6)?, end_reason: row.get(7)?,
                message_count: row.get(8)?, input_tokens: row.get(9)?,
                output_tokens: row.get(10)?, cache_read_tokens: row.get(11)?,
                cache_write_tokens: row.get(12)?, estimated_cost_usd: row.get(13)?,
                title: row.get(14)?, messages: Vec::new(),
            })
        });
        match session {
            Ok(mut s) => {
                if load_messages {
                    s.messages = self.get_messages(id)?;
                }
                Ok(Some(s))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn list_sessions(&self, source: Option<&str>, limit: usize, offset: usize) -> Result<Vec<SessionSummary>, SessionStoreError> {
        let mut sessions = Vec::new();
        let mut stmt = if source.is_some() {
            self.conn.prepare(
                "SELECT id, source, model, started_at, message_count, input_tokens, output_tokens, title
                 FROM sessions WHERE source = ?1 ORDER BY started_at DESC LIMIT ?2 OFFSET ?3"
            )?
        } else {
            self.conn.prepare(
                "SELECT id, source, model, started_at, message_count, input_tokens, output_tokens, title
                 FROM sessions ORDER BY started_at DESC LIMIT ?1 OFFSET ?2"
            )?
        };
        let rows = if let Some(src) = source {
            stmt.query_map(params![src, limit as i64, offset as i64], row_to_summary)?
        } else {
            stmt.query_map(params![limit as i64, offset as i64], row_to_summary)?
        };
        for row in rows {
            sessions.push(row?);
        }
        Ok(sessions)
    }

    pub fn delete_session(&self, id: &str) -> Result<bool, SessionStoreError> {
        let affected = self.conn.execute("DELETE FROM sessions WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }

    pub fn session_exists(&self, id: &str) -> Result<bool, SessionStoreError> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM sessions WHERE id = ?1", params![id], |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn append_message(&self, session_id: &str, msg: &StoredMessage) -> Result<i64, SessionStoreError> {
        self.conn.execute(
            "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp, token_count)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![session_id, msg.role, msg.content, msg.tool_call_id, msg.tool_calls, msg.tool_name, msg.timestamp, msg.token_count],
        )?;
        let id = self.conn.last_insert_rowid();
        self.conn.execute("UPDATE sessions SET message_count = message_count + 1, ended_at = NULL WHERE id = ?1", params![session_id])?;
        Ok(id)
    }

    pub fn append_messages_batch(&self, session_id: &str, msgs: &[StoredMessage]) -> Result<Vec<i64>, SessionStoreError> {
        let mut ids = Vec::with_capacity(msgs.len());
        let tx = self.conn.unchecked_transaction()?;
        for msg in msgs {
            tx.execute(
                "INSERT INTO messages (session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp, token_count)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![session_id, msg.role, msg.content, msg.tool_call_id, msg.tool_calls, msg.tool_name, msg.timestamp, msg.token_count],
            )?;
            ids.push(tx.last_insert_rowid());
        }
        tx.execute("UPDATE sessions SET message_count = message_count + ?1 WHERE id = ?2", params![msgs.len() as i64, session_id])?;
        tx.commit()?;
        Ok(ids)
    }

    pub fn get_messages(&self, session_id: &str) -> Result<Vec<StoredMessage>, SessionStoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, role, content, tool_call_id, tool_calls, tool_name, timestamp, token_count
             FROM messages WHERE session_id = ?1 ORDER BY timestamp",
        )?;
        let rows = stmt.query_map(params![session_id], row_to_message)?;
        let mut messages = Vec::new();
        for row in rows {
            messages.push(row?);
        }
        Ok(messages)
    }

    pub fn update_session_tokens(&self, id: &str, input: i64, output: i64) -> Result<(), SessionStoreError> {
        self.conn.execute(
            "UPDATE sessions SET input_tokens = input_tokens + ?1, output_tokens = output_tokens + ?2 WHERE id = ?3",
            params![input, output, id],
        )?;
        Ok(())
    }

    pub fn end_session(&self, id: &str, reason: Option<&str>) -> Result<(), SessionStoreError> {
        let now = Utc::now().timestamp() as f64;
        self.conn.execute(
            "UPDATE sessions SET ended_at = ?1, end_reason = ?2 WHERE id = ?3",
            params![now, reason, id],
        )?;
        Ok(())
    }

    pub fn count_sessions(&self) -> Result<i64, SessionStoreError> {
        let count = self.conn.query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))?;
        Ok(count)
    }
}
