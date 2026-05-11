use rusqlite::params;

#[deprecated(since = "0.2.0", note = "use crate::unified::UnifiedSessionStore")]
pub struct SessionStore;

pub struct StructuredEvent {
    pub session_id: String,
    pub event_type: String,
    pub data: serde_json::Value,
    pub timestamp: f64,
}

pub const EVENT_TYPES: &[&str] = &[
    "file_edit", "git_commit", "tool_call", "decision", "error",
    "user_preference", "checkpoint", "task_complete",
];

impl crate::store::SessionStore {
    pub fn ensure_event_table(&self) -> Result<(), crate::error::SessionStoreError> {
        let conn = self.conn();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS event_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                data TEXT NOT NULL,
                timestamp REAL NOT NULL,
                FOREIGN KEY (session_id) REFERENCES sessions(id)
            );
            CREATE INDEX IF NOT EXISTS idx_event_session ON event_log(session_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_event_type ON event_log(event_type);
            CREATE VIRTUAL TABLE IF NOT EXISTS event_log_fts USING fts5(data, tokenize='trigram');",
        )?;
        Ok(())
    }

    pub fn log_event(&self, session_id: &str, event_type: &str, data: &serde_json::Value) -> Result<i64, crate::error::SessionStoreError> {
        let conn = self.conn();
        conn.execute(
            "INSERT INTO event_log (session_id, event_type, data, timestamp) VALUES (?1, ?2, ?3, ?4)",
            params![session_id, event_type, serde_json::to_string(data).unwrap_or_default(), chrono::Utc::now().timestamp() as f64],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn search_events(&self, session_id: &str, query: &str, limit: usize) -> Result<Vec<StructuredEvent>, crate::error::SessionStoreError> {
        let conn = self.conn();
        let fts = query.split_whitespace().filter(|w| !w.is_empty())
            .map(|w| format!("\"{}\"", w.replace('"', "\"\"")))
            .collect::<Vec<_>>().join(" OR ");
        if fts.is_empty() { return Ok(Vec::new()); }
        let mut stmt = conn.prepare(
            "SELECT e.session_id, e.event_type, e.data, e.timestamp
             FROM event_log_fts f JOIN event_log e ON e.id = f.rowid
             WHERE e.session_id = ?1 AND event_log_fts MATCH ?2
             ORDER BY rank LIMIT ?3"
        )?;
        let rows = stmt.query_map(params![session_id, fts, limit as i64], |row| {
            Ok(StructuredEvent {
                session_id: row.get(0)?, event_type: row.get(1)?,
                data: serde_json::from_str(&row.get::<_, String>(2).unwrap_or_default()).unwrap_or_default(),
                timestamp: row.get(3)?,
            })
        })?;
        let mut events = Vec::new();
        for row in rows { events.push(row?); }
        Ok(events)
    }

    pub fn rebuild_context_block(&self, session_id: &str) -> Result<String, crate::error::SessionStoreError> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT event_type, data FROM event_log WHERE session_id = ?1 ORDER BY timestamp DESC LIMIT 50"
        )?;
        let rows = stmt.query_map(params![session_id], |row| -> rusqlite::Result<(String, String)> {
            Ok((row.get(0)?, row.get(1)?))
        })?;

        let mut ctx = String::from("## Session Context (rebuilt from events)\n");
        for row in rows {
            let (event_type, data) = row?;
            ctx.push_str(&format!("- [{event_type}] {data}\n"));
        }
        Ok(ctx)
    }
}
