use std::path::PathBuf;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct SessionEventLogger {
    db_path: PathBuf,
    conn: std::sync::Arc<Mutex<Option<rusqlite::Connection>>>,
}

impl SessionEventLogger {
    pub fn new(sessions_dir: &std::path::Path) -> rusqlite::Result<Self> {
        let db_path = sessions_dir.join("events.db");
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS session_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                turn_number INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                summary TEXT NOT NULL,
                recorded_at REAL NOT NULL,
                FOREIGN KEY (session_id) REFERENCES sessions(id)
            );
            CREATE INDEX IF NOT EXISTS idx_ev_session ON session_events(session_id, turn_number);"
        )?;
        Ok(Self { db_path, conn: std::sync::Arc::new(Mutex::new(Some(conn))) })
    }

    pub fn record(&self, session_id: &str, turn: u32, event_type: &str, summary: &str) {
        let guard = self.conn.lock().unwrap();
        if let Some(ref conn) = *guard {
            let _ = conn.execute(
                "INSERT INTO session_events (session_id, turn_number, event_type, summary, recorded_at) VALUES (?1,?2,?3,?4,?5)",
                rusqlite::params![session_id, turn, event_type, summary, chrono::Utc::now().timestamp() as f64],
            );
        }
    }

    pub fn rebuild_context(&self, session_id: &str, limit: usize) -> String {
        let guard = self.conn.lock().unwrap();
        let Some(ref conn) = *guard else { return String::new(); };
        let mut stmt = match conn.prepare(
            "SELECT event_type, summary FROM session_events WHERE session_id=?1 ORDER BY turn_number DESC, id DESC LIMIT ?2"
        ) {
            Ok(s) => s,
            Err(_) => return String::new(),
        };
        let mut rows = Vec::new();
        let mapped = stmt.query_map(
            rusqlite::params![session_id, limit as i64],
            |row| {
                let t: String = row.get(0)?;
                let s: String = row.get(1)?;
                Ok(format!("- [{t}] {s}"))
            },
        );
        if let Ok(iter) = mapped {
            for r in iter.flatten() { rows.push(r); }
        }

        if rows.is_empty() { return String::new(); }
        let mut ctx = String::from("## Session Context (rebuilt)\n");
        for row in rows { ctx.push_str(&row); ctx.push('\n'); }
        ctx
    }
}
