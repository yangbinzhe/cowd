//! Durable runtime event store.
//!
//! This is the Mission Harness fact ledger for runtime lifecycle events. It is
//! intentionally separate from Memory, Matrix, and Growth: projections can be
//! rebuilt from these events, while long-term learning remains in the other
//! engines.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::cowd_dirs;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEventScope {
    Mission,
    Session,
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
}

impl RuntimeEventScope {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mission => "mission",
            Self::Session => "session",
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
        }
    }
}

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
}

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

#[derive(Debug)]
pub struct RuntimeEventStore {
    path: PathBuf,
    conn: Mutex<Connection>,
}

impl RuntimeEventStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let conn = Connection::open(&path).map_err(|error| error.to_string())?;
        init_schema(&conn)?;
        Ok(Self {
            path,
            conn: Mutex::new(conn),
        })
    }

    pub fn open_in_memory() -> Result<Self, String> {
        let conn = Connection::open_in_memory().map_err(|error| error.to_string())?;
        init_schema(&conn)?;
        Ok(Self {
            path: PathBuf::from(":memory:"),
            conn: Mutex::new(conn),
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String> {
        if input.stream_id.trim().is_empty() {
            return Err("runtime event stream_id must not be empty".to_string());
        }
        if input.kind.trim().is_empty() {
            return Err("runtime event kind must not be empty".to_string());
        }
        let mut conn = self
            .conn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let tx = conn.transaction().map_err(|error| error.to_string())?;
        let next_sequence: u64 = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence), 0) + 1 FROM runtime_events WHERE stream_id = ?1",
                params![input.stream_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|error| error.to_string())? as u64;
        let event = DurableRuntimeEvent {
            event_id: format!("runtime-event-{}", uuid::Uuid::new_v4()),
            stream_id: input.stream_id,
            sequence: next_sequence,
            scope: input.scope,
            kind: input.kind,
            status: input.status,
            actor: input.actor,
            refs: input.refs,
            payload: input.payload,
            created_at_ms: now_ms(),
        };
        tx.execute(
            "INSERT INTO runtime_events \
             (event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                event.event_id,
                event.stream_id,
                event.sequence as i64,
                event.scope.as_str(),
                event.kind,
                event.status,
                event.actor,
                serde_json::to_string(&event.payload).map_err(|error| error.to_string())?,
                serde_json::to_string(&event.refs).map_err(|error| error.to_string())?,
                event.created_at_ms as i64,
            ],
        )
        .map_err(|error| error.to_string())?;
        tx.commit().map_err(|error| error.to_string())?;
        Ok(event)
    }

    pub fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String> {
        self.query_events(
            "SELECT event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms \
             FROM runtime_events WHERE stream_id = ?1 ORDER BY sequence ASC",
            params![stream_id],
        )
    }

    pub fn list_scope(
        &self,
        scope: RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let conn = self
            .conn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut stmt = conn
            .prepare(
                "SELECT event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms \
                 FROM runtime_events WHERE scope = ?1 ORDER BY created_at_ms DESC LIMIT ?2",
            )
            .map_err(|error| error.to_string())?;
        let rows = stmt
            .query_map(params![scope.as_str(), limit as i64], row_to_event)
            .map_err(|error| error.to_string())?;
        collect_rows(rows)
    }

    pub fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
        let conn = self
            .conn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut stmt = conn
            .prepare(
                "SELECT event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms \
                 FROM runtime_events ORDER BY created_at_ms DESC LIMIT ?1",
            )
            .map_err(|error| error.to_string())?;
        let rows = stmt
            .query_map(params![limit as i64], row_to_event)
            .map_err(|error| error.to_string())?;
        collect_rows(rows)
    }

    pub fn latest_for_stream(
        &self,
        stream_id: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        let conn = self
            .conn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        conn.query_row(
            "SELECT event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms \
             FROM runtime_events WHERE stream_id = ?1 ORDER BY sequence DESC LIMIT 1",
            params![stream_id],
            row_to_event,
        )
        .optional()
        .map_err(|error| error.to_string())
    }

    fn query_events<P>(&self, sql: &str, params: P) -> Result<Vec<DurableRuntimeEvent>, String>
    where
        P: rusqlite::Params,
    {
        let conn = self
            .conn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut stmt = conn.prepare(sql).map_err(|error| error.to_string())?;
        let rows = stmt
            .query_map(params, row_to_event)
            .map_err(|error| error.to_string())?;
        collect_rows(rows)
    }
}

pub fn global_runtime_event_store() -> &'static RuntimeEventStore {
    static STORE: OnceLock<RuntimeEventStore> = OnceLock::new();
    STORE.get_or_init(|| {
        RuntimeEventStore::open(default_event_store_path())
            .expect("runtime event store should open")
    })
}

pub fn record_runtime_event(input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String> {
    global_runtime_event_store().append(input)
}

fn default_event_store_path() -> PathBuf {
    cowd_dirs::config_home_dir().join("storage/runtime-events.sqlite")
}

fn init_schema(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(
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
            created_at_ms INTEGER NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_stream_sequence
            ON runtime_events(stream_id, sequence);
        CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_created
            ON runtime_events(scope, created_at_ms);",
    )
    .map_err(|error| error.to_string())
}

fn row_to_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<DurableRuntimeEvent> {
    let scope: String = row.get(3)?;
    let payload: String = row.get(7)?;
    let refs: String = row.get(8)?;
    Ok(DurableRuntimeEvent {
        event_id: row.get(0)?,
        stream_id: row.get(1)?,
        sequence: row.get::<_, i64>(2)? as u64,
        scope: parse_scope(&scope),
        kind: row.get(4)?,
        status: row.get(5)?,
        actor: row.get(6)?,
        payload: serde_json::from_str(&payload).unwrap_or(serde_json::Value::Null),
        refs: serde_json::from_str(&refs).unwrap_or_default(),
        created_at_ms: row.get::<_, i64>(9)? as u64,
    })
}

fn collect_rows<F>(rows: rusqlite::MappedRows<'_, F>) -> Result<Vec<DurableRuntimeEvent>, String>
where
    F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<DurableRuntimeEvent>,
{
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())
}

fn parse_scope(scope: &str) -> RuntimeEventScope {
    match scope {
        "mission" => RuntimeEventScope::Mission,
        "session" => RuntimeEventScope::Session,
        "session_command" => RuntimeEventScope::SessionCommand,
        "team" => RuntimeEventScope::Team,
        "agent" => RuntimeEventScope::Agent,
        "approval" => RuntimeEventScope::Approval,
        "relation" => RuntimeEventScope::Relation,
        "steward" => RuntimeEventScope::Steward,
        "task" => RuntimeEventScope::Task,
        "worker" => RuntimeEventScope::Worker,
        "schedule" => RuntimeEventScope::Schedule,
        "tool" => RuntimeEventScope::Tool,
        _ => RuntimeEventScope::Mission,
    }
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

    #[test]
    fn sqlite_event_store_appends_and_replays_streams() {
        let store = RuntimeEventStore::open_in_memory().expect("event store");
        let first = store
            .append(RuntimeEventInput {
                stream_id: "mission:one".to_string(),
                scope: RuntimeEventScope::Mission,
                kind: "mission.started".to_string(),
                status: Some("running".to_string()),
                actor: Some("test".to_string()),
                refs: vec![RuntimeEventRef {
                    kind: "session".to_string(),
                    id: "session-a".to_string(),
                }],
                payload: serde_json::json!({"title": "one"}),
            })
            .expect("append first");
        let second = store
            .append(RuntimeEventInput {
                stream_id: "mission:one".to_string(),
                scope: RuntimeEventScope::Mission,
                kind: "mission.completed".to_string(),
                status: Some("completed".to_string()),
                actor: Some("test".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({}),
            })
            .expect("append second");

        assert_eq!(first.sequence, 1);
        assert_eq!(second.sequence, 2);
        let replay = store.list_stream("mission:one").expect("replay");
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].kind, "mission.started");
        assert_eq!(
            store
                .latest_for_stream("mission:one")
                .expect("latest")
                .expect("event")
                .kind,
            "mission.completed"
        );
    }
}
