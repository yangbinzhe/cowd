//! PostgreSQL durable session adapter.
//!
//! The adapter is constructed only from the host-owned, bounded
//! [`storage::PostgresExecutor`]. It never accepts a path or a database URL.

use std::{fs, path::Path};

use memory::store::session::{
    OutboxFailureClass, OutboxStatus, SessionEvent, SessionListOptions, SessionListPage,
    SessionMessage, SessionMissionOutboxOperation, SessionMissionOutboxRecord,
    SessionMissionOutboxRequest, SessionRecord, SessionRecoveryManifest, SessionRecoverySignal,
    SessionRuntimeOutboxHealth, SessionRuntimeOutboxRecord, SessionRuntimeOutboxRequest,
    SessionSearchResult, SessionSnapshot, SqliteSessionStore,
};
use postgres::{types::ToSql, Row};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage::{
    PostgresConnection, PostgresConnectionConfig, PostgresExecutor, PostgresMigrationSpec,
    PostgresTransaction, SecretRefResolver,
};

const SESSION_DOMAIN: &str = "session";

/// Portable, complete durable Session state used only by a quiesced cutover.
/// It is deliberately absent from normal request paths: there is no dual
/// write or background replication between selected owners.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionMigrationSnapshot {
    pub schema_version: u32,
    pub sessions: Vec<SessionRecord>,
    pub associations: Vec<SessionMemoryAssociation>,
    pub messages: Vec<SessionMessage>,
    pub events: Vec<SessionEvent>,
    pub checkpoints: Vec<SessionEventCheckpoint>,
    pub snapshots: Vec<SessionSnapshot>,
    pub runtime_outbox: Vec<SessionRuntimeOutboxRecord>,
    pub mission_outbox: Vec<SessionMissionOutboxRecord>,
    pub runtime_history: Vec<SessionOutboxHistory>,
    pub mission_history: Vec<SessionOutboxHistory>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Ord, PartialOrd)]
pub struct SessionMemoryAssociation {
    pub session_id: String,
    pub memory_id: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Ord, PartialOrd)]
pub struct SessionEventCheckpoint {
    pub session_id: String,
    pub checkpoint_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Ord, PartialOrd)]
pub struct SessionOutboxHistory {
    pub request_id: String,
    pub action: String,
    pub actor: Option<String>,
    pub reason: Option<String>,
    pub from_status: String,
    pub to_status: String,
    pub attempts: u32,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMigrationManifest {
    pub domain: String,
    pub source_digest: String,
    pub target_digest: String,
    pub schema_version: u32,
    pub session_count: usize,
    pub message_count: usize,
    pub event_count: usize,
}

impl SessionMigrationSnapshot {
    pub fn canonical_digest(&self) -> memory::store::Result<String> {
        let bytes = serde_json::to_vec(self).map_err(|error| {
            memory::MemoryError::Store(format!("encode session migration snapshot: {error}"))
        })?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

/// Export every durable Session table from a quiesced SQLite owner.
pub fn export_sqlite_session_snapshot(
    source: &SqliteSessionStore,
) -> memory::store::Result<SessionMigrationSnapshot> {
    let connection = source.conn()?;
    let sessions = sqlite_rows(&connection, "SELECT session_id,platform,chat_id,user_id,model,created_at,last_activity,message_count,reset_policy,metadata_json,input_tokens,output_tokens,estimated_cost_usd,status FROM sessions ORDER BY session_id", sqlite_row_to_session)?;
    let associations = sqlite_rows(&connection, "SELECT session_id,memory_id,created_at FROM session_memories ORDER BY session_id,memory_id", |row| {
        Ok(SessionMemoryAssociation { session_id: row.get(0)?, memory_id: row.get(1)?, created_at: row.get(2)? })
    })?;
    let messages = sqlite_rows(&connection, "SELECT stable_message_id,session_id,sequence,role,content_json,blocks_count,tool_use_id,tool_name,token_usage_json,created_at_ms FROM messages ORDER BY session_id,sequence", sqlite_row_to_message)?;
    let events = sqlite_rows(&connection, "SELECT session_id,event_type,event_json,sequence,created_at_ms FROM session_events ORDER BY session_id,sequence", sqlite_row_to_event)?;
    // SQLite checkpoint dedupe is encoded in semantic events. Materialize the
    // same identity into PostgreSQL's indexed checkpoint table at cutover.
    let checkpoints = events
        .iter()
        .filter_map(|event| {
            checkpoint_from_event(event).map(|checkpoint_id| SessionEventCheckpoint {
                session_id: event.session_id.clone(),
                checkpoint_id,
            })
        })
        .collect::<Vec<_>>();
    let snapshots = sqlite_rows(&connection, "SELECT session_id,event_idx,messages_json,created_at_ms FROM session_snapshots ORDER BY session_id,event_idx", sqlite_row_to_snapshot)?;
    let runtime_outbox = sqlite_rows(&connection, "SELECT request_id,turn_id,message_id,session_id,sequence,status,runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms,runtime_options_json FROM session_runtime_outbox ORDER BY request_id", sqlite_row_to_runtime_outbox)?;
    let mission_outbox = sqlite_rows(&connection, "SELECT request_id,session_id,title,workspace_key,operation,status,attempts,next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms FROM session_mission_outbox ORDER BY request_id", sqlite_row_to_mission_outbox)?;
    let runtime_history = sqlite_rows(&connection, "SELECT request_id,action,actor,reason,from_status,to_status,attempts,created_at_ms FROM session_runtime_outbox_history ORDER BY id", sqlite_row_to_history)?;
    let mission_history = sqlite_rows(&connection, "SELECT request_id,action,actor,reason,from_status,to_status,attempts,created_at_ms FROM session_mission_outbox_history ORDER BY id", sqlite_row_to_history)?;
    Ok(SessionMigrationSnapshot {
        schema_version: 2,
        sessions,
        associations,
        messages,
        events,
        checkpoints,
        snapshots,
        runtime_outbox,
        mission_outbox,
        runtime_history,
        mission_history,
    })
}

fn sqlite_rows<T>(
    connection: &rusqlite::Connection,
    statement: &str,
    map: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> memory::store::Result<Vec<T>> {
    let mut prepared = connection
        .prepare(statement)
        .map_err(|error| memory::MemoryError::Store(error.to_string()))?;
    let rows = prepared
        .query_map([], map)
        .map_err(|error| memory::MemoryError::Store(error.to_string()))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| memory::MemoryError::Store(error.to_string()))
}

fn sqlite_row_to_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionMessage> {
    Ok(SessionMessage {
        stable_message_id: row.get(0)?,
        session_id: row.get(1)?,
        sequence: usize::try_from(row.get::<_, i64>(2)?).map_err(sqlite_conversion_error)?,
        role: row.get(3)?,
        content_json: row.get(4)?,
        blocks_count: usize::try_from(row.get::<_, i64>(5)?).map_err(sqlite_conversion_error)?,
        tool_use_id: row.get(6)?,
        tool_name: row.get(7)?,
        token_usage_json: row.get(8)?,
        created_at_ms: u64::try_from(row.get::<_, i64>(9)?).map_err(sqlite_conversion_error)?,
    })
}

fn sqlite_row_to_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRecord> {
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

fn sqlite_row_to_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionEvent> {
    Ok(SessionEvent {
        session_id: row.get(0)?,
        event_type: row.get(1)?,
        event_json: row.get(2)?,
        sequence: usize::try_from(row.get::<_, i64>(3)?).map_err(sqlite_conversion_error)?,
        created_at_ms: u64::try_from(row.get::<_, i64>(4)?).map_err(sqlite_conversion_error)?,
    })
}

fn sqlite_row_to_snapshot(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionSnapshot> {
    Ok(SessionSnapshot {
        session_id: row.get(0)?,
        event_idx: usize::try_from(row.get::<_, i64>(1)?).map_err(sqlite_conversion_error)?,
        messages_json: row.get(2)?,
        created_at_ms: u64::try_from(row.get::<_, i64>(3)?).map_err(sqlite_conversion_error)?,
    })
}

fn sqlite_row_to_runtime_outbox(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<SessionRuntimeOutboxRecord> {
    Ok(SessionRuntimeOutboxRecord {
        request_id: row.get(0)?,
        turn_id: row.get(1)?,
        message_id: row.get(2)?,
        session_id: row.get(3)?,
        sequence: usize::try_from(row.get::<_, i64>(4)?).map_err(sqlite_conversion_error)?,
        status: OutboxStatus::parse(&row.get::<_, String>(5)?)?,
        runtime_commit_cursor: row
            .get::<_, Option<i64>>(6)?
            .map(|value| u64::try_from(value).map_err(sqlite_conversion_error))
            .transpose()?,
        attempts: u32::try_from(row.get::<_, i64>(7)?).map_err(sqlite_conversion_error)?,
        next_attempt_at_ms: u64::try_from(row.get::<_, i64>(8)?)
            .map_err(sqlite_conversion_error)?,
        claim_owner: row.get(9)?,
        claim_expires_at_ms: row
            .get::<_, Option<i64>>(10)?
            .map(|value| u64::try_from(value).map_err(sqlite_conversion_error))
            .transpose()?,
        failure_class: row
            .get::<_, Option<String>>(11)?
            .as_deref()
            .map(OutboxFailureClass::parse)
            .transpose()?,
        last_error: row.get(12)?,
        revision: u64::try_from(row.get::<_, i64>(13)?).map_err(sqlite_conversion_error)?,
        created_at_ms: u64::try_from(row.get::<_, i64>(14)?).map_err(sqlite_conversion_error)?,
        updated_at_ms: u64::try_from(row.get::<_, i64>(15)?).map_err(sqlite_conversion_error)?,
        runtime_options_json: row.get(16)?,
    })
}

fn sqlite_row_to_mission_outbox(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<SessionMissionOutboxRecord> {
    Ok(SessionMissionOutboxRecord {
        request_id: row.get(0)?,
        session_id: row.get(1)?,
        title: row.get(2)?,
        workspace_key: row.get(3)?,
        operation: SessionMissionOutboxOperation::parse(&row.get::<_, String>(4)?)?,
        status: OutboxStatus::parse(&row.get::<_, String>(5)?)?,
        attempts: u32::try_from(row.get::<_, i64>(6)?).map_err(sqlite_conversion_error)?,
        next_attempt_at_ms: u64::try_from(row.get::<_, i64>(7)?)
            .map_err(sqlite_conversion_error)?,
        claim_owner: row.get(8)?,
        claim_expires_at_ms: row
            .get::<_, Option<i64>>(9)?
            .map(|value| u64::try_from(value).map_err(sqlite_conversion_error))
            .transpose()?,
        failure_class: row
            .get::<_, Option<String>>(10)?
            .as_deref()
            .map(OutboxFailureClass::parse)
            .transpose()?,
        last_error: row.get(11)?,
        revision: u64::try_from(row.get::<_, i64>(12)?).map_err(sqlite_conversion_error)?,
        created_at_ms: u64::try_from(row.get::<_, i64>(13)?).map_err(sqlite_conversion_error)?,
        updated_at_ms: u64::try_from(row.get::<_, i64>(14)?).map_err(sqlite_conversion_error)?,
    })
}

fn sqlite_row_to_history(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionOutboxHistory> {
    Ok(SessionOutboxHistory {
        request_id: row.get(0)?,
        action: row.get(1)?,
        actor: row.get(2)?,
        reason: row.get(3)?,
        from_status: row.get(4)?,
        to_status: row.get(5)?,
        attempts: u32::try_from(row.get::<_, i64>(6)?).map_err(sqlite_conversion_error)?,
        created_at_ms: u64::try_from(row.get::<_, i64>(7)?).map_err(sqlite_conversion_error)?,
    })
}

fn sqlite_conversion_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Integer, Box::new(error))
}

const SESSION_MIGRATIONS: &[PostgresMigrationSpec] = &[PostgresMigrationSpec {
    id: "session.0001.durable-session-owner",
    domain: SESSION_DOMAIN,
    version: 1,
    description: "create session, message, event, snapshot and fenced outbox owners",
    statements: &[
        "CREATE TABLE IF NOT EXISTS session_records (
            session_id TEXT PRIMARY KEY,
            platform TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            user_id TEXT,
            model TEXT,
            created_at TEXT NOT NULL,
            last_activity TEXT NOT NULL,
            message_count BIGINT NOT NULL DEFAULT 0,
            reset_policy TEXT NOT NULL,
            metadata_json TEXT,
            input_tokens BIGINT NOT NULL DEFAULT 0,
            output_tokens BIGINT NOT NULL DEFAULT 0,
            estimated_cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'active'
        )",
        "CREATE INDEX IF NOT EXISTS idx_session_records_activity
            ON session_records(last_activity DESC, session_id ASC)",
        "CREATE INDEX IF NOT EXISTS idx_session_records_platform
            ON session_records(platform, last_activity DESC, session_id ASC)",
        "CREATE INDEX IF NOT EXISTS idx_session_records_status_model
            ON session_records(status, model, last_activity DESC, session_id ASC)",
        "CREATE TABLE IF NOT EXISTS session_memory_associations (
            session_id TEXT NOT NULL REFERENCES session_records(session_id) ON DELETE CASCADE,
            memory_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(session_id, memory_id)
        )",
        "CREATE TABLE IF NOT EXISTS session_messages (
            stable_message_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL REFERENCES session_records(session_id) ON DELETE CASCADE,
            sequence BIGINT NOT NULL,
            role TEXT NOT NULL,
            content_json TEXT NOT NULL,
            blocks_count BIGINT NOT NULL DEFAULT 1,
            tool_use_id TEXT,
            tool_name TEXT,
            token_usage_json TEXT,
            created_at_ms BIGINT NOT NULL,
            UNIQUE(session_id, sequence)
        )",
        "CREATE INDEX IF NOT EXISTS idx_session_messages_session_sequence
            ON session_messages(session_id, sequence ASC)",
        "CREATE INDEX IF NOT EXISTS idx_session_messages_search
            ON session_messages USING GIN(to_tsvector('simple', coalesce(role, '') || ' ' || coalesce(content_json, '') || ' ' || coalesce(tool_name, '')))",
        "CREATE TABLE IF NOT EXISTS session_events (
            session_id TEXT NOT NULL REFERENCES session_records(session_id) ON DELETE CASCADE,
            sequence BIGINT NOT NULL,
            event_type TEXT NOT NULL,
            event_json TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            PRIMARY KEY(session_id, sequence)
        )",
        "CREATE INDEX IF NOT EXISTS idx_session_events_type_sequence
            ON session_events(session_id, event_type, sequence ASC)",
        "CREATE TABLE IF NOT EXISTS session_event_checkpoints (
            session_id TEXT NOT NULL REFERENCES session_records(session_id) ON DELETE CASCADE,
            checkpoint_id TEXT NOT NULL,
            PRIMARY KEY(session_id, checkpoint_id)
        )",
        "CREATE TABLE IF NOT EXISTS session_snapshots (
            session_id TEXT NOT NULL REFERENCES session_records(session_id) ON DELETE CASCADE,
            event_idx BIGINT NOT NULL,
            messages_json TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            PRIMARY KEY(session_id, event_idx)
        )",
        "CREATE INDEX IF NOT EXISTS idx_session_snapshots_latest
            ON session_snapshots(session_id, event_idx DESC)",
        "CREATE TABLE IF NOT EXISTS session_runtime_outbox (
            request_id TEXT PRIMARY KEY,
            turn_id TEXT NOT NULL UNIQUE,
            message_id TEXT NOT NULL UNIQUE REFERENCES session_messages(stable_message_id) ON DELETE CASCADE,
            session_id TEXT NOT NULL REFERENCES session_records(session_id) ON DELETE CASCADE,
            sequence BIGINT NOT NULL,
            status TEXT NOT NULL,
            runtime_commit_cursor BIGINT,
            attempts BIGINT NOT NULL DEFAULT 0,
            next_attempt_at_ms BIGINT NOT NULL,
            claim_owner TEXT,
            claim_expires_at_ms BIGINT,
            failure_class TEXT,
            last_error TEXT,
            revision BIGINT NOT NULL DEFAULT 0,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL,
            runtime_options_json TEXT
        )",
        "CREATE INDEX IF NOT EXISTS idx_session_runtime_outbox_claim
            ON session_runtime_outbox(status, next_attempt_at_ms, claim_expires_at_ms, sequence)",
        "CREATE INDEX IF NOT EXISTS idx_session_runtime_outbox_session
            ON session_runtime_outbox(session_id, created_at_ms DESC)",
        "CREATE TABLE IF NOT EXISTS session_runtime_outbox_history (
            history_id BIGSERIAL PRIMARY KEY,
            request_id TEXT NOT NULL REFERENCES session_runtime_outbox(request_id) ON DELETE CASCADE,
            action TEXT NOT NULL,
            actor TEXT,
            expected_revision BIGINT,
            previous_status TEXT NOT NULL,
            next_status TEXT NOT NULL,
            detail TEXT,
            reason TEXT,
            from_status TEXT,
            to_status TEXT,
            attempts BIGINT,
            created_at_ms BIGINT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS session_mission_outbox (
            request_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            title TEXT NOT NULL,
            workspace_key TEXT NOT NULL,
            operation TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts BIGINT NOT NULL DEFAULT 0,
            next_attempt_at_ms BIGINT NOT NULL,
            claim_owner TEXT,
            claim_expires_at_ms BIGINT,
            failure_class TEXT,
            last_error TEXT,
            revision BIGINT NOT NULL DEFAULT 0,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_session_mission_outbox_claim
            ON session_mission_outbox(workspace_key, status, next_attempt_at_ms, claim_expires_at_ms, created_at_ms)",
        "CREATE INDEX IF NOT EXISTS idx_session_mission_outbox_session
            ON session_mission_outbox(session_id, created_at_ms DESC)",
        "CREATE TABLE IF NOT EXISTS session_mission_outbox_history (
            history_id BIGSERIAL PRIMARY KEY,
            request_id TEXT NOT NULL REFERENCES session_mission_outbox(request_id) ON DELETE CASCADE,
            action TEXT NOT NULL,
            actor TEXT,
            expected_revision BIGINT,
            previous_status TEXT NOT NULL,
            next_status TEXT NOT NULL,
            detail TEXT,
            reason TEXT,
            from_status TEXT,
            to_status TEXT,
            attempts BIGINT,
            created_at_ms BIGINT NOT NULL
        )",
    ],
}, PostgresMigrationSpec {
    id: "session.0002.history-copy-fields",
    domain: SESSION_DOMAIN,
    version: 2,
    description: "preserve portable outbox history fields during SQLite cutover",
    statements: &[
        "ALTER TABLE session_runtime_outbox_history ADD COLUMN IF NOT EXISTS reason TEXT",
        "ALTER TABLE session_runtime_outbox_history ADD COLUMN IF NOT EXISTS from_status TEXT",
        "ALTER TABLE session_runtime_outbox_history ADD COLUMN IF NOT EXISTS to_status TEXT",
        "ALTER TABLE session_runtime_outbox_history ADD COLUMN IF NOT EXISTS attempts BIGINT",
        "ALTER TABLE session_mission_outbox_history ADD COLUMN IF NOT EXISTS reason TEXT",
        "ALTER TABLE session_mission_outbox_history ADD COLUMN IF NOT EXISTS from_status TEXT",
        "ALTER TABLE session_mission_outbox_history ADD COLUMN IF NOT EXISTS to_status TEXT",
        "ALTER TABLE session_mission_outbox_history ADD COLUMN IF NOT EXISTS attempts BIGINT",
    ],
}, PostgresMigrationSpec {
    id: "session.0003.reconcile-message-summaries",
    domain: SESSION_DOMAIN,
    version: 3,
    description: "reconcile legacy session counters with durable messages",
    statements: &[
        "UPDATE session_records
            SET message_count = (
                    SELECT COUNT(*) FROM session_messages
                     WHERE session_id=session_records.session_id
                ),
                input_tokens = COALESCE((
                    SELECT SUM(COALESCE(
                        (token_usage_json::jsonb->>'input_tokens')::bigint, 0
                    ))
                      FROM session_messages
                     WHERE session_id=session_records.session_id
                ), 0),
                output_tokens = COALESCE((
                    SELECT SUM(COALESCE(
                        (token_usage_json::jsonb->>'output_tokens')::bigint, 0
                    ))
                      FROM session_messages
                     WHERE session_id=session_records.session_id
        ), 0)",
    ],
}, PostgresMigrationSpec {
    id: "session.0004.safe-message-usage-reconciliation",
    domain: SESSION_DOMAIN,
    version: 4,
    description: "make durable message usage reconciliation tolerant of malformed legacy payloads",
    statements: &[
        "CREATE OR REPLACE FUNCTION cowd_safe_session_usage_token(raw TEXT, token_key TEXT)
         RETURNS BIGINT
         LANGUAGE plpgsql
         IMMUTABLE
         STRICT
         AS $$
         DECLARE
             parsed JSONB;
             token_text TEXT;
         BEGIN
             parsed := raw::jsonb;
             token_text := parsed ->> token_key;
             IF token_text IS NULL THEN
                 RETURN 0;
             END IF;
             IF jsonb_typeof(parsed -> token_key) = 'number'
                AND token_text ~ '^[0-9]+$'
                AND token_text::numeric <= 9223372036854775807::numeric THEN
                 RETURN token_text::bigint;
             END IF;
             RETURN 0;
         EXCEPTION WHEN OTHERS THEN
             RETURN 0;
         END
         $$",
        "UPDATE session_records
            SET message_count = (
                    SELECT COUNT(*) FROM session_messages
                     WHERE session_id=session_records.session_id
                ),
                input_tokens = COALESCE((
                    SELECT SUM(cowd_safe_session_usage_token(
                        token_usage_json, 'input_tokens'
                    ))
                      FROM session_messages
                     WHERE session_id=session_records.session_id
                ), 0),
                output_tokens = COALESCE((
                    SELECT SUM(cowd_safe_session_usage_token(
                        token_usage_json, 'output_tokens'
                    ))
                      FROM session_messages
                     WHERE session_id=session_records.session_id
        ), 0)",
    ],
}, PostgresMigrationSpec {
    id: "session.0005.monotonic-session-activity-ms",
    domain: SESSION_DOMAIN,
    version: 5,
    description: "align PostgreSQL session activity clocks with SQLite monotonic millisecond semantics",
    statements: &[
        "CREATE OR REPLACE FUNCTION cowd_safe_session_epoch_ms(raw TEXT)
         RETURNS BIGINT
         LANGUAGE plpgsql
         IMMUTABLE
         STRICT
         AS $$
         BEGIN
             RETURN GREATEST(
                 0,
                 FLOOR(EXTRACT(EPOCH FROM raw::timestamptz) * 1000)::bigint
             );
         EXCEPTION WHEN OTHERS THEN
             RETURN 0;
         END
         $$",
        "ALTER TABLE session_records ADD COLUMN IF NOT EXISTS created_at_ms BIGINT NOT NULL DEFAULT 0",
        "ALTER TABLE session_records ADD COLUMN IF NOT EXISTS updated_at_ms BIGINT NOT NULL DEFAULT 0",
        "UPDATE session_records
            SET created_at_ms = GREATEST(created_at_ms, cowd_safe_session_epoch_ms(created_at)),
                updated_at_ms = GREATEST(
                    updated_at_ms,
                    cowd_safe_session_epoch_ms(last_activity),
                    cowd_safe_session_epoch_ms(created_at)
                )",
        "CREATE INDEX IF NOT EXISTS idx_session_records_updated_ms
            ON session_records(updated_at_ms DESC, session_id ASC)",
    ],
}, PostgresMigrationSpec {
    id: "session.0006.recovery-manifest",
    domain: SESSION_DOMAIN,
    version: 6,
    description: "add body-free durable session recovery manifests",
    statements: &[
        "CREATE TABLE IF NOT EXISTS session_recovery_manifest (
            session_id TEXT PRIMARY KEY REFERENCES session_records(session_id) ON DELETE CASCADE,
            durable_cursor BIGINT NOT NULL DEFAULT 0,
            history_revision BIGINT NOT NULL DEFAULT 0,
            transcript_messages BIGINT NOT NULL DEFAULT 0,
            transcript_bytes BIGINT NOT NULL DEFAULT 0,
            in_flight_turn BOOLEAN NOT NULL DEFAULT FALSE,
            pending_approval BOOLEAN NOT NULL DEFAULT FALSE,
            active_writer_or_attachment BOOLEAN NOT NULL DEFAULT FALSE,
            mission_agent_team_continuation BOOLEAN NOT NULL DEFAULT FALSE,
            last_activity_ms BIGINT NOT NULL DEFAULT 0,
            manifest_revision BIGINT NOT NULL DEFAULT 0
        )",
        "CREATE INDEX IF NOT EXISTS idx_session_recovery_required
            ON session_recovery_manifest(
                in_flight_turn,
                pending_approval,
                active_writer_or_attachment,
                mission_agent_team_continuation,
                last_activity_ms DESC
            )",
        "CREATE OR REPLACE FUNCTION cowd_refresh_session_recovery_manifest(
             target_session_id TEXT,
             bump_history BOOLEAN
         )
         RETURNS VOID
         LANGUAGE plpgsql
         AS $$
         BEGIN
             INSERT INTO session_recovery_manifest(
                 session_id, durable_cursor, history_revision,
                 transcript_messages, transcript_bytes, in_flight_turn,
                 active_writer_or_attachment,
                 mission_agent_team_continuation, last_activity_ms,
                 manifest_revision
             )
             SELECT
                 record.session_id,
                 COALESCE((
                     SELECT MAX(sequence) + 1 FROM session_messages
                      WHERE session_id=record.session_id
                 ), 0),
                 CASE WHEN bump_history THEN 1 ELSE 0 END,
                 COALESCE((
                     SELECT COUNT(*) FROM session_messages
                      WHERE session_id=record.session_id
                 ), 0),
                 COALESCE((
                     SELECT SUM(
                         octet_length(stable_message_id)
                         + octet_length(session_id)
                         + octet_length(role)
                         + octet_length(content_json)
                         + octet_length(COALESCE(token_usage_json, ''))
                         + octet_length(COALESCE(tool_use_id, ''))
                         + octet_length(COALESCE(tool_name, ''))
                     )
                     FROM session_messages WHERE session_id=record.session_id
                 ), 0),
                 EXISTS(
                     SELECT 1 FROM session_runtime_outbox
                      WHERE session_id=record.session_id
                        AND status IN ('pending', 'claimed', 'retry_scheduled')
                 ),
                 COALESCE((
                     SELECT jsonb_array_length(
                         (event_json::jsonb)->'snapshot'->'attachments'
                     ) > 0
                       FROM session_events
                      WHERE session_id=record.session_id
                        AND event_type='session.lifecycle.v1'
                      ORDER BY sequence DESC
                      LIMIT 1
                 ), FALSE),
                 EXISTS(
                     SELECT 1 FROM session_mission_outbox
                      WHERE session_id=record.session_id
                        AND operation = 'start'
                        AND status IN ('pending', 'claimed', 'retry_scheduled')
                 ),
                 GREATEST(record.created_at_ms, record.updated_at_ms),
                 1
             FROM session_records AS record
             WHERE record.session_id=target_session_id
             ON CONFLICT(session_id) DO UPDATE SET
                 durable_cursor=EXCLUDED.durable_cursor,
                 history_revision=session_recovery_manifest.history_revision
                     + CASE WHEN bump_history THEN 1 ELSE 0 END,
                 transcript_messages=EXCLUDED.transcript_messages,
                 transcript_bytes=EXCLUDED.transcript_bytes,
                 in_flight_turn=EXCLUDED.in_flight_turn,
                 active_writer_or_attachment=
                     EXCLUDED.active_writer_or_attachment,
                 mission_agent_team_continuation=
                     EXCLUDED.mission_agent_team_continuation,
                 last_activity_ms=GREATEST(
                     session_recovery_manifest.last_activity_ms,
                     EXCLUDED.last_activity_ms
                 ),
                 manifest_revision=
                     session_recovery_manifest.manifest_revision + 1;
         END
         $$",
        "CREATE OR REPLACE FUNCTION cowd_session_recovery_manifest_trigger()
         RETURNS TRIGGER
         LANGUAGE plpgsql
         AS $$
         DECLARE
             target_session_id TEXT;
         BEGIN
             IF TG_OP = 'DELETE' THEN
                 target_session_id := OLD.session_id;
                 PERFORM cowd_refresh_session_recovery_manifest(
                     target_session_id,
                     TG_TABLE_NAME = 'session_messages'
                 );
                 RETURN OLD;
             ELSE
                 target_session_id := NEW.session_id;
                 PERFORM cowd_refresh_session_recovery_manifest(
                     target_session_id,
                     TG_TABLE_NAME = 'session_messages'
                 );
                 RETURN NEW;
             END IF;
         END
         $$",
        "DROP TRIGGER IF EXISTS session_recovery_record_change ON session_records",
        "CREATE TRIGGER session_recovery_record_change
             AFTER INSERT OR UPDATE OF status, last_activity, updated_at_ms
                ON session_records
              FOR EACH ROW EXECUTE FUNCTION cowd_session_recovery_manifest_trigger()",
        "DROP TRIGGER IF EXISTS session_recovery_message_change ON session_messages",
        "CREATE TRIGGER session_recovery_message_change
             AFTER INSERT OR UPDATE OR DELETE ON session_messages
              FOR EACH ROW EXECUTE FUNCTION cowd_session_recovery_manifest_trigger()",
        "DROP TRIGGER IF EXISTS session_recovery_lifecycle_event_change ON session_events",
        "CREATE TRIGGER session_recovery_lifecycle_event_change
             AFTER INSERT ON session_events
              FOR EACH ROW
              WHEN (NEW.event_type = 'session.lifecycle.v1')
              EXECUTE FUNCTION cowd_session_recovery_manifest_trigger()",
        "DROP TRIGGER IF EXISTS session_recovery_runtime_outbox_change ON session_runtime_outbox",
        "CREATE TRIGGER session_recovery_runtime_outbox_change
             AFTER INSERT OR UPDATE OF status ON session_runtime_outbox
              FOR EACH ROW EXECUTE FUNCTION cowd_session_recovery_manifest_trigger()",
        "DROP TRIGGER IF EXISTS session_recovery_mission_outbox_change ON session_mission_outbox",
        "CREATE TRIGGER session_recovery_mission_outbox_change
             AFTER INSERT OR UPDATE OF status ON session_mission_outbox
              FOR EACH ROW EXECUTE FUNCTION cowd_session_recovery_manifest_trigger()",
        "SELECT cowd_refresh_session_recovery_manifest(session_id, FALSE)
           FROM session_records",
    ],
}, PostgresMigrationSpec {
    id: "session.0007.lifecycle-recovery-signal",
    domain: SESSION_DOMAIN,
    version: 7,
    description: "derive durable recovery attachment state from lifecycle events",
    statements: &[
        "CREATE OR REPLACE FUNCTION cowd_session_recovery_lifecycle_trigger()
         RETURNS TRIGGER
         LANGUAGE plpgsql
         AS $$
         BEGIN
             UPDATE session_recovery_manifest
                SET active_writer_or_attachment =
                        COALESCE(
                            jsonb_array_length(
                                (NEW.event_json::jsonb)->'snapshot'->'attachments'
                            ) > 0,
                            FALSE
                        ),
                    last_activity_ms=GREATEST(last_activity_ms, NEW.created_at_ms),
                    manifest_revision=manifest_revision + 1
              WHERE session_id=NEW.session_id;
             RETURN NEW;
         END
         $$",
        "DROP TRIGGER IF EXISTS session_recovery_lifecycle_event_change ON session_events",
        "CREATE TRIGGER session_recovery_lifecycle_event_change
             AFTER INSERT ON session_events
              FOR EACH ROW
              WHEN (NEW.event_type = 'session.lifecycle.v1')
              EXECUTE FUNCTION cowd_session_recovery_lifecycle_trigger()",
        "UPDATE session_recovery_manifest AS manifest
            SET active_writer_or_attachment=latest.active,
                last_activity_ms=GREATEST(
                    manifest.last_activity_ms,
                    latest.created_at_ms
                ),
                manifest_revision=manifest.manifest_revision + 1
           FROM (
                SELECT DISTINCT ON (session_id)
                       session_id,
                       COALESCE(
                           jsonb_array_length(
                               (event_json::jsonb)->'snapshot'->'attachments'
                           ) > 0,
                           FALSE
                       ) AS active,
                       created_at_ms
                  FROM session_events
                 WHERE event_type='session.lifecycle.v1'
                 ORDER BY session_id, sequence DESC
           ) AS latest
          WHERE manifest.session_id=latest.session_id",
    ],
}];

#[derive(Clone, Debug)]
pub struct PostgresSessionStore {
    executor: PostgresExecutor,
}

impl PostgresSessionStore {
    pub fn new(executor: PostgresExecutor) -> memory::store::Result<Self> {
        prepare_legacy_session_usage_for_migration(&executor)?;
        executor
            .apply_migrations(SESSION_DOMAIN, SESSION_MIGRATIONS)
            .map_err(storage_error)?;
        Ok(Self { executor })
    }

    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> memory::store::Result<Self> {
        PostgresExecutor::connect(config, resolver)
            .map_err(storage_error)
            .and_then(Self::new)
    }

    #[must_use]
    pub fn executor(&self) -> &PostgresExecutor {
        &self.executor
    }

    pub fn create_session(&self, session: &SessionRecord) -> memory::store::Result<()> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .execute(
                "INSERT INTO session_records(
                    session_id, platform, chat_id, user_id, model, created_at,
                    last_activity, message_count, reset_policy, metadata_json,
                    input_tokens, output_tokens, estimated_cost_usd, status,
                    created_at_ms, updated_at_ms
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
                    cowd_safe_session_epoch_ms($6), cowd_safe_session_epoch_ms($7))
                 ON CONFLICT(session_id) DO NOTHING",
                &session_params(session),
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn get_session(&self, session_id: &str) -> memory::store::Result<Option<SessionRecord>> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .query_opt(SESSION_SELECT_BY_ID, &[&session_id])
            .map_err(postgres_error)?
            .map(|row| row_to_session(&row))
            .transpose()
    }

    pub fn get_session_recovery_manifest(
        &self,
        session_id: &str,
    ) -> memory::store::Result<Option<SessionRecoveryManifest>> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT session_id, durable_cursor, history_revision,
                        transcript_messages, transcript_bytes, in_flight_turn,
                        pending_approval, active_writer_or_attachment,
                        mission_agent_team_continuation, last_activity_ms,
                        manifest_revision
                   FROM session_recovery_manifest
                  WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_recovery_manifest(&row))
            .transpose()
    }

    pub fn list_active_session_recovery_manifests(
        &self,
        offset: usize,
        limit: usize,
    ) -> memory::store::Result<Vec<SessionRecoveryManifest>> {
        let offset = to_i64(offset, "recovery manifest offset")?;
        let limit = to_i64(limit.max(1), "recovery manifest limit")?;
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .query(
                "SELECT manifest.session_id, manifest.durable_cursor,
                        manifest.history_revision, manifest.transcript_messages,
                        manifest.transcript_bytes, manifest.in_flight_turn,
                        manifest.pending_approval,
                        manifest.active_writer_or_attachment,
                        manifest.mission_agent_team_continuation,
                        manifest.last_activity_ms, manifest.manifest_revision
                   FROM session_recovery_manifest AS manifest
                   JOIN session_records AS record
                     ON record.session_id=manifest.session_id
                  WHERE record.status='active'
                  ORDER BY manifest.last_activity_ms DESC, manifest.session_id ASC
                  LIMIT $1 OFFSET $2",
                &[&limit, &offset],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_recovery_manifest)
            .collect()
    }

    pub fn set_session_recovery_signal(
        &self,
        session_id: &str,
        signal: SessionRecoverySignal,
        active: bool,
        observed_at_ms: u64,
    ) -> memory::store::Result<SessionRecoveryManifest> {
        let column = match signal {
            SessionRecoverySignal::PendingApproval => "pending_approval",
            SessionRecoverySignal::ActiveWriterOrAttachment => "active_writer_or_attachment",
            SessionRecoverySignal::MissionAgentTeamContinuation => {
                "mission_agent_team_continuation"
            }
        };
        let observed_at_ms = to_u64_i64(observed_at_ms, "recovery observed_at_ms")?;
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let statement = format!(
            "UPDATE session_recovery_manifest
                SET {column}=$2,
                    last_activity_ms=GREATEST(last_activity_ms, $3),
                    manifest_revision=manifest_revision + 1
              WHERE session_id=$1
          RETURNING session_id, durable_cursor, history_revision,
                    transcript_messages, transcript_bytes, in_flight_turn,
                    pending_approval, active_writer_or_attachment,
                    mission_agent_team_continuation, last_activity_ms,
                    manifest_revision"
        );
        connection
            .query_opt(&statement, &[&session_id, &active, &observed_at_ms])
            .map_err(postgres_error)?
            .map(|row| row_to_recovery_manifest(&row))
            .transpose()?
            .ok_or_else(|| {
                memory::MemoryError::Store(format!(
                    "session recovery manifest `{session_id}` does not exist"
                ))
            })
    }

    pub fn update_session(&self, session: &SessionRecord) -> memory::store::Result<()> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .execute(
                "UPDATE session_records SET
                    platform=$2, chat_id=$3, user_id=$4, model=$5, created_at=$6,
                    last_activity=$7, message_count=$8, reset_policy=$9, metadata_json=$10,
                    input_tokens=$11, output_tokens=$12, estimated_cost_usd=$13, status=$14,
                    created_at_ms=cowd_safe_session_epoch_ms($6),
                    updated_at_ms=cowd_safe_session_epoch_ms($7)
                 WHERE session_id=$1",
                &session_params(session),
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn upsert_session(&self, session: &SessionRecord) -> memory::store::Result<()> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .execute(
                "INSERT INTO session_records(
                    session_id, platform, chat_id, user_id, model, created_at,
                    last_activity, message_count, reset_policy, metadata_json,
                    input_tokens, output_tokens, estimated_cost_usd, status,
                    created_at_ms, updated_at_ms
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
                    cowd_safe_session_epoch_ms($6), cowd_safe_session_epoch_ms($7))
                 ON CONFLICT(session_id) DO UPDATE SET
                    platform=EXCLUDED.platform, chat_id=EXCLUDED.chat_id,
                    user_id=EXCLUDED.user_id, model=EXCLUDED.model,
                    created_at=EXCLUDED.created_at, last_activity=EXCLUDED.last_activity,
                    message_count=EXCLUDED.message_count, reset_policy=EXCLUDED.reset_policy,
                    metadata_json=EXCLUDED.metadata_json, input_tokens=EXCLUDED.input_tokens,
                    output_tokens=EXCLUDED.output_tokens,
                    estimated_cost_usd=EXCLUDED.estimated_cost_usd, status=EXCLUDED.status,
                    created_at_ms=EXCLUDED.created_at_ms,
                    updated_at_ms=EXCLUDED.updated_at_ms",
                &session_params(session),
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn delete_session(&self, session_id: &str) -> memory::store::Result<()> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .execute(
                "DELETE FROM session_records WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn mark_session_closed(&self, session_id: &str) -> memory::store::Result<()> {
        let now_at = chrono::Utc::now();
        let now = now_at.to_rfc3339();
        let now_ms = now_at.timestamp_millis().max(0);
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .execute(
                "UPDATE session_records
                    SET status='closed', last_activity=$1,
                        updated_at_ms=GREATEST(updated_at_ms, $2)
                  WHERE session_id=$3",
                &[&now, &now_ms, &session_id],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn list_sessions(&self) -> memory::store::Result<Vec<SessionRecord>> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .query(
                "SELECT session_id, platform, chat_id, user_id, model, created_at,
                        last_activity, message_count, reset_policy, metadata_json,
                        input_tokens, output_tokens, estimated_cost_usd, status
                   FROM session_records ORDER BY last_activity DESC, session_id ASC",
                &[],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_session)
            .collect()
    }

    pub fn list_sessions_by_platform(
        &self,
        platform: &str,
    ) -> memory::store::Result<Vec<SessionRecord>> {
        self.query_sessions(
            "SELECT session_id, platform, chat_id, user_id, model, created_at,
                    last_activity, message_count, reset_policy, metadata_json,
                    input_tokens, output_tokens, estimated_cost_usd, status
               FROM session_records WHERE platform=$1
               ORDER BY last_activity DESC, session_id ASC",
            &[&platform],
        )
    }

    pub fn list_sessions_by_workspace_root(
        &self,
        workspace_root: &str,
    ) -> memory::store::Result<Vec<SessionRecord>> {
        self.query_sessions(
            "SELECT session_id, platform, chat_id, user_id, model, created_at,
                    last_activity, message_count, reset_policy, metadata_json,
                    input_tokens, output_tokens, estimated_cost_usd, status
               FROM session_records
              WHERE metadata_json IS NOT NULL
                AND metadata_json::jsonb ->> 'workspace_root' = $1
              ORDER BY last_activity DESC, session_id ASC",
            &[&workspace_root],
        )
    }

    pub fn list_sessions_page(
        &self,
        options: &SessionListOptions<'_>,
    ) -> memory::store::Result<SessionListPage> {
        let sort = match options.sort {
            "created_at" => "created_at",
            "message_count" => "message_count",
            "model" => "COALESCE(model, '')",
            "title" => "COALESCE(metadata_json::jsonb ->> 'title', '')",
            _ => "last_activity",
        };
        let order = if options.order.eq_ignore_ascii_case("asc") {
            "ASC"
        } else {
            "DESC"
        };
        let query = options.query.filter(|value| !value.trim().is_empty());
        let status = options.status.filter(|value| !value.trim().is_empty());
        let model = options.model.filter(|value| !value.trim().is_empty());
        let limit = i64::try_from(options.limit.clamp(1, 500))
            .map_err(|_| memory::MemoryError::Store("session page limit overflow".to_string()))?;
        let offset = i64::try_from(options.offset)
            .map_err(|_| memory::MemoryError::Store("session page offset overflow".to_string()))?;
        let where_clause = "WHERE ($1::text IS NULL OR to_tsvector('simple',
                coalesce(platform, '') || ' ' || coalesce(chat_id, '') || ' ' ||
                coalesce(user_id, '') || ' ' || coalesce(metadata_json, ''))
                @@ websearch_to_tsquery('simple', $1)
                OR platform ILIKE '%' || $1 || '%' OR chat_id ILIKE '%' || $1 || '%')
             AND ($2::text IS NULL OR status = $2)
             AND ($3::text IS NULL OR model = $3)";
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let total: i64 = connection
            .query_one(
                &format!("SELECT COUNT(*) FROM session_records {where_clause}"),
                &[&query, &status, &model],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let rows = connection
            .query(
                &format!(
                    "SELECT session_id, platform, chat_id, user_id, model, created_at,
                            last_activity, message_count, reset_policy, metadata_json,
                            input_tokens, output_tokens, estimated_cost_usd, status
                       FROM session_records {where_clause}
                      ORDER BY {sort} {order}, session_id ASC LIMIT $4 OFFSET $5"
                ),
                &[&query, &status, &model, &limit, &offset],
            )
            .map_err(postgres_error)?;
        let records = rows
            .iter()
            .map(row_to_session)
            .collect::<memory::store::Result<_>>()?;
        Ok(SessionListPage {
            records,
            total: usize::try_from(total).map_err(|_| {
                memory::MemoryError::Store("session page count overflow".to_string())
            })?,
        })
    }

    pub fn search_sessions(
        &self,
        query: &str,
        platform: Option<&str>,
        limit: usize,
    ) -> memory::store::Result<Vec<SessionSearchResult>> {
        let limit = i64::try_from(limit.clamp(1, 500))
            .map_err(|_| memory::MemoryError::Store("session search limit overflow".to_string()))?;
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let rows = connection
            .query(
                "SELECT session_id, platform, chat_id, user_id, created_at, last_activity,
                        message_count, null::text
                   FROM session_records
                  WHERE ($2::text IS NULL OR platform=$2)
                    AND (to_tsvector('simple', coalesce(platform, '') || ' ' ||
                         coalesce(chat_id, '') || ' ' || coalesce(user_id, '') || ' ' ||
                         coalesce(metadata_json, '')) @@ websearch_to_tsquery('simple', $1)
                         OR platform ILIKE '%' || $1 || '%' OR chat_id ILIKE '%' || $1 || '%')
                  ORDER BY last_activity DESC, session_id ASC LIMIT $3",
                &[&query, &platform, &limit],
            )
            .map_err(postgres_error)?;
        rows.iter().map(row_to_session_search).collect()
    }

    pub fn associate_memory(&self, session_id: &str, memory_id: &str) -> memory::store::Result<()> {
        let created_at = chrono::Utc::now().to_rfc3339();
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .execute(
                "INSERT INTO session_memory_associations(session_id, memory_id, created_at)
                 VALUES ($1,$2,$3) ON CONFLICT(session_id, memory_id) DO NOTHING",
                &[&session_id, &memory_id, &created_at],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn get_session_memories(&self, session_id: &str) -> memory::store::Result<Vec<String>> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .query(
                "SELECT memory_id FROM session_memory_associations
                 WHERE session_id=$1 ORDER BY memory_id ASC",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .iter()
            .map(|row| row.try_get(0).map_err(postgres_error))
            .collect()
    }

    pub fn disassociate_memory(
        &self,
        session_id: &str,
        memory_id: &str,
    ) -> memory::store::Result<()> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .execute(
                "DELETE FROM session_memory_associations WHERE session_id=$1 AND memory_id=$2",
                &[&session_id, &memory_id],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn insert_message(&self, message: &SessionMessage) -> memory::store::Result<()> {
        let sequence = to_i64(message.sequence, "message sequence")?;
        let blocks_count = to_i64(message.blocks_count, "message blocks")?;
        let created_at_ms = i64::try_from(message.created_at_ms)
            .map_err(|_| memory::MemoryError::Store("message time overflow".to_string()))?;
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .execute(
                "INSERT INTO session_messages(
                    stable_message_id, session_id, sequence, role, content_json, blocks_count,
                    tool_use_id, tool_name, token_usage_json, created_at_ms
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                 ON CONFLICT(session_id, sequence) DO UPDATE SET
                    role=EXCLUDED.role, content_json=EXCLUDED.content_json,
                    blocks_count=EXCLUDED.blocks_count, tool_use_id=EXCLUDED.tool_use_id,
                    tool_name=EXCLUDED.tool_name, token_usage_json=EXCLUDED.token_usage_json,
                    created_at_ms=EXCLUDED.created_at_ms",
                &[
                    &message.stable_message_id,
                    &message.session_id,
                    &sequence,
                    &message.role,
                    &message.content_json,
                    &blocks_count,
                    &message.tool_use_id,
                    &message.tool_name,
                    &message.token_usage_json,
                    &created_at_ms,
                ],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn get_messages(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> memory::store::Result<Vec<SessionMessage>> {
        let limit = to_i64(limit.clamp(1, 500), "message limit")?;
        let offset = to_i64(offset, "message offset")?;
        self.query_messages(
            "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                    tool_use_id, tool_name, token_usage_json, created_at_ms
               FROM session_messages WHERE session_id=$1
              ORDER BY sequence ASC LIMIT $2 OFFSET $3",
            &[&session_id, &limit, &offset],
        )
    }

    pub fn get_messages_from_sequence(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> memory::store::Result<Vec<SessionMessage>> {
        let from_sequence = to_i64(from_sequence, "message sequence")?;
        let limit = to_i64(limit.clamp(1, 500), "message limit")?;
        self.query_messages(
            "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                    tool_use_id, tool_name, token_usage_json, created_at_ms
               FROM session_messages WHERE session_id=$1 AND sequence >= $2
              ORDER BY sequence ASC LIMIT $3",
            &[&session_id, &from_sequence, &limit],
        )
    }

    pub fn get_message_count(&self, session_id: &str) -> memory::store::Result<usize> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let count: i64 = connection
            .query_one(
                "SELECT COUNT(*) FROM session_messages WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        usize::try_from(count)
            .map_err(|_| memory::MemoryError::Store("message count overflow".to_string()))
    }

    pub fn delete_messages_from(
        &self,
        session_id: &str,
        from_sequence: usize,
    ) -> memory::store::Result<usize> {
        let from_sequence = to_i64(from_sequence, "message sequence")?;
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let deleted = connection
            .execute(
                "DELETE FROM session_messages WHERE session_id=$1 AND sequence >= $2",
                &[&session_id, &from_sequence],
            )
            .map_err(postgres_error)?;
        Ok(deleted as usize)
    }

    pub fn get_all_messages(&self, session_id: &str) -> memory::store::Result<Vec<SessionMessage>> {
        self.query_messages(
            "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                    tool_use_id, tool_name, token_usage_json, created_at_ms
               FROM session_messages WHERE session_id=$1 ORDER BY sequence ASC",
            &[&session_id],
        )
    }

    pub fn insert_messages_batch(&self, messages: &[SessionMessage]) -> memory::store::Result<()> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        for message in messages {
            insert_message_tx(&mut transaction, message)?;
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(())
    }

    pub fn append_terminal_message_idempotent(
        &self,
        message_id: &str,
        session_id: &str,
        content_json: &str,
        token_usage_json: Option<&str>,
        created_at_ms: u64,
    ) -> memory::store::Result<(SessionMessage, bool)> {
        if message_id.trim().is_empty() || session_id.trim().is_empty() {
            return Err(memory::MemoryError::Store(
                "terminal message requires stable message and session IDs".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        if let Some(row) = transaction
            .query_opt(
                "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                    tool_use_id, tool_name, token_usage_json, created_at_ms
               FROM session_messages WHERE stable_message_id=$1",
                &[&message_id],
            )
            .map_err(postgres_error)?
        {
            let mut existing = row_to_message(&row)?;
            if existing.session_id != session_id
                || existing.role != "assistant"
                || existing.content_json != content_json
                || matches!(
                    (existing.token_usage_json.as_deref(), token_usage_json),
                    (Some(existing), Some(requested)) if existing != requested
                )
            {
                return Err(memory::MemoryError::Store(format!(
                    "terminal message_id `{message_id}` conflicts with committed content"
                )));
            }
            if existing.token_usage_json.is_none() && token_usage_json.is_some() {
                transaction
                    .execute(
                        "UPDATE session_messages SET token_usage_json=$2
                          WHERE stable_message_id=$1",
                        &[&message_id, &token_usage_json],
                    )
                    .map_err(postgres_error)?;
                existing.token_usage_json = token_usage_json.map(ToOwned::to_owned);
                refresh_session_usage_summary_tx(&mut transaction, session_id)?;
            }
            transaction.commit().map_err(postgres_error)?;
            return Ok((existing, false));
        }
        let sequence: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(sequence), -1) + 1 FROM session_messages WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let message = SessionMessage {
            stable_message_id: message_id.to_string(),
            session_id: session_id.to_string(),
            sequence: from_i64(sequence, "message sequence")?,
            role: "assistant".to_string(),
            content_json: content_json.to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: token_usage_json.map(ToOwned::to_owned),
            created_at_ms,
        };
        insert_message_tx(&mut transaction, &message)?;
        refresh_session_message_summary_tx(&mut transaction, session_id, created_at_ms)?;
        refresh_session_usage_summary_tx(&mut transaction, session_id)?;
        transaction.commit().map_err(postgres_error)?;
        Ok((message, true))
    }

    pub fn append_terminal_transcript_idempotent(
        &self,
        terminal_message_id: &str,
        ingress_message_id: &str,
        session_id: &str,
        messages: &[SessionMessage],
        created_at_ms: u64,
    ) -> memory::store::Result<(Vec<SessionMessage>, bool)> {
        if terminal_message_id.trim().is_empty()
            || ingress_message_id.trim().is_empty()
            || session_id.trim().is_empty()
            || messages.is_empty()
            || messages
                .last()
                .is_none_or(|message| message.stable_message_id != terminal_message_id)
        {
            return Err(memory::MemoryError::Store(
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
            return Err(memory::MemoryError::Store(
                "terminal transcript contains an invalid message row".to_string(),
            ));
        }
        let unique_ids = messages
            .iter()
            .map(|message| message.stable_message_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if unique_ids.len() != messages.len() {
            return Err(memory::MemoryError::Store(
                "terminal transcript contains duplicate stable message IDs".to_string(),
            ));
        }

        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        let mut loaded = Vec::with_capacity(messages.len());
        for requested in messages {
            let existing = transaction
                .query_opt(
                    "SELECT stable_message_id, session_id, sequence, role, content_json,
                            blocks_count, tool_use_id, tool_name, token_usage_json, created_at_ms
                       FROM session_messages WHERE stable_message_id=$1",
                    &[&requested.stable_message_id],
                )
                .map_err(postgres_error)?
                .map(|row| row_to_message(&row))
                .transpose()?;
            loaded.push(existing);
        }
        let terminal_exists = loaded.last().is_some_and(Option::is_some);
        if terminal_exists {
            let mut committed = Vec::with_capacity(messages.len());
            for (requested, existing) in messages.iter().zip(loaded.into_iter()) {
                let existing = existing.ok_or_else(|| {
                    memory::MemoryError::Store(format!(
                        "terminal transcript `{terminal_message_id}` is partially committed"
                    ))
                })?;
                if existing.session_id != requested.session_id
                    || existing.role != requested.role
                    || existing.content_json != requested.content_json
                    || existing.blocks_count != requested.blocks_count
                    || existing.tool_use_id != requested.tool_use_id
                    || existing.tool_name != requested.tool_name
                    || existing.token_usage_json != requested.token_usage_json
                {
                    return Err(memory::MemoryError::Store(format!(
                        "terminal transcript message_id `{}` conflicts with committed content",
                        requested.stable_message_id
                    )));
                }
                committed.push(existing);
            }
            committed.sort_by_key(|message| message.sequence);
            transaction.commit().map_err(postgres_error)?;
            return Ok((committed, false));
        }
        if loaded.iter().any(Option::is_some) {
            return Err(memory::MemoryError::Store(format!(
                "terminal transcript `{terminal_message_id}` collides with existing intermediate rows"
            )));
        }

        let _ingress_sequence: i64 = transaction
            .query_opt(
                "SELECT sequence FROM session_messages
                  WHERE stable_message_id=$1 AND session_id=$2 AND role='user'",
                &[&ingress_message_id, &session_id],
            )
            .map_err(postgres_error)?
            .ok_or_else(|| {
                memory::MemoryError::Store(format!(
                    "terminal transcript ingress `{ingress_message_id}` is not committed"
                ))
            })?
            .try_get(0)
            .map_err(postgres_error)?;
        // Published sequence values are immutable Surface cursors. Append the
        // transcript physically and reconstruct turn causality from metadata.
        let first_sequence = transaction
            .query_one(
                "SELECT COALESCE(MAX(sequence), -1) + 1
                   FROM session_messages WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get::<_, i64>(0)
            .map_err(postgres_error)
            .and_then(|sequence| from_i64(sequence, "message sequence"))?;
        let mut committed = Vec::with_capacity(messages.len());
        for (index, requested) in messages.iter().enumerate() {
            let mut message = requested.clone();
            message.sequence = first_sequence.saturating_add(index);
            message.created_at_ms = created_at_ms.saturating_add(index as u64);
            insert_message_tx(&mut transaction, &message)?;
            committed.push(message);
        }
        let last_created_at = committed
            .last()
            .map_or(created_at_ms, |message| message.created_at_ms);
        refresh_session_message_summary_tx(&mut transaction, session_id, last_created_at)?;
        refresh_session_usage_summary_tx(&mut transaction, session_id)?;
        transaction.commit().map_err(postgres_error)?;
        Ok((committed, true))
    }

    pub fn search_messages(
        &self,
        query: &str,
        session_id: Option<&str>,
        limit: usize,
    ) -> memory::store::Result<Vec<SessionMessage>> {
        let limit = to_i64(limit.clamp(1, 500), "message search limit")?;
        self.query_messages(
            "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                    tool_use_id, tool_name, token_usage_json, created_at_ms
               FROM session_messages
              WHERE ($2::text IS NULL OR session_id=$2)
                AND (to_tsvector('simple', coalesce(role,'') || ' ' || coalesce(content_json,'') || ' ' || coalesce(tool_name,''))
                      @@ websearch_to_tsquery('simple', $1)
                     OR content_json ILIKE '%' || $1 || '%')
              ORDER BY sequence ASC LIMIT $3",
            &[&query, &session_id, &limit],
        )
    }

    pub fn search_messages_in_sessions(
        &self,
        query: &str,
        session_ids: &[String],
        limit: usize,
    ) -> memory::store::Result<Vec<SessionMessage>> {
        if session_ids.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let scope = serde_json::to_string(session_ids).map_err(|error| {
            memory::MemoryError::Store(format!("encode search session scope: {error}"))
        })?;
        let limit = to_i64(limit.min(500), "message search limit")?;
        self.query_messages(
            "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                    tool_use_id, tool_name, token_usage_json, created_at_ms
               FROM session_messages
              WHERE session_id IN (SELECT value FROM jsonb_array_elements_text($2::jsonb))
                AND (to_tsvector('simple', coalesce(role,'') || ' ' || coalesce(content_json,'') || ' ' || coalesce(tool_name,''))
                      @@ websearch_to_tsquery('simple', $1)
                     OR content_json ILIKE '%' || $1 || '%')
              ORDER BY sequence ASC LIMIT $3",
            &[&query, &scope, &limit],
        )
    }

    pub fn append_event(&self, event: &SessionEvent) -> memory::store::Result<()> {
        let sequence = to_i64(event.sequence, "event sequence")?;
        let created_at_ms = i64::try_from(event.created_at_ms)
            .map_err(|_| memory::MemoryError::Store("event time overflow".to_string()))?;
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .execute(
                "INSERT INTO session_events(session_id, sequence, event_type, event_json, created_at_ms)
                 VALUES ($1,$2,$3,$4,$5)",
                &[
                    &event.session_id,
                    &sequence,
                    &event.event_type,
                    &event.event_json,
                    &created_at_ms,
                ],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    /// Allocate a contiguous, session-local event sequence under the row lock
    /// of its canonical session record. Independent sessions use different
    /// rows and therefore do not serialize behind a process-wide mutex.
    pub fn append_events_allocating_sequence(
        &self,
        events: &[SessionEvent],
    ) -> memory::store::Result<Vec<SessionEvent>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let session_id = events[0].session_id.as_str();
        if session_id.trim().is_empty() || events.iter().any(|event| event.session_id != session_id)
        {
            return Err(memory::MemoryError::Store(
                "session event batch must have one non-empty session id".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        let next: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let mut allocated = Vec::with_capacity(events.len());
        for (index, event) in events.iter().enumerate() {
            let sequence = next
                .checked_add(i64::try_from(index).map_err(|_| {
                    memory::MemoryError::Store("event batch index overflow".to_string())
                })?)
                .ok_or_else(|| memory::MemoryError::Store("event sequence overflow".to_string()))?;
            let created_at_ms = i64::try_from(event.created_at_ms)
                .map_err(|_| memory::MemoryError::Store("event time overflow".to_string()))?;
            let stored_sequence = from_i64(sequence, "event sequence")?;
            let event_json = event_json_with_allocated_sequence(event, stored_sequence)?;
            transaction
                .execute(
                    "INSERT INTO session_events(session_id, sequence, event_type, event_json, created_at_ms)
                     VALUES ($1,$2,$3,$4,$5)",
                    &[
                        &event.session_id,
                        &sequence,
                        &event.event_type,
                        &event_json,
                        &created_at_ms,
                    ],
                )
                .map_err(postgres_error)?;
            let mut event = event.clone();
            event.sequence = stored_sequence;
            event.event_json = event_json;
            allocated.push(event);
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(allocated)
    }

    pub fn append_event_allocating_sequence(
        &self,
        event: &SessionEvent,
    ) -> memory::store::Result<SessionEvent> {
        self.append_events_allocating_sequence(std::slice::from_ref(event))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                memory::MemoryError::Store("event allocation returned no row".to_string())
            })
    }

    pub fn append_events_allocating_sequence_if_checkpoint_absent(
        &self,
        events: &[SessionEvent],
        checkpoint_id: &str,
    ) -> memory::store::Result<Option<Vec<SessionEvent>>> {
        if events.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let session_id = events[0].session_id.as_str();
        if session_id.trim().is_empty() || events.iter().any(|event| event.session_id != session_id)
        {
            return Err(memory::MemoryError::Store(
                "atomic session event batch must contain one non-empty session_id".to_string(),
            ));
        }
        if checkpoint_id.trim().is_empty() {
            return Err(memory::MemoryError::Store(
                "checkpoint-aware event batch requires a non-empty checkpoint_id".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        let exists: bool = transaction
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM session_event_checkpoints WHERE session_id=$1 AND checkpoint_id=$2)",
                &[&session_id, &checkpoint_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        if exists {
            transaction.commit().map_err(postgres_error)?;
            return Ok(None);
        }
        transaction
            .execute(
                "INSERT INTO session_event_checkpoints(session_id,checkpoint_id) VALUES($1,$2)",
                &[&session_id, &checkpoint_id],
            )
            .map_err(postgres_error)?;
        let next: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let mut allocated = Vec::with_capacity(events.len());
        for (offset, event) in events.iter().enumerate() {
            let sequence = next
                .checked_add(i64::try_from(offset).map_err(|_| {
                    memory::MemoryError::Store("event batch offset overflow".to_string())
                })?)
                .ok_or_else(|| memory::MemoryError::Store("event sequence overflow".to_string()))?;
            let stored_sequence = from_i64(sequence, "event sequence")?;
            let event_json = event_json_with_allocated_sequence(event, stored_sequence)?;
            transaction.execute(
                "INSERT INTO session_events(session_id, sequence, event_type, event_json, created_at_ms)
                 VALUES ($1,$2,$3,$4,$5)",
                &[&event.session_id, &sequence, &event.event_type, &event_json,
                  &to_u64_i64(event.created_at_ms, "event time")?],
            ).map_err(postgres_error)?;
            let mut stored = event.clone();
            stored.sequence = stored_sequence;
            stored.event_json = event_json;
            allocated.push(stored);
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(Some(allocated))
    }

    pub fn append_context_envelope_event_if_absent_allocating_sequence(
        &self,
        event: &SessionEvent,
    ) -> memory::store::Result<Option<SessionEvent>> {
        if event.event_type != "ContextEnvelope" {
            return self.append_event_allocating_sequence(event).map(Some);
        }
        let envelope_id = context_envelope_id(&event.event_json)?;
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&event.session_id],
            )
            .map_err(postgres_error)?;
        let exists: bool = transaction.query_one(
            "SELECT EXISTS(SELECT 1 FROM session_events WHERE event_type='ContextEnvelope'
              AND COALESCE(event_json::jsonb #>> '{envelope,id}', event_json::jsonb ->> 'envelope_id')=$1)",
            &[&envelope_id],
        ).map_err(postgres_error)?.try_get(0).map_err(postgres_error)?;
        if exists {
            transaction.commit().map_err(postgres_error)?;
            return Ok(None);
        }
        let sequence: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id=$1",
                &[&event.session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        transaction.execute(
            "INSERT INTO session_events(session_id, sequence, event_type, event_json, created_at_ms)
             VALUES ($1,$2,$3,$4,$5)",
            &[&event.session_id, &sequence, &event.event_type, &event.event_json,
              &to_u64_i64(event.created_at_ms, "event time")?],
        ).map_err(postgres_error)?;
        transaction.commit().map_err(postgres_error)?;
        let mut stored = event.clone();
        stored.sequence = from_i64(sequence, "event sequence")?;
        Ok(Some(stored))
    }

    pub fn append_context_envelope_event_if_absent(
        &self,
        event: &SessionEvent,
    ) -> memory::store::Result<bool> {
        self.append_context_envelope_event_if_absent_allocating_sequence(event)
            .map(|stored| stored.is_some())
    }

    pub fn get_events(
        &self,
        session_id: &str,
        from_seq: usize,
    ) -> memory::store::Result<Vec<SessionEvent>> {
        self.query_events(
            "SELECT session_id, event_type, event_json, sequence, created_at_ms FROM session_events
             WHERE session_id=$1 AND sequence >= $2 ORDER BY sequence ASC",
            &[&session_id, &to_i64(from_seq, "event sequence")?],
        )
    }

    pub fn get_events_limited(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> memory::store::Result<Vec<SessionEvent>> {
        self.query_events(
            "SELECT session_id, event_type, event_json, sequence, created_at_ms FROM session_events
             WHERE session_id=$1 AND sequence >= $2 ORDER BY sequence ASC LIMIT $3",
            &[
                &session_id,
                &to_i64(from_seq, "event sequence")?,
                &to_i64(limit, "event limit")?,
            ],
        )
    }

    pub fn get_session_domain_timeline_limited(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> memory::store::Result<Vec<SessionEvent>> {
        self.query_events(
            "SELECT session_id, event_type, event_json, sequence, created_at_ms FROM session_events
             WHERE session_id=$1 AND sequence >= $2 AND event_type != 'RuntimeEvent'
             ORDER BY sequence ASC LIMIT $3",
            &[
                &session_id,
                &to_i64(from_seq, "event sequence")?,
                &to_i64(limit, "event limit")?,
            ],
        )
    }

    pub fn count_session_domain_timeline_from(
        &self,
        session_id: &str,
        from_seq: usize,
    ) -> memory::store::Result<usize> {
        self.count_events_sql("SELECT COUNT(*) FROM session_events WHERE session_id=$1 AND sequence >= $2 AND event_type != 'RuntimeEvent'", &[&session_id, &to_i64(from_seq, "event sequence")?])
    }

    pub fn get_events_by_type_limited(
        &self,
        session_id: &str,
        event_type: &str,
        from_seq: usize,
        limit: usize,
    ) -> memory::store::Result<Vec<SessionEvent>> {
        self.query_events(
            "SELECT session_id, event_type, event_json, sequence, created_at_ms FROM session_events
             WHERE session_id=$1 AND event_type=$2 AND sequence >= $3 ORDER BY sequence ASC LIMIT $4",
            &[&session_id, &event_type, &to_i64(from_seq, "event sequence")?, &to_i64(limit, "event limit")?],
        )
    }

    pub fn count_events_from(
        &self,
        session_id: &str,
        from_seq: usize,
    ) -> memory::store::Result<usize> {
        self.count_events_sql(
            "SELECT COUNT(*) FROM session_events WHERE session_id=$1 AND sequence >= $2",
            &[&session_id, &to_i64(from_seq, "event sequence")?],
        )
    }

    pub fn count_events_by_type_from(
        &self,
        session_id: &str,
        event_type: &str,
        from_seq: usize,
    ) -> memory::store::Result<usize> {
        self.count_events_sql("SELECT COUNT(*) FROM session_events WHERE session_id=$1 AND event_type=$2 AND sequence >= $3", &[&session_id, &event_type, &to_i64(from_seq, "event sequence")?])
    }

    pub fn get_context_event_by_envelope_id(
        &self,
        envelope_id: &str,
    ) -> memory::store::Result<Option<SessionEvent>> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection.query_opt(
            "SELECT session_id, event_type, event_json, sequence, created_at_ms FROM session_events
             WHERE event_type='ContextEnvelope' AND COALESCE(event_json::jsonb #>> '{envelope,id}', event_json::jsonb ->> 'envelope_id')=$1
             ORDER BY created_at_ms DESC LIMIT 1",
            &[&envelope_id],
        ).map_err(postgres_error)?.map(|row| row_to_event(&row)).transpose()
    }

    pub fn next_event_sequence(&self, session_id: &str) -> memory::store::Result<usize> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let value: i64 = connection
            .query_one(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        from_i64(value, "event sequence")
    }

    pub fn delete_events_from(
        &self,
        session_id: &str,
        from_sequence: usize,
    ) -> memory::store::Result<usize> {
        self.delete_events_sql(
            "DELETE FROM session_events WHERE session_id=$1 AND sequence >= $2",
            &[&session_id, &to_i64(from_sequence, "event sequence")?],
        )
    }

    pub fn delete_events_by_type_from(
        &self,
        session_id: &str,
        event_type: &str,
        from_sequence: usize,
    ) -> memory::store::Result<usize> {
        self.delete_events_sql(
            "DELETE FROM session_events WHERE session_id=$1 AND event_type=$2 AND sequence >= $3",
            &[
                &session_id,
                &event_type,
                &to_i64(from_sequence, "event sequence")?,
            ],
        )
    }

    pub fn save_snapshot(&self, snapshot: &SessionSnapshot) -> memory::store::Result<()> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection.execute(
            "INSERT INTO session_snapshots(session_id,event_idx,messages_json,created_at_ms) VALUES($1,$2,$3,$4)
             ON CONFLICT(session_id,event_idx) DO UPDATE SET messages_json=EXCLUDED.messages_json, created_at_ms=EXCLUDED.created_at_ms",
            &[&snapshot.session_id, &to_i64(snapshot.event_idx, "snapshot index")?, &snapshot.messages_json, &to_u64_i64(snapshot.created_at_ms, "snapshot time")?],
        ).map_err(postgres_error)?;
        Ok(())
    }

    pub fn get_latest_snapshot(
        &self,
        session_id: &str,
    ) -> memory::store::Result<Option<SessionSnapshot>> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection.query_opt(
            "SELECT session_id,event_idx,messages_json,created_at_ms FROM session_snapshots WHERE session_id=$1 ORDER BY event_idx DESC LIMIT 1",
            &[&session_id],
        ).map_err(postgres_error)?.map(|row| row_to_snapshot(&row)).transpose()
    }

    pub fn prune_before(&self, cutoff_iso8601: &str) -> memory::store::Result<usize> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let deleted = connection
            .execute(
                "DELETE FROM session_records WHERE last_activity < $1",
                &[&cutoff_iso8601],
            )
            .map_err(postgres_error)?;
        Ok(deleted as usize)
    }

    pub fn upsert_session_with_mission_outbox(
        &self,
        session: &SessionRecord,
        request: &SessionMissionOutboxRequest,
    ) -> memory::store::Result<SessionMissionOutboxRecord> {
        validate_mission_request(request)?;
        if request.session_id != session.session_id {
            return Err(memory::MemoryError::Store(
                "session/mission outbox session identity does not match record".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        upsert_session_tx(&mut transaction, session)?;
        let record = insert_mission_outbox_tx(&mut transaction, request)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    pub fn delete_session_with_mission_outbox(
        &self,
        request: &SessionMissionOutboxRequest,
    ) -> memory::store::Result<bool> {
        validate_mission_request(request)?;
        if request.operation != SessionMissionOutboxOperation::Close {
            return Err(memory::MemoryError::Store(
                "session deletion requires a close mission outbox operation".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let exists: bool = transaction
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM session_records WHERE session_id=$1 FOR UPDATE)",
                &[&request.session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        if !exists {
            transaction.commit().map_err(postgres_error)?;
            return Ok(false);
        }
        // The mission outbox intentionally has no FK to the session record:
        // close must survive the session cascade until Runtime consumes it.
        let record = insert_mission_outbox_tx(&mut transaction, request)?;
        transaction
            .execute(
                "DELETE FROM session_records WHERE session_id=$1",
                &[&request.session_id],
            )
            .map_err(postgres_error)?;
        transaction.commit().map_err(postgres_error)?;
        drop(record);
        Ok(true)
    }

    pub fn get_session_mission_outbox(
        &self,
        request_id: &str,
    ) -> memory::store::Result<Option<SessionMissionOutboxRecord>> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .query_opt(MISSION_OUTBOX_SELECT, &[&request_id])
            .map_err(postgres_error)?
            .map(|row| row_to_mission_outbox(&row))
            .transpose()
    }

    pub fn claim_session_mission_outbox(
        &self,
        workspace_key: &str,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> memory::store::Result<Vec<SessionMissionOutboxRecord>> {
        if workspace_key.trim().is_empty() || worker_id.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let now = to_u64_i64(now_ms, "mission outbox clock")?;
        let lease_expires = now
            .checked_add(to_u64_i64(lease_ms, "mission outbox lease")?)
            .ok_or_else(|| {
                memory::MemoryError::Store("mission outbox lease overflow".to_string())
            })?;
        let limit = to_i64(limit.min(500), "mission outbox limit")?;
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let rows = transaction
            .query(
                "WITH candidates AS (
               SELECT request_id FROM session_mission_outbox
                WHERE workspace_key=$1
                  AND ((status IN ('pending','retry_scheduled') AND next_attempt_at_ms <= $2)
                    OR (status='claimed' AND claim_expires_at_ms <= $2))
                ORDER BY created_at_ms ASC, request_id ASC
                FOR UPDATE SKIP LOCKED LIMIT $3
             )
             UPDATE session_mission_outbox o SET status='claimed', claim_owner=$4,
                    claim_expires_at_ms=$5, attempts=o.attempts+1, revision=o.revision+1,
                    updated_at_ms=$2
               FROM candidates c WHERE o.request_id=c.request_id
             RETURNING o.request_id,o.session_id,o.title,o.workspace_key,o.operation,o.status,
                       o.attempts,o.next_attempt_at_ms,o.claim_owner,o.claim_expires_at_ms,
                       o.failure_class,o.last_error,o.revision,o.created_at_ms,o.updated_at_ms",
                &[&workspace_key, &now, &limit, &worker_id, &lease_expires],
            )
            .map_err(postgres_error)?;
        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let record = row_to_mission_outbox(&row)?;
            append_mission_history_tx(
                &mut transaction,
                &record,
                "claim",
                Some(worker_id),
                None,
                OutboxStatus::Pending,
                OutboxStatus::Claimed,
                None,
                now_ms,
            )?;
            claimed.push(record);
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(claimed)
    }

    pub fn ack_session_mission_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> memory::store::Result<SessionMissionOutboxRecord> {
        self.transition_mission_outbox(
            request_id,
            worker_id,
            expected_revision,
            now_ms,
            "materialized",
            None,
            None,
            "ack",
        )
    }

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
    ) -> memory::store::Result<SessionMissionOutboxRecord> {
        let now = to_u64_i64(now_ms, "mission outbox clock")?;
        let retry_at = to_u64_i64(retry_at_ms, "mission outbox retry")?;
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let existing = mission_outbox_for_update(&mut transaction, request_id)?;
        assert_mission_lease(&existing, worker_id, expected_revision, now_ms)?;
        let blocked =
            failure_class != OutboxFailureClass::Retryable || existing.attempts >= max_attempts;
        let status = if blocked {
            OutboxStatus::BlockedMaterialization
        } else {
            OutboxStatus::RetryScheduled
        };
        let row = transaction.query_one(
            "UPDATE session_mission_outbox SET status=$1, next_attempt_at_ms=$2,
                    claim_owner=NULL, claim_expires_at_ms=NULL, failure_class=$3, last_error=$4,
                    revision=revision+1, updated_at_ms=$5 WHERE request_id=$6 RETURNING
                    request_id,session_id,title,workspace_key,operation,status,attempts,next_attempt_at_ms,
                    claim_owner,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms",
            &[&status.as_str(), &(if blocked { now } else { retry_at }), &failure_class.as_str(), &error,
              &now, &request_id],
        ).map_err(postgres_error)?;
        let record = row_to_mission_outbox(&row)?;
        append_mission_history_tx(
            &mut transaction,
            &record,
            "fail",
            Some(worker_id),
            Some(expected_revision),
            existing.status,
            status,
            Some(error),
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    fn transition_mission_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
        next_status: &str,
        failure_class: Option<OutboxFailureClass>,
        error: Option<&str>,
        action: &str,
    ) -> memory::store::Result<SessionMissionOutboxRecord> {
        let now = to_u64_i64(now_ms, "mission outbox clock")?;
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let existing = mission_outbox_for_update(&mut transaction, request_id)?;
        assert_mission_lease(&existing, worker_id, expected_revision, now_ms)?;
        let status = OutboxStatus::parse(next_status)
            .map_err(|error| memory::MemoryError::Store(error.to_string()))?;
        let row = transaction.query_one(
            "UPDATE session_mission_outbox SET status=$1, claim_owner=NULL, claim_expires_at_ms=NULL,
                    failure_class=$2, last_error=$3, revision=revision+1, updated_at_ms=$4
              WHERE request_id=$5 RETURNING
                    request_id,session_id,title,workspace_key,operation,status,attempts,next_attempt_at_ms,
                    claim_owner,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms",
            &[&next_status, &failure_class.map(OutboxFailureClass::as_str), &error, &now, &request_id],
        ).map_err(postgres_error)?;
        let record = row_to_mission_outbox(&row)?;
        append_mission_history_tx(
            &mut transaction,
            &record,
            action,
            Some(worker_id),
            Some(expected_revision),
            existing.status,
            status,
            error,
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    pub fn append_message_with_runtime_outbox(
        &self,
        message: &SessionMessage,
        request: &SessionRuntimeOutboxRequest,
    ) -> memory::store::Result<SessionRuntimeOutboxRecord> {
        validate_runtime_request(message, request)?;
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&message.session_id],
            )
            .map_err(postgres_error)?;
        if let Some(existing) = runtime_outbox_tx(&mut transaction, &request.request_id)? {
            if existing.turn_id == request.turn_id
                && existing.message_id == request.message_id
                && existing.session_id == message.session_id
                && existing.sequence == message.sequence
            {
                transaction.commit().map_err(postgres_error)?;
                return Ok(existing);
            }
            return Err(memory::MemoryError::Store(format!(
                "outbox request_id `{}` is already bound to another message",
                request.request_id
            )));
        }
        if message.stable_message_id != request.message_id {
            return Err(memory::MemoryError::Store(
                "runtime outbox message identity must equal stable message identity".to_string(),
            ));
        }
        insert_message_tx(&mut transaction, message)?;
        refresh_session_message_summary_tx(
            &mut transaction,
            &message.session_id,
            message.created_at_ms,
        )?;
        let record = insert_runtime_outbox_tx(&mut transaction, message, request)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    pub fn append_ingress_with_runtime_outbox(
        &self,
        session_id: &str,
        role: &str,
        content_json: Option<&str>,
        created_at_ms: u64,
        request: &SessionRuntimeOutboxRequest,
    ) -> memory::store::Result<SessionRuntimeOutboxRecord> {
        if session_id.trim().is_empty()
            || role.trim().is_empty()
            || request.request_id.trim().is_empty()
            || request.turn_id.trim().is_empty()
            || request.message_id.trim().is_empty()
        {
            return Err(memory::MemoryError::Store(
                "ingress outbox requires non-empty session, role and request identities"
                    .to_string(),
            ));
        }
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        if let Some(existing) = runtime_outbox_tx(&mut transaction, &request.request_id)? {
            if existing.session_id == session_id
                && existing.message_id == request.message_id
                && existing.turn_id == request.turn_id
            {
                transaction.commit().map_err(postgres_error)?;
                return Ok(existing);
            }
            return Err(memory::MemoryError::Store(format!(
                "outbox request `{}` conflicts with its committed ingress",
                request.request_id
            )));
        }
        let sequence: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(sequence), -1) + 1 FROM session_messages WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let content_json = content_json.ok_or_else(|| {
            memory::MemoryError::Store("ingress message requires content_json".to_string())
        })?;
        let message = SessionMessage {
            stable_message_id: request.message_id.clone(),
            session_id: session_id.to_string(),
            sequence: from_i64(sequence, "message sequence")?,
            role: role.to_string(),
            content_json: content_json.to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms,
        };
        insert_message_tx(&mut transaction, &message)?;
        refresh_session_message_summary_tx(&mut transaction, session_id, created_at_ms)?;
        let record = insert_runtime_outbox_tx(&mut transaction, &message, request)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    pub fn claim_session_runtime_outbox(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> memory::store::Result<Vec<SessionRuntimeOutboxRecord>> {
        if worker_id.trim().is_empty() || lease_ms == 0 || limit == 0 {
            return Err(memory::MemoryError::Store(
                "outbox claim requires worker_id, positive lease and positive limit".to_string(),
            ));
        }
        let now = to_u64_i64(now_ms, "runtime outbox clock")?;
        let expires = now
            .checked_add(to_u64_i64(lease_ms, "runtime outbox lease")?)
            .ok_or_else(|| {
                memory::MemoryError::Store("runtime outbox lease overflow".to_string())
            })?;
        let limit = to_i64(limit.min(500), "runtime outbox limit")?;
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let rows = transaction.query(
            "WITH candidates AS (
               SELECT request_id,status FROM session_runtime_outbox
                WHERE ((status IN ('pending','retry_scheduled') AND next_attempt_at_ms <= $1)
                   OR (status='claimed' AND claim_expires_at_ms <= $1))
                ORDER BY next_attempt_at_ms ASC, sequence ASC, request_id ASC
                FOR UPDATE SKIP LOCKED LIMIT $2
             ) UPDATE session_runtime_outbox o SET status='claimed', attempts=o.attempts+1,
                    claim_owner=$3, claim_expires_at_ms=$4, updated_at_ms=$1, revision=o.revision+1
               FROM candidates c WHERE o.request_id=c.request_id
             RETURNING o.request_id,o.turn_id,o.message_id,o.session_id,o.sequence,o.status,
                       o.runtime_commit_cursor,o.attempts,o.next_attempt_at_ms,o.claim_owner,
                       o.claim_expires_at_ms,o.failure_class,o.last_error,o.revision,o.created_at_ms,
                       o.updated_at_ms,o.runtime_options_json",
            &[&now,&limit,&worker_id,&expires],
        ).map_err(postgres_error)?;
        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let record = row_to_runtime_outbox(&row)?;
            append_runtime_history_tx(
                &mut transaction,
                &record,
                "claim",
                Some(worker_id),
                None,
                OutboxStatus::Pending,
                OutboxStatus::Claimed,
                None,
                now_ms,
            )?;
            claimed.push(record);
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(claimed)
    }

    pub fn ack_session_runtime_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        runtime_commit_cursor: u64,
        now_ms: u64,
    ) -> memory::store::Result<SessionRuntimeOutboxRecord> {
        self.transition_runtime_outbox(
            request_id,
            worker_id,
            expected_revision,
            now_ms,
            Some(runtime_commit_cursor),
            OutboxStatus::Materialized,
            None,
            None,
            None,
            "ack",
        )
    }

    pub fn renew_session_runtime_outbox_lease(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
        lease_ms: u64,
    ) -> memory::store::Result<SessionRuntimeOutboxRecord> {
        if lease_ms == 0 {
            return Err(memory::MemoryError::Store(
                "outbox lease renewal requires a positive lease".to_string(),
            ));
        }
        let now = to_u64_i64(now_ms, "runtime outbox clock")?;
        let expires = now
            .checked_add(to_u64_i64(lease_ms, "runtime outbox lease")?)
            .ok_or_else(|| {
                memory::MemoryError::Store("runtime outbox lease overflow".to_string())
            })?;
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let existing = runtime_outbox_for_update(&mut transaction, request_id)?;
        assert_runtime_lease(&existing, worker_id, expected_revision, now_ms)?;
        let row = transaction.query_one(
            "UPDATE session_runtime_outbox SET claim_expires_at_ms=$1,updated_at_ms=$2,revision=revision+1
              WHERE request_id=$3 RETURNING request_id,turn_id,message_id,session_id,sequence,status,
                runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_expires_at_ms,
                failure_class,last_error,revision,created_at_ms,updated_at_ms,runtime_options_json",
            &[&expires,&now,&request_id],
        ).map_err(postgres_error)?;
        let record = row_to_runtime_outbox(&row)?;
        append_runtime_history_tx(
            &mut transaction,
            &record,
            "renew_lease",
            Some(worker_id),
            Some(expected_revision),
            OutboxStatus::Claimed,
            OutboxStatus::Claimed,
            None,
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

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
    ) -> memory::store::Result<SessionRuntimeOutboxRecord> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let existing = runtime_outbox_for_update(&mut transaction, request_id)?;
        assert_runtime_lease(&existing, worker_id, expected_revision, now_ms)?;
        let retry = failure_class == OutboxFailureClass::Retryable
            && existing.attempts < max_attempts.max(1);
        let next = if retry {
            OutboxStatus::RetryScheduled
        } else {
            OutboxStatus::BlockedMaterialization
        };
        let now = to_u64_i64(now_ms, "runtime outbox clock")?;
        let retry_at = if retry {
            to_u64_i64(retry_at_ms, "runtime outbox retry")?
        } else {
            now
        };
        let row = transaction.query_one(
            "UPDATE session_runtime_outbox SET status=$1,next_attempt_at_ms=$2,claim_owner=NULL,
                    claim_expires_at_ms=NULL,failure_class=$3,last_error=$4,updated_at_ms=$5,
                    revision=revision+1 WHERE request_id=$6 RETURNING
                    request_id,turn_id,message_id,session_id,sequence,status,runtime_commit_cursor,
                    attempts,next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,
                    revision,created_at_ms,updated_at_ms,runtime_options_json",
            &[&next.as_str(),&retry_at,&failure_class.as_str(),&error,&now,&request_id],
        ).map_err(postgres_error)?;
        let record = row_to_runtime_outbox(&row)?;
        append_runtime_history_tx(
            &mut transaction,
            &record,
            if retry { "retry" } else { "block" },
            Some(worker_id),
            Some(expected_revision),
            existing.status,
            next,
            Some(error),
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    pub fn retry_blocked_session_runtime_outbox(
        &self,
        request_id: &str,
        expected_revision: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> memory::store::Result<SessionRuntimeOutboxRecord> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(memory::MemoryError::Store(
                "manual outbox retry requires actor and reason".to_string(),
            ));
        }
        let now = to_u64_i64(now_ms, "runtime outbox clock")?;
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let existing = runtime_outbox_for_update(&mut transaction, request_id)?;
        if existing.status != OutboxStatus::BlockedMaterialization
            || existing.revision != expected_revision
        {
            return Err(memory::MemoryError::Store(format!(
                "outbox `{request_id}` is not blocked at revision {expected_revision}"
            )));
        }
        let row = transaction.query_one(
            "UPDATE session_runtime_outbox SET status='pending',next_attempt_at_ms=$1,claim_owner=NULL,
                    claim_expires_at_ms=NULL,failure_class=NULL,updated_at_ms=$1,revision=revision+1
              WHERE request_id=$2 RETURNING request_id,turn_id,message_id,session_id,sequence,status,
                    runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_expires_at_ms,
                    failure_class,last_error,revision,created_at_ms,updated_at_ms,runtime_options_json",
            &[&now,&request_id],
        ).map_err(postgres_error)?;
        let record = row_to_runtime_outbox(&row)?;
        append_runtime_history_tx(
            &mut transaction,
            &record,
            "manual_retry",
            Some(actor),
            Some(expected_revision),
            OutboxStatus::BlockedMaterialization,
            OutboxStatus::Pending,
            Some(reason),
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    pub fn get_session_runtime_outbox(
        &self,
        request_id: &str,
    ) -> memory::store::Result<Option<SessionRuntimeOutboxRecord>> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .query_opt(RUNTIME_OUTBOX_SELECT, &[&request_id])
            .map_err(postgres_error)?
            .map(|row| row_to_runtime_outbox(&row))
            .transpose()
    }

    pub fn session_runtime_outbox_for_session(
        &self,
        session_id: &str,
        limit: usize,
    ) -> memory::store::Result<Vec<SessionRuntimeOutboxRecord>> {
        self.query_runtime_outbox(
            "SELECT request_id,turn_id,message_id,session_id,sequence,status,runtime_commit_cursor,attempts,
                    next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,revision,
                    created_at_ms,updated_at_ms,runtime_options_json FROM session_runtime_outbox
              WHERE session_id=$1 ORDER BY updated_at_ms DESC,sequence DESC,request_id DESC LIMIT $2",
            &[&session_id,&to_i64(limit.clamp(1,500), "runtime outbox limit")?],
        )
    }

    pub fn active_session_runtime_outbox(
        &self,
        limit: usize,
    ) -> memory::store::Result<Vec<SessionRuntimeOutboxRecord>> {
        self.query_runtime_outbox(
            "SELECT request_id,turn_id,message_id,session_id,sequence,status,runtime_commit_cursor,attempts,
                    next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,revision,
                    created_at_ms,updated_at_ms,runtime_options_json FROM session_runtime_outbox
              WHERE status != 'materialized' ORDER BY updated_at_ms DESC,sequence DESC,request_id DESC LIMIT $1",
            &[&to_i64(limit.clamp(1,500), "runtime outbox limit")?],
        )
    }

    pub fn blocked_session_runtime_outbox(
        &self,
        limit: usize,
    ) -> memory::store::Result<Vec<SessionRuntimeOutboxRecord>> {
        self.query_runtime_outbox(
            "SELECT request_id,turn_id,message_id,session_id,sequence,status,runtime_commit_cursor,attempts,
                    next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,revision,
                    created_at_ms,updated_at_ms,runtime_options_json FROM session_runtime_outbox
              WHERE status='blocked_materialization' ORDER BY updated_at_ms ASC,sequence ASC,request_id ASC LIMIT $1",
            &[&to_i64(limit.clamp(1,500), "runtime outbox limit")?],
        )
    }

    pub fn session_runtime_outbox_health(
        &self,
    ) -> memory::store::Result<SessionRuntimeOutboxHealth> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let rows = connection
            .query(
                "SELECT status,COUNT(*) FROM session_runtime_outbox GROUP BY status",
                &[],
            )
            .map_err(postgres_error)?;
        let mut health = SessionRuntimeOutboxHealth::default();
        for row in rows {
            let status: String = row.try_get(0).map_err(postgres_error)?;
            let count = from_i64(
                row.try_get(1).map_err(postgres_error)?,
                "runtime outbox count",
            )?;
            match OutboxStatus::parse(&status)
                .map_err(|error| memory::MemoryError::Store(error.to_string()))?
            {
                OutboxStatus::Pending => health.pending = count,
                OutboxStatus::Claimed => health.claimed = count,
                OutboxStatus::RetryScheduled => health.retry_scheduled = count,
                OutboxStatus::Materialized => health.materialized = count,
                OutboxStatus::BlockedMaterialization => health.blocked = count,
            }
        }
        Ok(health)
    }

    #[allow(clippy::too_many_arguments)]
    fn transition_runtime_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
        runtime_cursor: Option<u64>,
        next: OutboxStatus,
        failure_class: Option<OutboxFailureClass>,
        error: Option<&str>,
        next_attempt: Option<u64>,
        action: &str,
    ) -> memory::store::Result<SessionRuntimeOutboxRecord> {
        let now = to_u64_i64(now_ms, "runtime outbox clock")?;
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let existing = runtime_outbox_for_update(&mut transaction, request_id)?;
        assert_runtime_lease(&existing, worker_id, expected_revision, now_ms)?;
        let cursor = runtime_cursor
            .map(|value| to_u64_i64(value, "runtime cursor"))
            .transpose()?;
        let next_attempt = next_attempt
            .map(|value| to_u64_i64(value, "runtime next attempt"))
            .transpose()?
            .unwrap_or(existing.next_attempt_at_ms as i64);
        let row = transaction.query_one(
            "UPDATE session_runtime_outbox SET status=$1,runtime_commit_cursor=$2,next_attempt_at_ms=$3,
                    claim_owner=NULL,claim_expires_at_ms=NULL,failure_class=$4,last_error=$5,
                    updated_at_ms=$6,revision=revision+1 WHERE request_id=$7 RETURNING
                    request_id,turn_id,message_id,session_id,sequence,status,runtime_commit_cursor,
                    attempts,next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,
                    revision,created_at_ms,updated_at_ms,runtime_options_json",
            &[&next.as_str(),&cursor,&next_attempt,&failure_class.map(OutboxFailureClass::as_str),&error,&now,&request_id],
        ).map_err(postgres_error)?;
        let record = row_to_runtime_outbox(&row)?;
        append_runtime_history_tx(
            &mut transaction,
            &record,
            action,
            Some(worker_id),
            Some(expected_revision),
            existing.status,
            next,
            error,
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    /// Export every normalized PG table in canonical SQL order. This is a
    /// cutover-only API; normal request handling stays on the selected owner.
    pub fn export_migration_snapshot(&self) -> memory::store::Result<SessionMigrationSnapshot> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let sessions = connection.query("SELECT session_id,platform,chat_id,user_id,model,created_at,last_activity,message_count,reset_policy,metadata_json,input_tokens,output_tokens,estimated_cost_usd,status FROM session_records ORDER BY session_id", &[]).map_err(postgres_error)?.iter().map(row_to_session).collect::<memory::store::Result<_>>()?;
        let associations = connection.query("SELECT session_id,memory_id,created_at FROM session_memory_associations ORDER BY session_id,memory_id",&[]).map_err(postgres_error)?.iter().map(|row| Ok(SessionMemoryAssociation { session_id: row.try_get(0).map_err(postgres_error)?, memory_id: row.try_get(1).map_err(postgres_error)?, created_at: row.try_get(2).map_err(postgres_error)?})).collect::<memory::store::Result<_>>()?;
        let messages = connection.query("SELECT stable_message_id,session_id,sequence,role,content_json,blocks_count,tool_use_id,tool_name,token_usage_json,created_at_ms FROM session_messages ORDER BY session_id,sequence",&[]).map_err(postgres_error)?.iter().map(row_to_message).collect::<memory::store::Result<_>>()?;
        let events = connection.query("SELECT session_id,event_type,event_json,sequence,created_at_ms FROM session_events ORDER BY session_id,sequence",&[]).map_err(postgres_error)?.iter().map(row_to_event).collect::<memory::store::Result<_>>()?;
        let checkpoints = connection.query("SELECT session_id,checkpoint_id FROM session_event_checkpoints ORDER BY session_id,checkpoint_id",&[]).map_err(postgres_error)?.iter().map(|row| Ok(SessionEventCheckpoint {session_id: row.try_get(0).map_err(postgres_error)?,checkpoint_id: row.try_get(1).map_err(postgres_error)?})).collect::<memory::store::Result<_>>()?;
        let snapshots = connection.query("SELECT session_id,event_idx,messages_json,created_at_ms FROM session_snapshots ORDER BY session_id,event_idx",&[]).map_err(postgres_error)?.iter().map(row_to_snapshot).collect::<memory::store::Result<_>>()?;
        let runtime_outbox = connection.query("SELECT request_id,turn_id,message_id,session_id,sequence,status,runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms,runtime_options_json FROM session_runtime_outbox ORDER BY request_id",&[]).map_err(postgres_error)?.iter().map(row_to_runtime_outbox).collect::<memory::store::Result<_>>()?;
        let mission_outbox = connection.query("SELECT request_id,session_id,title,workspace_key,operation,status,attempts,next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms FROM session_mission_outbox ORDER BY request_id",&[]).map_err(postgres_error)?.iter().map(row_to_mission_outbox).collect::<memory::store::Result<_>>()?;
        let runtime_history = pg_history_rows(&mut connection, "session_runtime_outbox_history")?;
        let mission_history = pg_history_rows(&mut connection, "session_mission_outbox_history")?;
        Ok(SessionMigrationSnapshot {
            schema_version: 2,
            sessions,
            associations,
            messages,
            events,
            checkpoints,
            snapshots,
            runtime_outbox,
            mission_outbox,
            runtime_history,
            mission_history,
        })
    }

    /// Import only into an empty target or one already holding the identical
    /// snapshot. A conflicting nonempty target is refused; no dual write is
    /// introduced as a fallback.
    pub fn import_migration_snapshot(
        &self,
        snapshot: &SessionMigrationSnapshot,
    ) -> memory::store::Result<()> {
        if snapshot.schema_version != 2 {
            return Err(memory::MemoryError::Store(format!(
                "unsupported session migration schema {}",
                snapshot.schema_version
            )));
        }
        let existing = self.export_migration_snapshot()?;
        if !snapshot_is_empty(&existing) {
            if existing.canonical_digest()? == snapshot.canonical_digest()? {
                return Ok(());
            }
            return Err(memory::MemoryError::Store(
                "refusing divergent non-empty PostgreSQL session target".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        for session in &snapshot.sessions {
            upsert_session_tx(&mut transaction, session)?;
        }
        for association in &snapshot.associations {
            transaction.execute("INSERT INTO session_memory_associations(session_id,memory_id,created_at) VALUES($1,$2,$3)", &[&association.session_id,&association.memory_id,&association.created_at]).map_err(postgres_error)?;
        }
        for message in &snapshot.messages {
            insert_message_tx(&mut transaction, message)?;
        }
        for event in &snapshot.events {
            transaction.execute("INSERT INTO session_events(session_id,sequence,event_type,event_json,created_at_ms) VALUES($1,$2,$3,$4,$5)", &[&event.session_id,&to_i64(event.sequence,"event sequence")?,&event.event_type,&event.event_json,&to_u64_i64(event.created_at_ms,"event time")?]).map_err(postgres_error)?;
        }
        for checkpoint in &snapshot.checkpoints {
            transaction
                .execute(
                    "INSERT INTO session_event_checkpoints(session_id,checkpoint_id) VALUES($1,$2)",
                    &[&checkpoint.session_id, &checkpoint.checkpoint_id],
                )
                .map_err(postgres_error)?;
        }
        for item in &snapshot.snapshots {
            transaction.execute("INSERT INTO session_snapshots(session_id,event_idx,messages_json,created_at_ms) VALUES($1,$2,$3,$4)", &[&item.session_id,&to_i64(item.event_idx,"snapshot index")?,&item.messages_json,&to_u64_i64(item.created_at_ms,"snapshot time")?]).map_err(postgres_error)?;
        }
        for item in &snapshot.runtime_outbox {
            import_runtime_outbox_tx(&mut transaction, item)?;
        }
        for item in &snapshot.mission_outbox {
            import_mission_outbox_tx(&mut transaction, item)?;
        }
        for item in &snapshot.runtime_history {
            import_history_tx(&mut transaction, "session_runtime_outbox_history", item)?;
        }
        for item in &snapshot.mission_history {
            import_history_tx(&mut transaction, "session_mission_outbox_history", item)?;
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(())
    }

    fn query_sessions(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> memory::store::Result<Vec<SessionRecord>> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .query(statement, params)
            .map_err(postgres_error)?
            .iter()
            .map(row_to_session)
            .collect()
    }

    fn query_messages(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> memory::store::Result<Vec<SessionMessage>> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .query(statement, params)
            .map_err(postgres_error)?
            .iter()
            .map(row_to_message)
            .collect()
    }

    fn query_events(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> memory::store::Result<Vec<SessionEvent>> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .query(statement, params)
            .map_err(postgres_error)?
            .iter()
            .map(row_to_event)
            .collect()
    }

    fn query_runtime_outbox(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> memory::store::Result<Vec<SessionRuntimeOutboxRecord>> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        connection
            .query(statement, params)
            .map_err(postgres_error)?
            .iter()
            .map(row_to_runtime_outbox)
            .collect()
    }

    fn count_events_sql(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> memory::store::Result<usize> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let count: i64 = connection
            .query_one(statement, params)
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        from_i64(count, "event count")
    }

    fn delete_events_sql(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> memory::store::Result<usize> {
        let mut connection = self.executor.checkout_runtime().map_err(storage_error)?;
        let deleted = connection
            .execute(statement, params)
            .map_err(postgres_error)?;
        Ok(deleted as usize)
    }
}

/// Migration 0003 predates tolerant usage parsing and its checksum is already
/// part of the production migration ledger, so it cannot be rewritten.
/// Quarantine only unusable legacy usage payloads before that migration runs.
/// Valid JSON with bounded numeric token fields is preserved byte-for-byte.
fn prepare_legacy_session_usage_for_migration(
    executor: &PostgresExecutor,
) -> memory::store::Result<()> {
    let mut connection = executor.checkout_runtime().map_err(storage_error)?;
    let mut transaction = connection.transaction().map_err(postgres_error)?;
    // This compatibility preflight creates helper functions before the
    // immutable migration ledger can run migration 0003. It must share the
    // exact domain lock used by `PostgresExecutor::apply_migrations`;
    // otherwise concurrent Gateway processes can both enter PostgreSQL's
    // `CREATE OR REPLACE FUNCTION` catalogue path and one fails on
    // `pg_proc_proname_args_nsp_index`.
    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtext($1))",
            &[&format!("cowd-storage:{SESSION_DOMAIN}")],
        )
        .map_err(postgres_error)?;
    transaction
        .batch_execute(
            "CREATE OR REPLACE FUNCTION cowd_session_usage_json_is_reconcilable(raw TEXT)
             RETURNS BOOLEAN
             LANGUAGE plpgsql
             IMMUTABLE
             STRICT
             AS $$
             DECLARE
                 parsed JSONB;
                 token_text TEXT;
                 token_key TEXT;
             BEGIN
                 parsed := raw::jsonb;
                 FOREACH token_key IN ARRAY ARRAY['input_tokens', 'output_tokens'] LOOP
                     token_text := parsed ->> token_key;
                     IF token_text IS NOT NULL
                        AND (jsonb_typeof(parsed -> token_key) <> 'number'
                             OR token_text !~ '^[0-9]+$'
                             OR token_text::numeric > 9223372036854775807::numeric) THEN
                         RETURN FALSE;
                     END IF;
                 END LOOP;
                 RETURN TRUE;
             EXCEPTION WHEN OTHERS THEN
                 RETURN FALSE;
             END
             $$;
             CREATE OR REPLACE FUNCTION cowd_session_usage_json_for_legacy_cast(raw TEXT)
             RETURNS TEXT
             LANGUAGE plpgsql
             IMMUTABLE
             STRICT
             AS $$
             DECLARE
                 parsed JSONB;
                 token_text TEXT;
                 token_key TEXT;
                 token_value BIGINT;
             BEGIN
                 BEGIN
                     parsed := raw::jsonb;
                 EXCEPTION WHEN OTHERS THEN
                     parsed := '{}'::jsonb;
                 END;
                 FOREACH token_key IN ARRAY ARRAY['input_tokens', 'output_tokens'] LOOP
                     token_text := parsed ->> token_key;
                     token_value := 0;
                     IF jsonb_typeof(parsed -> token_key) = 'number'
                        AND token_text ~ '^[0-9]+$'
                        AND token_text::numeric <= 9223372036854775807::numeric THEN
                         token_value := token_text::bigint;
                     END IF;
                     parsed := jsonb_set(
                         parsed,
                         ARRAY[token_key],
                         to_jsonb(token_value),
                         TRUE
                     );
                 END LOOP;
                 RETURN parsed::text;
             END
             $$;
             DO $$
             DECLARE
                 migration_applied BOOLEAN := FALSE;
             BEGIN
                 IF to_regclass('cowd_schema_migrations') IS NOT NULL THEN
                     EXECUTE
                         'SELECT EXISTS(
                              SELECT 1
                                FROM cowd_schema_migrations
                               WHERE id = $1
                          )'
                        INTO migration_applied
                       USING 'session.0003.reconcile-message-summaries';
                 END IF;
                 IF to_regclass('session_messages') IS NOT NULL
                    AND NOT migration_applied THEN
                     UPDATE session_messages
                        SET token_usage_json =
                            cowd_session_usage_json_for_legacy_cast(token_usage_json)
                      WHERE token_usage_json IS NOT NULL
                        AND NOT cowd_session_usage_json_is_reconcilable(token_usage_json);
                 END IF;
             END
             $$;",
        )
        .map_err(postgres_error)?;
    transaction.commit().map_err(postgres_error)?;
    Ok(())
}

const SESSION_SELECT_BY_ID: &str =
    "SELECT session_id, platform, chat_id, user_id, model, created_at,
    last_activity, message_count, reset_policy, metadata_json, input_tokens, output_tokens,
    estimated_cost_usd, status FROM session_records WHERE session_id=$1";

const MISSION_OUTBOX_SELECT: &str =
    "SELECT request_id,session_id,title,workspace_key,operation,status,attempts,next_attempt_at_ms,
            claim_owner,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms
       FROM session_mission_outbox WHERE request_id=$1";

const RUNTIME_OUTBOX_SELECT: &str =
    "SELECT request_id,turn_id,message_id,session_id,sequence,status,runtime_commit_cursor,attempts,
            next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,revision,
            created_at_ms,updated_at_ms,runtime_options_json
       FROM session_runtime_outbox WHERE request_id=$1";

fn session_params(session: &SessionRecord) -> [&(dyn ToSql + Sync); 14] {
    [
        &session.session_id,
        &session.platform,
        &session.chat_id,
        &session.user_id,
        &session.model,
        &session.created_at,
        &session.last_activity,
        &session.message_count,
        &session.reset_policy,
        &session.metadata_json,
        &session.input_tokens,
        &session.output_tokens,
        &session.estimated_cost_usd,
        &session.status,
    ]
}

fn upsert_session_tx(
    transaction: &mut PostgresTransaction<'_>,
    session: &SessionRecord,
) -> memory::store::Result<()> {
    transaction.execute(
        "INSERT INTO session_records(
            session_id,platform,chat_id,user_id,model,created_at,last_activity,message_count,
            reset_policy,metadata_json,input_tokens,output_tokens,estimated_cost_usd,status,
            created_at_ms,updated_at_ms
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
            cowd_safe_session_epoch_ms($6),cowd_safe_session_epoch_ms($7))
         ON CONFLICT(session_id) DO UPDATE SET
            platform=EXCLUDED.platform,chat_id=EXCLUDED.chat_id,user_id=EXCLUDED.user_id,
            model=EXCLUDED.model,created_at=EXCLUDED.created_at,last_activity=EXCLUDED.last_activity,
            message_count=EXCLUDED.message_count,reset_policy=EXCLUDED.reset_policy,
            metadata_json=EXCLUDED.metadata_json,input_tokens=EXCLUDED.input_tokens,
            output_tokens=EXCLUDED.output_tokens,estimated_cost_usd=EXCLUDED.estimated_cost_usd,
            status=EXCLUDED.status,created_at_ms=EXCLUDED.created_at_ms,
            updated_at_ms=EXCLUDED.updated_at_ms",
        &session_params(session),
    ).map_err(postgres_error)?;
    Ok(())
}

fn validate_mission_request(request: &SessionMissionOutboxRequest) -> memory::store::Result<()> {
    if request.request_id.trim().is_empty()
        || request.session_id.trim().is_empty()
        || request.title.trim().is_empty()
        || request.workspace_key.trim().is_empty()
    {
        return Err(memory::MemoryError::Store(
            "mission outbox requires non-empty request, session, title and workspace identities"
                .to_string(),
        ));
    }
    Ok(())
}

fn insert_mission_outbox_tx(
    transaction: &mut PostgresTransaction<'_>,
    request: &SessionMissionOutboxRequest,
) -> memory::store::Result<SessionMissionOutboxRecord> {
    if let Some(row) = transaction
        .query_opt(MISSION_OUTBOX_SELECT, &[&request.request_id])
        .map_err(postgres_error)?
    {
        let existing = row_to_mission_outbox(&row)?;
        if existing.session_id == request.session_id
            && existing.title == request.title
            && existing.workspace_key == request.workspace_key
            && existing.operation == request.operation
        {
            return Ok(existing);
        }
        return Err(memory::MemoryError::Store(format!(
            "mission outbox request_id `{}` is already bound to another intent",
            request.request_id
        )));
    }
    let now = to_u64_i64(request.created_at_ms, "mission outbox time")?;
    let row = transaction.query_one(
        "INSERT INTO session_mission_outbox(
             request_id,session_id,title,workspace_key,operation,status,attempts,next_attempt_at_ms,
             revision,created_at_ms,updated_at_ms
         ) VALUES($1,$2,$3,$4,$5,'pending',0,$6,0,$6,$6)
         RETURNING request_id,session_id,title,workspace_key,operation,status,attempts,next_attempt_at_ms,
                   claim_owner,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms",
        &[&request.request_id,&request.session_id,&request.title,&request.workspace_key,
          &request.operation.as_str(),&now],
    ).map_err(postgres_error)?;
    let record = row_to_mission_outbox(&row)?;
    append_mission_history_tx(
        transaction,
        &record,
        "enqueue",
        None,
        None,
        OutboxStatus::Pending,
        OutboxStatus::Pending,
        None,
        request.created_at_ms,
    )?;
    Ok(record)
}

fn row_to_mission_outbox(row: &Row) -> memory::store::Result<SessionMissionOutboxRecord> {
    let operation: String = row.try_get(4).map_err(postgres_error)?;
    let status: String = row.try_get(5).map_err(postgres_error)?;
    let failure: Option<String> = row.try_get(11).map_err(postgres_error)?;
    Ok(SessionMissionOutboxRecord {
        request_id: row.try_get(0).map_err(postgres_error)?,
        session_id: row.try_get(1).map_err(postgres_error)?,
        title: row.try_get(2).map_err(postgres_error)?,
        workspace_key: row.try_get(3).map_err(postgres_error)?,
        operation: SessionMissionOutboxOperation::parse(&operation)
            .map_err(|error| memory::MemoryError::Store(error.to_string()))?,
        status: OutboxStatus::parse(&status)
            .map_err(|error| memory::MemoryError::Store(error.to_string()))?,
        attempts: i64_to_u32(row.try_get(6).map_err(postgres_error)?, "mission attempts")?,
        next_attempt_at_ms: i64_to_u64(
            row.try_get(7).map_err(postgres_error)?,
            "mission next attempt",
        )?,
        claim_owner: row.try_get(8).map_err(postgres_error)?,
        claim_expires_at_ms: row
            .try_get::<_, Option<i64>>(9)
            .map_err(postgres_error)?
            .map(|value| i64_to_u64(value, "mission lease"))
            .transpose()?,
        failure_class: failure
            .map(|value| {
                OutboxFailureClass::parse(&value)
                    .map_err(|error| memory::MemoryError::Store(error.to_string()))
            })
            .transpose()?,
        last_error: row.try_get(12).map_err(postgres_error)?,
        revision: i64_to_u64(row.try_get(13).map_err(postgres_error)?, "mission revision")?,
        created_at_ms: i64_to_u64(
            row.try_get(14).map_err(postgres_error)?,
            "mission created time",
        )?,
        updated_at_ms: i64_to_u64(
            row.try_get(15).map_err(postgres_error)?,
            "mission updated time",
        )?,
    })
}

fn validate_runtime_request(
    message: &SessionMessage,
    request: &SessionRuntimeOutboxRequest,
) -> memory::store::Result<()> {
    if request.request_id.trim().is_empty()
        || request.turn_id.trim().is_empty()
        || request.message_id.trim().is_empty()
        || message.session_id.trim().is_empty()
    {
        return Err(memory::MemoryError::Store(
            "runtime outbox requires non-empty request, turn, message and session identities"
                .to_string(),
        ));
    }
    Ok(())
}

fn insert_runtime_outbox_tx(
    transaction: &mut PostgresTransaction<'_>,
    message: &SessionMessage,
    request: &SessionRuntimeOutboxRequest,
) -> memory::store::Result<SessionRuntimeOutboxRecord> {
    let now = to_u64_i64(request.created_at_ms, "runtime outbox time")?;
    let row = transaction.query_one(
        "INSERT INTO session_runtime_outbox(
             request_id,turn_id,message_id,session_id,sequence,status,attempts,next_attempt_at_ms,
             revision,created_at_ms,updated_at_ms,runtime_options_json
         ) VALUES($1,$2,$3,$4,$5,'pending',0,$6,0,$6,$6,$7)
         RETURNING request_id,turn_id,message_id,session_id,sequence,status,runtime_commit_cursor,
                   attempts,next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,
                   revision,created_at_ms,updated_at_ms,runtime_options_json",
        &[&request.request_id,&request.turn_id,&request.message_id,&message.session_id,
          &to_i64(message.sequence, "message sequence")?,&now,&request.runtime_options_json],
    ).map_err(postgres_error)?;
    let record = row_to_runtime_outbox(&row)?;
    append_runtime_history_tx(
        transaction,
        &record,
        "enqueue",
        None,
        None,
        OutboxStatus::Pending,
        OutboxStatus::Pending,
        None,
        request.created_at_ms,
    )?;
    Ok(record)
}

fn row_to_runtime_outbox(row: &Row) -> memory::store::Result<SessionRuntimeOutboxRecord> {
    let status: String = row.try_get(5).map_err(postgres_error)?;
    let failure: Option<String> = row.try_get(11).map_err(postgres_error)?;
    Ok(SessionRuntimeOutboxRecord {
        request_id: row.try_get(0).map_err(postgres_error)?,
        turn_id: row.try_get(1).map_err(postgres_error)?,
        message_id: row.try_get(2).map_err(postgres_error)?,
        session_id: row.try_get(3).map_err(postgres_error)?,
        sequence: from_i64(
            row.try_get(4).map_err(postgres_error)?,
            "runtime message sequence",
        )?,
        status: OutboxStatus::parse(&status)
            .map_err(|error| memory::MemoryError::Store(error.to_string()))?,
        runtime_commit_cursor: row
            .try_get::<_, Option<i64>>(6)
            .map_err(postgres_error)?
            .map(|value| i64_to_u64(value, "runtime cursor"))
            .transpose()?,
        attempts: i64_to_u32(row.try_get(7).map_err(postgres_error)?, "runtime attempts")?,
        next_attempt_at_ms: i64_to_u64(
            row.try_get(8).map_err(postgres_error)?,
            "runtime next attempt",
        )?,
        claim_owner: row.try_get(9).map_err(postgres_error)?,
        claim_expires_at_ms: row
            .try_get::<_, Option<i64>>(10)
            .map_err(postgres_error)?
            .map(|value| i64_to_u64(value, "runtime lease"))
            .transpose()?,
        failure_class: failure
            .map(|value| {
                OutboxFailureClass::parse(&value)
                    .map_err(|error| memory::MemoryError::Store(error.to_string()))
            })
            .transpose()?,
        last_error: row.try_get(12).map_err(postgres_error)?,
        revision: i64_to_u64(row.try_get(13).map_err(postgres_error)?, "runtime revision")?,
        created_at_ms: i64_to_u64(
            row.try_get(14).map_err(postgres_error)?,
            "runtime created time",
        )?,
        updated_at_ms: i64_to_u64(
            row.try_get(15).map_err(postgres_error)?,
            "runtime updated time",
        )?,
        runtime_options_json: row.try_get(16).map_err(postgres_error)?,
    })
}

fn pg_history_rows(
    connection: &mut PostgresConnection,
    table: &str,
) -> memory::store::Result<Vec<SessionOutboxHistory>> {
    debug_assert!(matches!(
        table,
        "session_runtime_outbox_history" | "session_mission_outbox_history"
    ));
    connection.query(
        &format!("SELECT request_id,action,actor,COALESCE(reason,detail),COALESCE(from_status,previous_status),COALESCE(to_status,next_status),attempts,created_at_ms FROM {table} ORDER BY history_id"),
        &[],
    ).map_err(postgres_error)?.iter().map(|row| Ok(SessionOutboxHistory {
        request_id: row.try_get(0).map_err(postgres_error)?, action: row.try_get(1).map_err(postgres_error)?,
        actor: row.try_get(2).map_err(postgres_error)?, reason: row.try_get(3).map_err(postgres_error)?,
        from_status: row.try_get(4).map_err(postgres_error)?, to_status: row.try_get(5).map_err(postgres_error)?,
        attempts: i64_to_u32(row.try_get(6).map_err(postgres_error)?,"history attempts")?,
        created_at_ms: i64_to_u64(row.try_get(7).map_err(postgres_error)?,"history time")?,
    })).collect()
}

fn snapshot_is_empty(snapshot: &SessionMigrationSnapshot) -> bool {
    snapshot.sessions.is_empty()
        && snapshot.associations.is_empty()
        && snapshot.messages.is_empty()
        && snapshot.events.is_empty()
        && snapshot.checkpoints.is_empty()
        && snapshot.snapshots.is_empty()
        && snapshot.runtime_outbox.is_empty()
        && snapshot.mission_outbox.is_empty()
        && snapshot.runtime_history.is_empty()
        && snapshot.mission_history.is_empty()
}

fn import_runtime_outbox_tx(
    transaction: &mut PostgresTransaction<'_>,
    item: &SessionRuntimeOutboxRecord,
) -> memory::store::Result<()> {
    transaction.execute(
        "INSERT INTO session_runtime_outbox(request_id,turn_id,message_id,session_id,sequence,status,runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms,runtime_options_json)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
        &[&item.request_id,&item.turn_id,&item.message_id,&item.session_id,&to_i64(item.sequence,"runtime sequence")?,&item.status.as_str(),
          &item.runtime_commit_cursor.map(|value| to_u64_i64(value,"runtime cursor")).transpose()?,&to_i64(item.attempts as usize,"runtime attempts")?,&to_u64_i64(item.next_attempt_at_ms,"runtime next")?,&item.claim_owner,&item.claim_expires_at_ms.map(|value|to_u64_i64(value,"runtime lease")).transpose()?,&item.failure_class.map(OutboxFailureClass::as_str),&item.last_error,&to_u64_i64(item.revision,"runtime revision")?,&to_u64_i64(item.created_at_ms,"runtime created")?,&to_u64_i64(item.updated_at_ms,"runtime updated")?,&item.runtime_options_json],
    ).map_err(postgres_error)?;
    Ok(())
}

fn import_mission_outbox_tx(
    transaction: &mut PostgresTransaction<'_>,
    item: &SessionMissionOutboxRecord,
) -> memory::store::Result<()> {
    transaction.execute(
        "INSERT INTO session_mission_outbox(request_id,session_id,title,workspace_key,operation,status,attempts,next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        &[&item.request_id,&item.session_id,&item.title,&item.workspace_key,&item.operation.as_str(),&item.status.as_str(),&to_i64(item.attempts as usize,"mission attempts")?,&to_u64_i64(item.next_attempt_at_ms,"mission next")?,&item.claim_owner,&item.claim_expires_at_ms.map(|value|to_u64_i64(value,"mission lease")).transpose()?,&item.failure_class.map(OutboxFailureClass::as_str),&item.last_error,&to_u64_i64(item.revision,"mission revision")?,&to_u64_i64(item.created_at_ms,"mission created")?,&to_u64_i64(item.updated_at_ms,"mission updated")?],
    ).map_err(postgres_error)?;
    Ok(())
}

fn import_history_tx(
    transaction: &mut PostgresTransaction<'_>,
    table: &str,
    item: &SessionOutboxHistory,
) -> memory::store::Result<()> {
    debug_assert!(matches!(
        table,
        "session_runtime_outbox_history" | "session_mission_outbox_history"
    ));
    transaction.execute(
        &format!("INSERT INTO {table}(request_id,action,actor,previous_status,next_status,detail,reason,from_status,to_status,attempts,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,$6,$4,$5,$7,$8)"),
        &[&item.request_id,&item.action,&item.actor,&item.from_status,&item.to_status,&item.reason,&to_i64(item.attempts as usize,"history attempts")?,&to_u64_i64(item.created_at_ms,"history time")?],
    ).map_err(postgres_error)?;
    Ok(())
}

/// Quiesced SQLite-to-PG copy with source/target digest proof and an atomic,
/// redacted manifest. It never changes selected topology itself.
pub fn copy_quiesced_session_store(
    source: &SqliteSessionStore,
    target: &PostgresSessionStore,
    manifest_path: impl AsRef<Path>,
) -> memory::store::Result<SessionMigrationManifest> {
    let snapshot = export_sqlite_session_snapshot(source)?;
    let source_digest = snapshot.canonical_digest()?;
    target.import_migration_snapshot(&snapshot)?;
    if export_sqlite_session_snapshot(source)?.canonical_digest()? != source_digest {
        return Err(memory::MemoryError::Store(
            "session SQLite source changed during quiesced copy".to_string(),
        ));
    }
    let target_digest = target.export_migration_snapshot()?.canonical_digest()?;
    if target_digest != source_digest {
        return Err(memory::MemoryError::Store(
            "session PostgreSQL target digest differs from source".to_string(),
        ));
    }
    let manifest = SessionMigrationManifest {
        domain: SESSION_DOMAIN.to_string(),
        source_digest,
        target_digest,
        schema_version: snapshot.schema_version,
        session_count: snapshot.sessions.len(),
        message_count: snapshot.messages.len(),
        event_count: snapshot.events.len(),
    };
    let path = manifest_path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| memory::MemoryError::Store(error.to_string()))?;
    }
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(&manifest)
            .map_err(|error| memory::MemoryError::Store(error.to_string()))?,
    )
    .map_err(|error| memory::MemoryError::Store(error.to_string()))?;
    fs::rename(&temporary, path).map_err(|error| memory::MemoryError::Store(error.to_string()))?;
    Ok(manifest)
}

fn runtime_outbox_tx(
    transaction: &mut PostgresTransaction<'_>,
    request_id: &str,
) -> memory::store::Result<Option<SessionRuntimeOutboxRecord>> {
    transaction
        .query_opt(RUNTIME_OUTBOX_SELECT, &[&request_id])
        .map_err(postgres_error)?
        .map(|row| row_to_runtime_outbox(&row))
        .transpose()
}

fn runtime_outbox_for_update(
    transaction: &mut PostgresTransaction<'_>,
    request_id: &str,
) -> memory::store::Result<SessionRuntimeOutboxRecord> {
    transaction.query_opt(
        "SELECT request_id,turn_id,message_id,session_id,sequence,status,runtime_commit_cursor,attempts,
                next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,revision,
                created_at_ms,updated_at_ms,runtime_options_json FROM session_runtime_outbox
          WHERE request_id=$1 FOR UPDATE", &[&request_id],
    ).map_err(postgres_error)?.map(|row| row_to_runtime_outbox(&row)).transpose()?
        .ok_or_else(|| memory::MemoryError::Store(format!("session runtime outbox `{request_id}` not found")))
}

fn assert_runtime_lease(
    record: &SessionRuntimeOutboxRecord,
    worker_id: &str,
    expected_revision: u64,
    now_ms: u64,
) -> memory::store::Result<()> {
    if record.status != OutboxStatus::Claimed
        || record.claim_owner.as_deref() != Some(worker_id)
        || record.revision != expected_revision
        || record
            .claim_expires_at_ms
            .is_none_or(|expires| expires < now_ms)
    {
        return Err(memory::MemoryError::Store(
            "runtime outbox transition rejected by lease/revision fencing".to_string(),
        ));
    }
    Ok(())
}

fn append_runtime_history_tx(
    transaction: &mut PostgresTransaction<'_>,
    record: &SessionRuntimeOutboxRecord,
    action: &str,
    actor: Option<&str>,
    expected_revision: Option<u64>,
    previous_status: OutboxStatus,
    next_status: OutboxStatus,
    detail: Option<&str>,
    created_at_ms: u64,
) -> memory::store::Result<()> {
    transaction
        .execute(
            "INSERT INTO session_runtime_outbox_history(
            request_id,action,actor,expected_revision,previous_status,next_status,detail,
            reason,from_status,to_status,attempts,created_at_ms
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$7,$5,$6,$8,$9)",
            &[
                &record.request_id,
                &action,
                &actor,
                &expected_revision
                    .map(|value| to_u64_i64(value, "expected revision"))
                    .transpose()?,
                &previous_status.as_str(),
                &next_status.as_str(),
                &detail,
                &to_i64(record.attempts as usize, "runtime history attempts")?,
                &to_u64_i64(created_at_ms, "runtime history time")?,
            ],
        )
        .map_err(postgres_error)?;
    Ok(())
}

fn mission_outbox_for_update(
    transaction: &mut PostgresTransaction<'_>,
    request_id: &str,
) -> memory::store::Result<SessionMissionOutboxRecord> {
    transaction.query_opt(
        "SELECT request_id,session_id,title,workspace_key,operation,status,attempts,next_attempt_at_ms,
                claim_owner,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms
           FROM session_mission_outbox WHERE request_id=$1 FOR UPDATE",
        &[&request_id],
    ).map_err(postgres_error)?.map(|row| row_to_mission_outbox(&row)).transpose()?
        .ok_or_else(|| memory::MemoryError::Store(format!("mission outbox `{request_id}` was not found")))
}

fn assert_mission_lease(
    record: &SessionMissionOutboxRecord,
    worker_id: &str,
    expected_revision: u64,
    now_ms: u64,
) -> memory::store::Result<()> {
    if record.status != OutboxStatus::Claimed
        || record.claim_owner.as_deref() != Some(worker_id)
        || record.revision != expected_revision
        || record
            .claim_expires_at_ms
            .is_none_or(|expires| expires < now_ms)
    {
        return Err(memory::MemoryError::Store(
            "mission outbox transition rejected by lease/revision fencing".to_string(),
        ));
    }
    Ok(())
}

fn append_mission_history_tx(
    transaction: &mut PostgresTransaction<'_>,
    record: &SessionMissionOutboxRecord,
    action: &str,
    actor: Option<&str>,
    expected_revision: Option<u64>,
    previous_status: OutboxStatus,
    next_status: OutboxStatus,
    detail: Option<&str>,
    created_at_ms: u64,
) -> memory::store::Result<()> {
    transaction
        .execute(
            "INSERT INTO session_mission_outbox_history(
            request_id,action,actor,expected_revision,previous_status,next_status,detail,
            reason,from_status,to_status,attempts,created_at_ms
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$7,$5,$6,$8,$9)",
            &[
                &record.request_id,
                &action,
                &actor,
                &expected_revision
                    .map(|value| to_u64_i64(value, "expected revision"))
                    .transpose()?,
                &previous_status.as_str(),
                &next_status.as_str(),
                &detail,
                &to_i64(record.attempts as usize, "mission history attempts")?,
                &to_u64_i64(created_at_ms, "mission history time")?,
            ],
        )
        .map_err(postgres_error)?;
    Ok(())
}

fn row_to_session(row: &Row) -> memory::store::Result<SessionRecord> {
    Ok(SessionRecord {
        session_id: row.try_get(0).map_err(postgres_error)?,
        platform: row.try_get(1).map_err(postgres_error)?,
        chat_id: row.try_get(2).map_err(postgres_error)?,
        user_id: row.try_get(3).map_err(postgres_error)?,
        model: row.try_get(4).map_err(postgres_error)?,
        created_at: row.try_get(5).map_err(postgres_error)?,
        last_activity: row.try_get(6).map_err(postgres_error)?,
        message_count: row.try_get(7).map_err(postgres_error)?,
        reset_policy: row.try_get(8).map_err(postgres_error)?,
        metadata_json: row.try_get(9).map_err(postgres_error)?,
        input_tokens: row.try_get(10).map_err(postgres_error)?,
        output_tokens: row.try_get(11).map_err(postgres_error)?,
        estimated_cost_usd: row.try_get(12).map_err(postgres_error)?,
        status: row.try_get(13).map_err(postgres_error)?,
    })
}

fn row_to_recovery_manifest(row: &Row) -> memory::store::Result<SessionRecoveryManifest> {
    Ok(SessionRecoveryManifest {
        session_id: row.try_get(0).map_err(postgres_error)?,
        durable_cursor: i64_to_u64(
            row.try_get(1).map_err(postgres_error)?,
            "recovery durable cursor",
        )?,
        history_revision: i64_to_u64(
            row.try_get(2).map_err(postgres_error)?,
            "recovery history revision",
        )?,
        transcript_messages: i64_to_u64(
            row.try_get(3).map_err(postgres_error)?,
            "recovery transcript messages",
        )?,
        transcript_bytes: i64_to_u64(
            row.try_get(4).map_err(postgres_error)?,
            "recovery transcript bytes",
        )?,
        in_flight_turn: row.try_get(5).map_err(postgres_error)?,
        pending_approval: row.try_get(6).map_err(postgres_error)?,
        active_writer_or_attachment: row.try_get(7).map_err(postgres_error)?,
        mission_agent_team_continuation: row.try_get(8).map_err(postgres_error)?,
        last_activity_ms: i64_to_u64(
            row.try_get(9).map_err(postgres_error)?,
            "recovery last activity",
        )?,
        manifest_revision: i64_to_u64(
            row.try_get(10).map_err(postgres_error)?,
            "recovery manifest revision",
        )?,
    })
}

fn refresh_session_message_summary_tx(
    transaction: &mut PostgresTransaction<'_>,
    session_id: &str,
    activity_ms: u64,
) -> memory::store::Result<()> {
    let activity = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(to_u64_i64(
        activity_ms,
        "session activity time",
    )?)
    .unwrap_or_else(chrono::Utc::now)
    .to_rfc3339();
    let activity_ms = to_u64_i64(activity_ms, "session activity time")?;
    transaction
        .execute(
            "UPDATE session_records
                SET message_count = (
                        SELECT COUNT(*) FROM session_messages WHERE session_id=$1
                    ),
                    last_activity = CASE
                        WHEN updated_at_ms <= $3 THEN $2
                        ELSE last_activity
                    END,
                    updated_at_ms = GREATEST(updated_at_ms, $3)
              WHERE session_id=$1",
            &[&session_id, &activity, &activity_ms],
        )
        .map_err(postgres_error)?;
    Ok(())
}

fn refresh_session_usage_summary_tx(
    transaction: &mut PostgresTransaction<'_>,
    session_id: &str,
) -> memory::store::Result<()> {
    transaction
        .execute(
            "UPDATE session_records
                SET input_tokens = COALESCE((
                        SELECT SUM(cowd_safe_session_usage_token(
                            token_usage_json, 'input_tokens'
                        ))
                          FROM session_messages WHERE session_id=$1
                    ), 0),
                    output_tokens = COALESCE((
                        SELECT SUM(cowd_safe_session_usage_token(
                            token_usage_json, 'output_tokens'
                        ))
                          FROM session_messages WHERE session_id=$1
                    ), 0)
              WHERE session_id=$1",
            &[&session_id],
        )
        .map_err(postgres_error)?;
    Ok(())
}

fn insert_message_tx(
    transaction: &mut PostgresTransaction<'_>,
    message: &SessionMessage,
) -> memory::store::Result<()> {
    let stable_message_id = if message.stable_message_id.trim().is_empty() {
        format!("legacy:{}:{}", message.session_id, message.sequence)
    } else {
        message.stable_message_id.clone()
    };
    transaction
        .execute(
            "INSERT INTO session_messages(
            stable_message_id, session_id, sequence, role, content_json, blocks_count,
            tool_use_id, tool_name, token_usage_json, created_at_ms
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
         ON CONFLICT(session_id, sequence) DO UPDATE SET
            role=EXCLUDED.role, content_json=EXCLUDED.content_json,
            blocks_count=EXCLUDED.blocks_count, tool_use_id=EXCLUDED.tool_use_id,
            tool_name=EXCLUDED.tool_name, token_usage_json=EXCLUDED.token_usage_json,
            created_at_ms=EXCLUDED.created_at_ms",
            &[
                &stable_message_id,
                &message.session_id,
                &to_i64(message.sequence, "message sequence")?,
                &message.role,
                &message.content_json,
                &to_i64(message.blocks_count, "message blocks")?,
                &message.tool_use_id,
                &message.tool_name,
                &message.token_usage_json,
                &to_u64_i64(message.created_at_ms, "message time")?,
            ],
        )
        .map_err(postgres_error)?;
    Ok(())
}

fn row_to_session_search(row: &Row) -> memory::store::Result<SessionSearchResult> {
    Ok(SessionSearchResult {
        session_id: row.try_get(0).map_err(postgres_error)?,
        platform: row.try_get(1).map_err(postgres_error)?,
        chat_id: row.try_get(2).map_err(postgres_error)?,
        user_id: row.try_get(3).map_err(postgres_error)?,
        created_at: row.try_get(4).map_err(postgres_error)?,
        last_activity: row.try_get(5).map_err(postgres_error)?,
        message_count: row.try_get(6).map_err(postgres_error)?,
        snippet: row.try_get(7).map_err(postgres_error)?,
    })
}

fn row_to_message(row: &Row) -> memory::store::Result<SessionMessage> {
    Ok(SessionMessage {
        stable_message_id: row.try_get(0).map_err(postgres_error)?,
        session_id: row.try_get(1).map_err(postgres_error)?,
        sequence: from_i64(row.try_get(2).map_err(postgres_error)?, "message sequence")?,
        role: row.try_get(3).map_err(postgres_error)?,
        content_json: row.try_get(4).map_err(postgres_error)?,
        blocks_count: from_i64(row.try_get(5).map_err(postgres_error)?, "message blocks")?,
        tool_use_id: row.try_get(6).map_err(postgres_error)?,
        tool_name: row.try_get(7).map_err(postgres_error)?,
        token_usage_json: row.try_get(8).map_err(postgres_error)?,
        created_at_ms: u64::try_from(row.try_get::<_, i64>(9).map_err(postgres_error)?)
            .map_err(|_| memory::MemoryError::Store("message time overflow".to_string()))?,
    })
}

fn row_to_event(row: &Row) -> memory::store::Result<SessionEvent> {
    Ok(SessionEvent {
        session_id: row.try_get(0).map_err(postgres_error)?,
        event_type: row.try_get(1).map_err(postgres_error)?,
        event_json: row.try_get(2).map_err(postgres_error)?,
        sequence: from_i64(row.try_get(3).map_err(postgres_error)?, "event sequence")?,
        created_at_ms: i64_to_u64(row.try_get(4).map_err(postgres_error)?, "event time")?,
    })
}

fn row_to_snapshot(row: &Row) -> memory::store::Result<SessionSnapshot> {
    Ok(SessionSnapshot {
        session_id: row.try_get(0).map_err(postgres_error)?,
        event_idx: from_i64(row.try_get(1).map_err(postgres_error)?, "snapshot index")?,
        messages_json: row.try_get(2).map_err(postgres_error)?,
        created_at_ms: i64_to_u64(row.try_get(3).map_err(postgres_error)?, "snapshot time")?,
    })
}

fn event_json_with_allocated_sequence(
    event: &SessionEvent,
    sequence: usize,
) -> memory::store::Result<String> {
    let mut value: serde_json::Value =
        serde_json::from_str(&event.event_json).map_err(|error| {
            memory::MemoryError::Store(format!("decode allocated session event JSON: {error}"))
        })?;
    if let Some(object) = value.as_object_mut() {
        object.insert("sequence".to_string(), serde_json::Value::from(sequence));
    }
    serde_json::to_string(&value).map_err(|error| {
        memory::MemoryError::Store(format!("encode allocated session event JSON: {error}"))
    })
}

fn context_envelope_id(event_json: &str) -> memory::store::Result<String> {
    serde_json::from_str::<serde_json::Value>(event_json)
        .ok()
        .and_then(|payload| {
            payload
                .pointer("/envelope/id")
                .or_else(|| payload.get("envelope_id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .ok_or_else(|| {
            memory::MemoryError::Store(
                "ContextEnvelope append requires envelope.id or envelope_id".to_string(),
            )
        })
}

fn checkpoint_from_event(event: &SessionEvent) -> Option<String> {
    if event.event_type != memory::SESSION_DOMAIN_EVENT_TYPE {
        return None;
    }
    let payload = serde_json::from_str::<serde_json::Value>(&event.event_json).ok()?;
    if payload.get("kind")?.as_str()? != "memory.semantic_checkpoint.created" {
        return None;
    }
    payload
        .pointer("/payload/checkpoint/checkpoint_id")?
        .as_str()
        .map(str::to_string)
}

fn storage_error(error: storage::StorageError) -> memory::MemoryError {
    match error {
        storage::StorageError::Postgres(error) => postgres_error(error),
        other => memory::MemoryError::Store(other.to_string()),
    }
}

fn postgres_error(error: postgres::Error) -> memory::MemoryError {
    let detail = error.as_db_error().map_or_else(
        || error.to_string(),
        |database_error| {
            format!(
                "{} (SQLSTATE {})",
                database_error.message(),
                database_error.code().code()
            )
        },
    );
    memory::MemoryError::Store(detail)
}

fn to_i64(value: usize, label: &str) -> memory::store::Result<i64> {
    i64::try_from(value).map_err(|_| memory::MemoryError::Store(format!("{label} overflow")))
}

fn from_i64(value: i64, label: &str) -> memory::store::Result<usize> {
    usize::try_from(value).map_err(|_| memory::MemoryError::Store(format!("{label} overflow")))
}

fn to_u64_i64(value: u64, label: &str) -> memory::store::Result<i64> {
    i64::try_from(value).map_err(|_| memory::MemoryError::Store(format!("{label} overflow")))
}

fn i64_to_u64(value: i64, label: &str) -> memory::store::Result<u64> {
    u64::try_from(value).map_err(|_| memory::MemoryError::Store(format!("{label} overflow")))
}

fn i64_to_u32(value: i64, label: &str) -> memory::store::Result<u32> {
    u32::try_from(value).map_err(|_| memory::MemoryError::Store(format!("{label} overflow")))
}

// Keep this explicit rather than using partial/default methods: adding a new
// Session operation fails compilation until PostgreSQL has a real owner.
#[allow(clippy::too_many_arguments)]
impl memory::SessionStoreBackend for PostgresSessionStore {
    fn create_session(&self, v: &SessionRecord) -> memory::store::Result<()> {
        self.create_session(v)
    }
    fn get_session(&self, v: &str) -> memory::store::Result<Option<SessionRecord>> {
        self.get_session(v)
    }
    fn get_session_recovery_manifest(
        &self,
        v: &str,
    ) -> memory::store::Result<Option<SessionRecoveryManifest>> {
        self.get_session_recovery_manifest(v)
    }
    fn list_active_session_recovery_manifests(
        &self,
        offset: usize,
        limit: usize,
    ) -> memory::store::Result<Vec<SessionRecoveryManifest>> {
        self.list_active_session_recovery_manifests(offset, limit)
    }
    fn set_session_recovery_signal(
        &self,
        session_id: &str,
        signal: SessionRecoverySignal,
        active: bool,
        observed_at_ms: u64,
    ) -> memory::store::Result<SessionRecoveryManifest> {
        self.set_session_recovery_signal(session_id, signal, active, observed_at_ms)
    }
    fn update_session(&self, v: &SessionRecord) -> memory::store::Result<()> {
        self.update_session(v)
    }
    fn upsert_session(&self, v: &SessionRecord) -> memory::store::Result<()> {
        self.upsert_session(v)
    }
    fn upsert_session_with_mission_outbox(
        &self,
        v: &SessionRecord,
        r: &SessionMissionOutboxRequest,
    ) -> memory::store::Result<SessionMissionOutboxRecord> {
        self.upsert_session_with_mission_outbox(v, r)
    }
    fn delete_session(&self, v: &str) -> memory::store::Result<()> {
        self.delete_session(v)
    }
    fn delete_session_with_mission_outbox(
        &self,
        r: &SessionMissionOutboxRequest,
    ) -> memory::store::Result<bool> {
        self.delete_session_with_mission_outbox(r)
    }
    fn mark_session_closed(&self, v: &str) -> memory::store::Result<()> {
        self.mark_session_closed(v)
    }
    fn list_sessions(&self) -> memory::store::Result<Vec<SessionRecord>> {
        self.list_sessions()
    }
    fn list_sessions_page(
        &self,
        v: &SessionListOptions<'_>,
    ) -> memory::store::Result<SessionListPage> {
        self.list_sessions_page(v)
    }
    fn list_sessions_by_platform(&self, v: &str) -> memory::store::Result<Vec<SessionRecord>> {
        self.list_sessions_by_platform(v)
    }
    fn list_sessions_by_workspace_root(
        &self,
        v: &str,
    ) -> memory::store::Result<Vec<SessionRecord>> {
        self.list_sessions_by_workspace_root(v)
    }
    fn search_sessions(
        &self,
        q: &str,
        l: usize,
    ) -> memory::store::Result<Vec<SessionSearchResult>> {
        self.search_sessions(q, None, l)
    }
    fn search_sessions_by_platform(
        &self,
        q: &str,
        p: &str,
        l: usize,
    ) -> memory::store::Result<Vec<SessionSearchResult>> {
        self.search_sessions(q, Some(p), l)
    }
    fn associate_memory(&self, a: &str, b: &str) -> memory::store::Result<()> {
        self.associate_memory(a, b)
    }
    fn get_session_memories(&self, a: &str) -> memory::store::Result<Vec<String>> {
        self.get_session_memories(a)
    }
    fn disassociate_memory(&self, a: &str, b: &str) -> memory::store::Result<()> {
        self.disassociate_memory(a, b)
    }
    fn append_event(&self, v: &SessionEvent) -> memory::store::Result<()> {
        self.append_event(v)
    }
    fn append_event_allocating_sequence(
        &self,
        v: &SessionEvent,
    ) -> memory::store::Result<SessionEvent> {
        self.append_event_allocating_sequence(v)
    }
    fn append_events_allocating_sequence(
        &self,
        v: &[SessionEvent],
    ) -> memory::store::Result<Vec<SessionEvent>> {
        self.append_events_allocating_sequence(v)
    }
    fn append_events_allocating_sequence_if_checkpoint_absent(
        &self,
        v: &[SessionEvent],
        c: &str,
    ) -> memory::store::Result<Option<Vec<SessionEvent>>> {
        self.append_events_allocating_sequence_if_checkpoint_absent(v, c)
    }
    fn append_context_envelope_event_if_absent(
        &self,
        v: &SessionEvent,
    ) -> memory::store::Result<bool> {
        self.append_context_envelope_event_if_absent(v)
    }
    fn append_context_envelope_event_if_absent_allocating_sequence(
        &self,
        v: &SessionEvent,
    ) -> memory::store::Result<Option<SessionEvent>> {
        self.append_context_envelope_event_if_absent_allocating_sequence(v)
    }
    fn get_events(&self, a: &str, b: usize) -> memory::store::Result<Vec<SessionEvent>> {
        self.get_events(a, b)
    }
    fn get_events_limited(
        &self,
        a: &str,
        b: usize,
        c: usize,
    ) -> memory::store::Result<Vec<SessionEvent>> {
        self.get_events_limited(a, b, c)
    }
    fn get_session_domain_timeline_limited(
        &self,
        a: &str,
        b: usize,
        c: usize,
    ) -> memory::store::Result<Vec<SessionEvent>> {
        self.get_session_domain_timeline_limited(a, b, c)
    }
    fn count_session_domain_timeline_from(
        &self,
        a: &str,
        b: usize,
    ) -> memory::store::Result<usize> {
        self.count_session_domain_timeline_from(a, b)
    }
    fn get_events_by_type_limited(
        &self,
        a: &str,
        b: &str,
        c: usize,
        d: usize,
    ) -> memory::store::Result<Vec<SessionEvent>> {
        self.get_events_by_type_limited(a, b, c, d)
    }
    fn count_events_from(&self, a: &str, b: usize) -> memory::store::Result<usize> {
        self.count_events_from(a, b)
    }
    fn count_events_by_type_from(
        &self,
        a: &str,
        b: &str,
        c: usize,
    ) -> memory::store::Result<usize> {
        self.count_events_by_type_from(a, b, c)
    }
    fn get_context_event_by_envelope_id(
        &self,
        a: &str,
    ) -> memory::store::Result<Option<SessionEvent>> {
        self.get_context_event_by_envelope_id(a)
    }
    fn next_event_sequence(&self, a: &str) -> memory::store::Result<usize> {
        self.next_event_sequence(a)
    }
    fn delete_events_from(&self, a: &str, b: usize) -> memory::store::Result<usize> {
        self.delete_events_from(a, b)
    }
    fn delete_events_by_type_from(
        &self,
        a: &str,
        b: &str,
        c: usize,
    ) -> memory::store::Result<usize> {
        self.delete_events_by_type_from(a, b, c)
    }
    fn save_snapshot(&self, v: &SessionSnapshot) -> memory::store::Result<()> {
        self.save_snapshot(v)
    }
    fn get_latest_snapshot(&self, a: &str) -> memory::store::Result<Option<SessionSnapshot>> {
        self.get_latest_snapshot(a)
    }
    fn prune_before(&self, a: &str) -> memory::store::Result<usize> {
        self.prune_before(a)
    }
    fn insert_message(&self, v: &SessionMessage) -> memory::store::Result<()> {
        self.insert_message(v)
    }
    fn append_terminal_message_idempotent(
        &self,
        a: &str,
        b: &str,
        c: &str,
        d: Option<&str>,
        e: u64,
    ) -> memory::store::Result<(SessionMessage, bool)> {
        self.append_terminal_message_idempotent(a, b, c, d, e)
    }
    fn append_terminal_transcript_idempotent(
        &self,
        a: &str,
        b: &str,
        c: &str,
        d: &[SessionMessage],
        e: u64,
    ) -> memory::store::Result<(Vec<SessionMessage>, bool)> {
        self.append_terminal_transcript_idempotent(a, b, c, d, e)
    }
    fn insert_messages_batch(&self, a: &[SessionMessage]) -> memory::store::Result<()> {
        self.insert_messages_batch(a)
    }
    fn append_message_with_runtime_outbox(
        &self,
        a: &SessionMessage,
        b: &SessionRuntimeOutboxRequest,
    ) -> memory::store::Result<SessionRuntimeOutboxRecord> {
        self.append_message_with_runtime_outbox(a, b)
    }
    fn append_ingress_with_runtime_outbox(
        &self,
        a: &str,
        b: &str,
        c: Option<&str>,
        d: u64,
        e: &SessionRuntimeOutboxRequest,
    ) -> memory::store::Result<SessionRuntimeOutboxRecord> {
        self.append_ingress_with_runtime_outbox(a, b, c, d, e)
    }
    fn claim_session_runtime_outbox(
        &self,
        a: &str,
        b: u64,
        c: u64,
        d: usize,
    ) -> memory::store::Result<Vec<SessionRuntimeOutboxRecord>> {
        self.claim_session_runtime_outbox(a, b, c, d)
    }
    fn ack_session_runtime_outbox(
        &self,
        a: &str,
        b: &str,
        c: u64,
        d: u64,
        e: u64,
    ) -> memory::store::Result<SessionRuntimeOutboxRecord> {
        self.ack_session_runtime_outbox(a, b, c, d, e)
    }
    fn renew_session_runtime_outbox_lease(
        &self,
        a: &str,
        b: &str,
        c: u64,
        d: u64,
        e: u64,
    ) -> memory::store::Result<SessionRuntimeOutboxRecord> {
        self.renew_session_runtime_outbox_lease(a, b, c, d, e)
    }
    fn fail_session_runtime_outbox(
        &self,
        a: &str,
        b: &str,
        c: u64,
        d: OutboxFailureClass,
        e: &str,
        f: u64,
        g: u32,
        h: u64,
    ) -> memory::store::Result<SessionRuntimeOutboxRecord> {
        self.fail_session_runtime_outbox(a, b, c, d, e, f, g, h)
    }
    fn retry_blocked_session_runtime_outbox(
        &self,
        a: &str,
        b: u64,
        c: &str,
        d: &str,
        e: u64,
    ) -> memory::store::Result<SessionRuntimeOutboxRecord> {
        self.retry_blocked_session_runtime_outbox(a, b, c, d, e)
    }
    fn get_session_runtime_outbox(
        &self,
        a: &str,
    ) -> memory::store::Result<Option<SessionRuntimeOutboxRecord>> {
        self.get_session_runtime_outbox(a)
    }
    fn session_runtime_outbox_for_session(
        &self,
        a: &str,
        b: usize,
    ) -> memory::store::Result<Vec<SessionRuntimeOutboxRecord>> {
        self.session_runtime_outbox_for_session(a, b)
    }
    fn active_session_runtime_outbox(
        &self,
        a: usize,
    ) -> memory::store::Result<Vec<SessionRuntimeOutboxRecord>> {
        self.active_session_runtime_outbox(a)
    }
    fn session_runtime_outbox_health(&self) -> memory::store::Result<SessionRuntimeOutboxHealth> {
        self.session_runtime_outbox_health()
    }
    fn blocked_session_runtime_outbox(
        &self,
        a: usize,
    ) -> memory::store::Result<Vec<SessionRuntimeOutboxRecord>> {
        self.blocked_session_runtime_outbox(a)
    }
    fn claim_session_mission_outbox(
        &self,
        a: &str,
        b: &str,
        c: u64,
        d: u64,
        e: usize,
    ) -> memory::store::Result<Vec<SessionMissionOutboxRecord>> {
        self.claim_session_mission_outbox(a, b, c, d, e)
    }
    fn ack_session_mission_outbox(
        &self,
        a: &str,
        b: &str,
        c: u64,
        d: u64,
    ) -> memory::store::Result<SessionMissionOutboxRecord> {
        self.ack_session_mission_outbox(a, b, c, d)
    }
    fn fail_session_mission_outbox(
        &self,
        a: &str,
        b: &str,
        c: u64,
        d: OutboxFailureClass,
        e: &str,
        f: u64,
        g: u32,
        h: u64,
    ) -> memory::store::Result<SessionMissionOutboxRecord> {
        self.fail_session_mission_outbox(a, b, c, d, e, f, g, h)
    }
    fn get_session_mission_outbox(
        &self,
        a: &str,
    ) -> memory::store::Result<Option<SessionMissionOutboxRecord>> {
        self.get_session_mission_outbox(a)
    }
    fn get_messages(
        &self,
        a: &str,
        b: usize,
        c: usize,
    ) -> memory::store::Result<Vec<SessionMessage>> {
        self.get_messages(a, b, c)
    }
    fn get_messages_from_sequence(
        &self,
        a: &str,
        b: usize,
        c: usize,
    ) -> memory::store::Result<Vec<SessionMessage>> {
        self.get_messages_from_sequence(a, b, c)
    }
    fn get_all_messages(&self, a: &str) -> memory::store::Result<Vec<SessionMessage>> {
        self.get_all_messages(a)
    }
    fn get_message_count(&self, a: &str) -> memory::store::Result<usize> {
        self.get_message_count(a)
    }
    fn delete_messages_from(&self, a: &str, b: usize) -> memory::store::Result<usize> {
        self.delete_messages_from(a, b)
    }
    fn search_messages(
        &self,
        a: &str,
        b: Option<&str>,
        c: usize,
    ) -> memory::store::Result<Vec<SessionMessage>> {
        self.search_messages(a, b, c)
    }
    fn search_messages_in_sessions(
        &self,
        a: &str,
        b: &[String],
        c: usize,
    ) -> memory::store::Result<Vec<SessionMessage>> {
        self.search_messages_in_sessions(a, b, c)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier, Mutex, MutexGuard, OnceLock};

    use memory::{SessionStoreBackend, UnifiedSessionStore};
    use storage::StaticSecretRefResolver;

    use super::*;

    fn postgres_test_guard() -> MutexGuard<'static, ()> {
        static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
        GUARD
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn session(id: &str) -> SessionRecord {
        SessionRecord {
            session_id: id.to_string(),
            platform: "test".to_string(),
            chat_id: "chat".to_string(),
            user_id: Some("user".to_string()),
            model: Some("model".to_string()),
            created_at: "2026-07-23T00:00:00Z".to_string(),
            last_activity: "2026-07-23T00:00:00Z".to_string(),
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: Some(
                r#"{"workspace_root":"/work","title":"session migration"}"#.to_string(),
            ),
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        }
    }

    fn real_store() -> PostgresSessionStore {
        let url =
            std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
        let resolver = StaticSecretRefResolver::new([("test.pg".to_string(), url)]);
        PostgresSessionStore::connect(
            PostgresConnectionConfig::new(
                "session-postgres-test",
                "test.pg",
                "cowd-session-postgres-contract",
            ),
            &resolver,
        )
        .expect("isolated PostgreSQL session store opens")
    }

    #[test]
    fn sqlite_snapshot_contains_full_session_truth_and_is_stable() {
        let source = SqliteSessionStore::open_in_memory().expect("SQLite source opens");
        source
            .create_session(&session("migration-session"))
            .expect("session");
        source
            .insert_message(&SessionMessage {
                stable_message_id: "m-1".to_string(),
                session_id: "migration-session".to_string(),
                sequence: 0,
                role: "user".to_string(),
                content_json: r#"[{"type":"text","text":"hello"}]"#.to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: 1,
            })
            .expect("message");
        source
            .append_event(&SessionEvent {
                session_id: "migration-session".to_string(),
                event_type: "SessionCreated".to_string(),
                event_json: r#"{"kind":"session.created"}"#.to_string(),
                sequence: 0,
                created_at_ms: 2,
            })
            .expect("event");
        source
            .append_event(&SessionEvent {
                session_id: "migration-session".to_string(),
                event_type: memory::SESSION_DOMAIN_EVENT_TYPE.to_string(),
                event_json: r#"{"kind":"memory.semantic_checkpoint.created","payload":{"checkpoint":{"checkpoint_id":"checkpoint-1"}}}"#.to_string(),
                sequence: 1,
                created_at_ms: 3,
            })
            .expect("checkpoint event");
        source
            .save_snapshot(&SessionSnapshot {
                session_id: "migration-session".to_string(),
                event_idx: 0,
                messages_json: "[]".to_string(),
                created_at_ms: 4,
            })
            .expect("snapshot");
        let first = export_sqlite_session_snapshot(&source).expect("first snapshot");
        let second = export_sqlite_session_snapshot(&source).expect("second snapshot");
        assert_eq!(
            first.canonical_digest().unwrap(),
            second.canonical_digest().unwrap()
        );
        assert_eq!(first.sessions.len(), 1);
        assert_eq!(first.messages.len(), 1);
        assert_eq!(first.events.len(), 2);
        assert_eq!(first.checkpoints.len(), 1);
        assert_eq!(first.snapshots.len(), 1);
    }

    #[tokio::test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    async fn postgres_adapter_real_copy_fences_and_injected_facade() {
        let _guard = postgres_test_guard();
        let target = real_store();
        let source = SqliteSessionStore::open_in_memory().expect("SQLite source opens");
        source
            .create_session(&session("migration-session"))
            .expect("session");
        source
            .insert_message(&SessionMessage {
                stable_message_id: "m-copy".to_string(),
                session_id: "migration-session".to_string(),
                sequence: 0,
                role: "user".to_string(),
                content_json: "[]".to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: 1,
            })
            .expect("message");
        let root = tempfile::tempdir().expect("manifest root");
        let manifest =
            copy_quiesced_session_store(&source, &target, root.path().join("session.json"))
                .expect("copy");
        assert_eq!(manifest.source_digest, manifest.target_digest);
        let injected = UnifiedSessionStore::from_backend(Arc::new(target.clone()));
        assert_eq!(
            injected
                .list_sessions()
                .await
                .expect("injected facade read")
                .len(),
            target.list_sessions().unwrap().len()
        );
        let seed = SessionEvent {
            session_id: "migration-session".to_string(),
            event_type: "parallel".to_string(),
            event_json: "{}".to_string(),
            sequence: 0,
            created_at_ms: 5,
        };
        let backend: Arc<dyn SessionStoreBackend> = Arc::new(target.clone());
        let gate = Arc::new(Barrier::new(2));
        let workers = (0..2)
            .map(|_| {
                let backend = Arc::clone(&backend);
                let gate = Arc::clone(&gate);
                let seed = seed.clone();
                std::thread::spawn(move || {
                    gate.wait();
                    backend
                        .append_event_allocating_sequence(&seed)
                        .expect("allocated")
                })
            })
            .collect::<Vec<_>>();
        let mut sequences = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker").sequence)
            .collect::<Vec<_>>();
        sequences.sort_unstable();
        assert_eq!(sequences, vec![0, 1]);
        target
            .delete_session("migration-session")
            .expect("delete isolated migration session");
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn postgres_terminal_transcript_preserves_published_cursor_and_is_idempotent() {
        let _guard = postgres_test_guard();
        let store = real_store();
        let session_id = format!(
            "causal-terminal-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_millis()
        );
        store
            .create_session(&session(&session_id))
            .expect("create isolated session");
        for (sequence, id, text, turn_id) in [
            (0, "user-1", "first", "turn-1"),
            (1, "user-2", "second", "turn-2"),
        ] {
            store
                .insert_message(&SessionMessage {
                    stable_message_id: format!("{session_id}:{id}"),
                    session_id: session_id.clone(),
                    sequence,
                    role: "user".to_string(),
                    content_json: serde_json::json!([{
                        "type": "text",
                        "text": text,
                        "cowd_turn_id": turn_id,
                        "cowd_turn_ingress_message_id": format!("{session_id}:{id}"),
                    }])
                    .to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: sequence as u64 + 1,
                })
                .expect("insert ingress");
        }
        let terminal_id = format!("{session_id}:assistant-1");
        let ingress_id = format!("{session_id}:user-1");
        let transcript = vec![SessionMessage {
            stable_message_id: terminal_id.clone(),
            session_id: session_id.clone(),
            sequence: usize::MAX,
            role: "assistant".to_string(),
            content_json: serde_json::json!([{
                "type": "text",
                "text": "first answer",
                "cowd_turn_id": "turn-1",
                "cowd_turn_ingress_message_id": ingress_id,
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
                &terminal_id,
                &ingress_id,
                &session_id,
                &transcript,
                3,
            )
            .expect("commit terminal transcript");
        assert!(inserted);
        assert_eq!(committed[0].sequence, 2);
        let physical = store
            .get_all_messages(&session_id)
            .expect("load physical order");
        assert_eq!(
            physical
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1, 2],
            "published ingress cursors must stay immutable in PostgreSQL"
        );

        let (replayed, inserted) = store
            .append_terminal_transcript_idempotent(
                &terminal_id,
                &ingress_id,
                &session_id,
                &transcript,
                99,
            )
            .expect("replay terminal transcript");
        assert!(!inserted);
        assert_eq!(replayed, committed);
        assert_eq!(
            store
                .get_all_messages(&session_id)
                .expect("reload physical order"),
            physical
        );
        store
            .delete_session(&session_id)
            .expect("delete isolated session");
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn postgres_concurrent_store_startup_serializes_preflight_and_migrations() {
        let _guard = postgres_test_guard();
        let url =
            std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
        let worker_count = 8;
        let gate = Arc::new(Barrier::new(worker_count));
        let workers = (0..worker_count)
            .map(|worker| {
                let gate = Arc::clone(&gate);
                let url = url.clone();
                std::thread::spawn(move || {
                    let resolver = StaticSecretRefResolver::new([("test.pg".to_string(), url)]);
                    gate.wait();
                    PostgresSessionStore::connect(
                        PostgresConnectionConfig::new(
                            format!("session-postgres-concurrent-{worker}"),
                            "test.pg",
                            "cowd-concurrent-session-test",
                        ),
                        &resolver,
                    )
                    .expect("concurrent PostgreSQL session store opens")
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().expect("startup worker does not panic");
        }
    }
}
