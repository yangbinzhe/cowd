//! PostgreSQL durable session adapter.
//!
//! The adapter is constructed only from the host-owned, bounded
//! [`storage::PostgresExecutor`]. It never accepts a path or a database URL.

use std::{collections::BTreeMap, fs, path::Path};

use harness_contract::turn::InputRoutingDecision;
use postgres::{types::ToSql, Row};
use serde::{Deserialize, Serialize};
use session::{
    build_context_index_cards, context_index_card_digest, context_index_source_digest,
    ContextIndexCard, ContextIndexCoverage, OutboxFailureClass, OutboxStatus,
    SessionBranchActivation, SessionBranchActivationPhase, SessionBranchActivationTransition,
    SessionBranchRequest, SessionBranchResult, SessionCloseDisposition, SessionEvent,
    SessionInputAdmission, SessionLifecycleFenceRequest, SessionLifecycleIntent,
    SessionLifecyclePhase, SessionLifecyclePlan, SessionLifecycleTombstoneRequest,
    SessionLifecycleTransition, SessionListOptions, SessionListPage, SessionMessage,
    SessionMessageMetadata, SessionMissionOutboxOperation, SessionMissionOutboxRecord,
    SessionMissionOutboxRequest, SessionPresenceProjection, SessionRecord, SessionRecoveryManifest,
    SessionRecoverySignal, SessionRuntimeInputStatus, SessionRuntimeOutboxHealth,
    SessionRuntimeOutboxRecord, SessionRuntimeOutboxRequest, SessionSearchResult, SessionSnapshot,
    SessionTerminalTranscriptCommit, SessionTerminalTranscriptReceipt, SessionUsageBucket,
    SessionUsageSummary, SqliteSessionStore,
};
use session::{SessionDomainEvent, SessionDomainRef, SessionDomainScope};
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
    pub input_admissions: Vec<SessionInputAdmission>,
    pub lifecycle_intents: Vec<SessionLifecycleIntent>,
    pub branch_activations: Vec<SessionBranchActivation>,
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
    pub fn canonical_digest(&self) -> session::SessionResult<String> {
        let bytes = serde_json::to_vec(self).map_err(|error| {
            session::SessionError::Store(format!("encode session migration snapshot: {error}"))
        })?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

/// Export every durable Session table from a quiesced SQLite owner.
pub fn export_sqlite_session_snapshot(
    source: &SqliteSessionStore,
) -> session::SessionResult<SessionMigrationSnapshot> {
    let connection = source.conn()?;
    let sessions = sqlite_rows(&connection, "SELECT session_id,platform,chat_id,user_id,model,created_at,last_activity,message_count,reset_policy,metadata_json,input_tokens,output_tokens,estimated_cost_usd,status FROM sessions ORDER BY session_id", sqlite_row_to_session)?;
    let input_admissions = sqlite_rows(
        &connection,
        "SELECT session_id,input_generation,input_admission_open FROM sessions ORDER BY session_id",
        |row| {
            Ok(SessionInputAdmission {
                session_id: row.get(0)?,
                generation: u64::try_from(row.get::<_, i64>(1)?)
                    .map_err(sqlite_conversion_error)?,
                open: row.get(2)?,
            })
        },
    )?;
    let lifecycle_intents = sqlite_rows(
        &connection,
        "SELECT operation_id,session_id,disposition,phase,last_stable_phase,
                expected_generation,created_at_ms,updated_at_ms,last_error,revision
           FROM session_lifecycle_intents ORDER BY operation_id",
        sqlite_row_to_lifecycle_intent,
    )?;
    let branch_activations = sqlite_rows(
        &connection,
        "SELECT operation_id,source_session_id,target_session_id,source_message_count,
                phase,created_at_ms,updated_at_ms,last_error,revision
           FROM session_branch_activations ORDER BY operation_id",
        sqlite_row_to_branch_activation,
    )?;
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
    let runtime_outbox = sqlite_rows(&connection, "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,decision,target_turn_id,classification_json,status,runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch FROM session_runtime_outbox ORDER BY request_id", sqlite_row_to_runtime_outbox)?;
    let mission_outbox = sqlite_rows(&connection, "SELECT request_id,session_id,title,workspace_key,operation,status,attempts,next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms FROM session_mission_outbox ORDER BY request_id", sqlite_row_to_mission_outbox)?;
    let runtime_history = sqlite_rows(&connection, "SELECT request_id,action,actor,reason,from_status,to_status,attempts,created_at_ms FROM session_runtime_outbox_history ORDER BY id", sqlite_row_to_history)?;
    let mission_history = sqlite_rows(&connection, "SELECT request_id,action,actor,reason,from_status,to_status,attempts,created_at_ms FROM session_mission_outbox_history ORDER BY id", sqlite_row_to_history)?;
    Ok(SessionMigrationSnapshot {
        schema_version: 5,
        sessions,
        input_admissions,
        lifecycle_intents,
        branch_activations,
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
) -> session::SessionResult<Vec<T>> {
    let mut prepared = connection
        .prepare(statement)
        .map_err(|error| session::SessionError::Store(error.to_string()))?;
    let rows = prepared
        .query_map([], map)
        .map_err(|error| session::SessionError::Store(error.to_string()))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| session::SessionError::Store(error.to_string()))
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

fn sqlite_row_to_lifecycle_intent(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<SessionLifecycleIntent> {
    Ok(SessionLifecycleIntent {
        operation_id: row.get(0)?,
        session_id: row.get(1)?,
        disposition: SessionCloseDisposition::parse(&row.get::<_, String>(2)?)
            .map_err(sqlite_text_conversion_error)?,
        phase: SessionLifecyclePhase::parse(&row.get::<_, String>(3)?)
            .map_err(sqlite_text_conversion_error)?,
        last_stable_phase: SessionLifecyclePhase::parse(&row.get::<_, String>(4)?)
            .map_err(sqlite_text_conversion_error)?,
        expected_generation: u64::try_from(row.get::<_, i64>(5)?)
            .map_err(sqlite_conversion_error)?,
        created_at_ms: u64::try_from(row.get::<_, i64>(6)?).map_err(sqlite_conversion_error)?,
        updated_at_ms: u64::try_from(row.get::<_, i64>(7)?).map_err(sqlite_conversion_error)?,
        last_error: row.get(8)?,
        revision: u64::try_from(row.get::<_, i64>(9)?).map_err(sqlite_conversion_error)?,
    })
}

fn sqlite_row_to_branch_activation(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<SessionBranchActivation> {
    Ok(SessionBranchActivation {
        operation_id: row.get(0)?,
        source_session_id: row.get(1)?,
        target_session_id: row.get(2)?,
        source_message_count: usize::try_from(row.get::<_, i64>(3)?)
            .map_err(sqlite_conversion_error)?,
        phase: SessionBranchActivationPhase::parse(&row.get::<_, String>(4)?)
            .map_err(sqlite_text_conversion_error)?,
        created_at_ms: u64::try_from(row.get::<_, i64>(5)?).map_err(sqlite_conversion_error)?,
        updated_at_ms: u64::try_from(row.get::<_, i64>(6)?).map_err(sqlite_conversion_error)?,
        last_error: row.get(7)?,
        revision: u64::try_from(row.get::<_, i64>(8)?).map_err(sqlite_conversion_error)?,
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
        input_id: row.get(0)?,
        request_id: row.get(1)?,
        turn_id: row.get(2)?,
        message_id: row.get(3)?,
        session_id: row.get(4)?,
        sequence: usize::try_from(row.get::<_, i64>(5)?).map_err(sqlite_conversion_error)?,
        session_generation: u64::try_from(row.get::<_, i64>(6)?)
            .map_err(sqlite_conversion_error)?,
        decision: parse_input_decision_sqlite(&row.get::<_, String>(7)?)?,
        target_turn_id: row.get(8)?,
        classification_json: row.get(9)?,
        status: SessionRuntimeInputStatus::parse(&row.get::<_, String>(10)?)?,
        runtime_commit_cursor: row
            .get::<_, Option<i64>>(11)?
            .map(|value| u64::try_from(value).map_err(sqlite_conversion_error))
            .transpose()?,
        attempts: u32::try_from(row.get::<_, i64>(12)?).map_err(sqlite_conversion_error)?,
        next_attempt_at_ms: u64::try_from(row.get::<_, i64>(13)?)
            .map_err(sqlite_conversion_error)?,
        claim_owner: row.get(14)?,
        claim_token: row.get(15)?,
        claim_expires_at_ms: row
            .get::<_, Option<i64>>(16)?
            .map(|value| u64::try_from(value).map_err(sqlite_conversion_error))
            .transpose()?,
        failure_class: row
            .get::<_, Option<String>>(17)?
            .as_deref()
            .map(OutboxFailureClass::parse)
            .transpose()?,
        last_error: row.get(18)?,
        revision: u64::try_from(row.get::<_, i64>(19)?).map_err(sqlite_conversion_error)?,
        created_at_ms: u64::try_from(row.get::<_, i64>(20)?).map_err(sqlite_conversion_error)?,
        updated_at_ms: u64::try_from(row.get::<_, i64>(21)?).map_err(sqlite_conversion_error)?,
        terminal_at_ms: row
            .get::<_, Option<i64>>(22)?
            .map(|value| u64::try_from(value).map_err(sqlite_conversion_error))
            .transpose()?,
        runtime_options_json: row.get(23)?,
        claim_fence_epoch: row
            .get::<_, Option<i64>>(24)?
            .map(|value| u64::try_from(value).map_err(sqlite_conversion_error))
            .transpose()?,
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

fn sqlite_text_conversion_error(error: session::SessionError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        error.to_string().into(),
    )
}

fn parse_input_decision_sqlite(value: &str) -> rusqlite::Result<InputRoutingDecision> {
    parse_input_decision(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            error.to_string().into(),
        )
    })
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
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_session_domain_event_id
            ON session_events(session_id, (event_json::jsonb ->> 'event_id'))
            WHERE event_type = 'SessionDomainEvent'
              AND (event_json::jsonb ->> 'event_id') IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_session_domain_kind_sequence
            ON session_events(session_id, (event_json::jsonb ->> 'kind'), sequence ASC)
            WHERE event_type = 'SessionDomainEvent'",
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
}, PostgresMigrationSpec {
    id: "session.0008.durable-session-input-authority",
    domain: SESSION_DOMAIN,
    version: 8,
    description: "add durable classified input, admission generation and fenced ownership",
    statements: &[
        "ALTER TABLE session_records
             ADD COLUMN IF NOT EXISTS input_generation BIGINT NOT NULL DEFAULT 1",
        "ALTER TABLE session_records
             ADD COLUMN IF NOT EXISTS input_admission_open BOOLEAN NOT NULL DEFAULT TRUE",
        "ALTER TABLE session_runtime_outbox ADD COLUMN IF NOT EXISTS input_id TEXT",
        "ALTER TABLE session_runtime_outbox
             ADD COLUMN IF NOT EXISTS session_generation BIGINT NOT NULL DEFAULT 1",
        "ALTER TABLE session_runtime_outbox
             ADD COLUMN IF NOT EXISTS decision TEXT NOT NULL DEFAULT 'start_new_turn'",
        "ALTER TABLE session_runtime_outbox ADD COLUMN IF NOT EXISTS target_turn_id TEXT",
        "ALTER TABLE session_runtime_outbox ADD COLUMN IF NOT EXISTS classification_json TEXT",
        "ALTER TABLE session_runtime_outbox ADD COLUMN IF NOT EXISTS claim_token TEXT",
        "ALTER TABLE session_runtime_outbox ADD COLUMN IF NOT EXISTS terminal_at_ms BIGINT",
        "UPDATE session_runtime_outbox
            SET input_id=request_id
          WHERE input_id IS NULL OR btrim(input_id)=''",
        "UPDATE session_runtime_outbox
            SET status=CASE status
                WHEN 'pending' THEN 'queued'
                WHEN 'retry_scheduled' THEN 'queued'
                WHEN 'materialized' THEN 'completed'
                WHEN 'blocked_materialization' THEN 'blocked'
                ELSE status
            END",
        "UPDATE session_runtime_outbox
            SET terminal_at_ms=COALESCE(terminal_at_ms,updated_at_ms)
          WHERE status IN ('completed','supplemented','failed','cancelled','expired')",
        "ALTER TABLE session_runtime_outbox ALTER COLUMN input_id SET NOT NULL",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_session_runtime_outbox_input_id
             ON session_runtime_outbox(input_id)",
        "DROP INDEX IF EXISTS idx_session_runtime_outbox_claim",
        "CREATE INDEX idx_session_runtime_outbox_claim
             ON session_runtime_outbox(
                 status, next_attempt_at_ms, claim_expires_at_ms,
                 session_id, sequence, request_id
             )",
        "CREATE INDEX IF NOT EXISTS idx_session_runtime_outbox_session_head
             ON session_runtime_outbox(
                 session_id, session_generation, sequence, request_id, status
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
                 session_id,durable_cursor,history_revision,
                 transcript_messages,transcript_bytes,in_flight_turn,
                 active_writer_or_attachment,
                 mission_agent_team_continuation,last_activity_ms,
                 manifest_revision
             )
             SELECT
                 record.session_id,
                 COALESCE((
                     SELECT MAX(sequence)+1 FROM session_messages
                      WHERE session_id=record.session_id
                 ),0),
                 CASE WHEN bump_history THEN 1 ELSE 0 END,
                 COALESCE((
                     SELECT COUNT(*) FROM session_messages
                      WHERE session_id=record.session_id
                 ),0),
                 COALESCE((
                     SELECT SUM(
                         octet_length(stable_message_id)
                         + octet_length(session_id)
                         + octet_length(role)
                         + octet_length(content_json)
                         + octet_length(COALESCE(token_usage_json,''))
                         + octet_length(COALESCE(tool_use_id,''))
                         + octet_length(COALESCE(tool_name,''))
                     )
                     FROM session_messages WHERE session_id=record.session_id
                 ),0),
                 EXISTS(
                     SELECT 1 FROM session_runtime_outbox
                      WHERE session_id=record.session_id
                        AND status IN (
                            'accepted','classified','queued','claimed',
                            'running','reclassified','blocked'
                        )
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
                 ),FALSE),
                 EXISTS(
                     SELECT 1 FROM session_mission_outbox
                      WHERE session_id=record.session_id
                        AND operation='start'
                        AND status IN ('pending','claimed','retry_scheduled')
                 ),
                 GREATEST(record.created_at_ms,record.updated_at_ms),
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
                     session_recovery_manifest.manifest_revision+1;
         END
         $$",
        "CREATE OR REPLACE FUNCTION cowd_session_runtime_input_recovery_trigger()
         RETURNS TRIGGER
         LANGUAGE plpgsql
         AS $$
         BEGIN
             PERFORM cowd_refresh_session_recovery_manifest(NEW.session_id, FALSE);
             UPDATE session_recovery_manifest
                SET in_flight_turn=EXISTS(
                        SELECT 1 FROM session_runtime_outbox
                         WHERE session_id=NEW.session_id
                           AND status IN (
                               'accepted','classified','queued','claimed',
                               'running','reclassified','blocked'
                           )
                    ),
                    manifest_revision=manifest_revision + 1
              WHERE session_id=NEW.session_id;
             RETURN NEW;
         END
         $$",
        "DROP TRIGGER IF EXISTS session_recovery_runtime_outbox_change
             ON session_runtime_outbox",
        "CREATE TRIGGER session_recovery_runtime_outbox_change
             AFTER INSERT OR UPDATE OF status ON session_runtime_outbox
              FOR EACH ROW
              EXECUTE FUNCTION cowd_session_runtime_input_recovery_trigger()",
        "UPDATE session_recovery_manifest AS manifest
            SET in_flight_turn=EXISTS(
                    SELECT 1 FROM session_runtime_outbox
                     WHERE session_id=manifest.session_id
                       AND status IN (
                           'accepted','classified','queued','claimed',
                           'running','reclassified','blocked'
                       )
                ),
                manifest_revision=manifest_revision + 1",
    ],
}, PostgresMigrationSpec {
    id: "session.0009.lifecycle-and-branch-recovery",
    domain: SESSION_DOMAIN,
    version: 9,
    description: "add durable Session lifecycle intents and branch activation receipts",
    statements: &[
        "CREATE TABLE IF NOT EXISTS session_lifecycle_intents (
            operation_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL REFERENCES session_records(session_id) ON DELETE CASCADE,
            disposition TEXT NOT NULL,
            phase TEXT NOT NULL,
            last_stable_phase TEXT NOT NULL,
            expected_generation BIGINT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL,
            last_error TEXT,
            revision BIGINT NOT NULL DEFAULT 0
        )",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_session_lifecycle_active
             ON session_lifecycle_intents(session_id)
             WHERE phase != 'unloaded'",
        "CREATE INDEX IF NOT EXISTS idx_session_lifecycle_recovery
             ON session_lifecycle_intents(phase, updated_at_ms, operation_id)",
        "CREATE TABLE IF NOT EXISTS session_branch_activations (
            operation_id TEXT PRIMARY KEY,
            source_session_id TEXT NOT NULL
                REFERENCES session_records(session_id) ON DELETE CASCADE,
            target_session_id TEXT NOT NULL UNIQUE
                REFERENCES session_records(session_id) ON DELETE CASCADE,
            source_message_count BIGINT NOT NULL,
            phase TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL,
            last_error TEXT,
            revision BIGINT NOT NULL DEFAULT 0
        )",
        "CREATE INDEX IF NOT EXISTS idx_session_branch_activation_recovery
             ON session_branch_activations(phase, updated_at_ms, operation_id)",
    ],
}, PostgresMigrationSpec {
    id: "session.0010.terminal-claim-fence-epoch",
    domain: SESSION_DOMAIN,
    version: 10,
    description: "separate immutable terminal claim fence epoch from mutable lease revision",
    statements: &[
        "ALTER TABLE session_runtime_outbox
             ADD COLUMN IF NOT EXISTS claim_fence_epoch BIGINT",
        "UPDATE session_runtime_outbox
            SET claim_fence_epoch=revision
          WHERE claim_fence_epoch IS NULL
            AND claim_token IS NOT NULL
            AND status IN ('claimed','running')",
        "ALTER TABLE session_runtime_outbox
             DROP CONSTRAINT IF EXISTS session_runtime_claim_fence_epoch_positive",
        "ALTER TABLE session_runtime_outbox
             ADD CONSTRAINT session_runtime_claim_fence_epoch_positive
             CHECK (claim_fence_epoch IS NULL OR claim_fence_epoch > 0)",
    ],
}, PostgresMigrationSpec {
    id: "session.0011.activation-index",
    domain: SESSION_DOMAIN,
    version: 11,
    description: "add checkpoint-first activation manifest and rebuildable context index",
    statements: &[
        "ALTER TABLE session_recovery_manifest
             ADD COLUMN IF NOT EXISTS event_cursor BIGINT NOT NULL DEFAULT 0",
        "ALTER TABLE session_recovery_manifest
             ADD COLUMN IF NOT EXISTS latest_checkpoint_sequence BIGINT",
        "ALTER TABLE session_recovery_manifest
             ADD COLUMN IF NOT EXISTS latest_checkpoint_event_id TEXT",
        "ALTER TABLE session_recovery_manifest
             ADD COLUMN IF NOT EXISTS index_generation BIGINT NOT NULL DEFAULT 0",
        "ALTER TABLE session_recovery_manifest
             ADD COLUMN IF NOT EXISTS indexed_through_sequence BIGINT",
        "ALTER TABLE session_recovery_manifest
             ADD COLUMN IF NOT EXISTS index_card_count BIGINT NOT NULL DEFAULT 0",
        "ALTER TABLE session_recovery_manifest
             ADD COLUMN IF NOT EXISTS index_pending BOOLEAN NOT NULL DEFAULT FALSE",
        "CREATE TABLE IF NOT EXISTS session_context_index_outbox (
            session_id TEXT NOT NULL REFERENCES session_records(session_id) ON DELETE CASCADE,
            source_sequence BIGINT NOT NULL,
            operation TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts BIGINT NOT NULL DEFAULT 0,
            created_at_ms BIGINT NOT NULL DEFAULT 0,
            updated_at_ms BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY(session_id, source_sequence, operation)
        )",
        "CREATE INDEX IF NOT EXISTS idx_session_context_index_outbox_pending
             ON session_context_index_outbox(status, updated_at_ms, session_id)",
        "CREATE TABLE IF NOT EXISTS session_context_index_cards (
            card_id TEXT PRIMARY KEY,
            parent_card_id TEXT,
            session_id TEXT NOT NULL REFERENCES session_records(session_id) ON DELETE CASCADE,
            source_start_sequence BIGINT NOT NULL,
            source_end_sequence BIGINT NOT NULL,
            source_message_count BIGINT NOT NULL,
            source_digest TEXT NOT NULL,
            summary TEXT NOT NULL,
            scope TEXT NOT NULL,
            authority TEXT NOT NULL,
            generation BIGINT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_session_context_cards_range
             ON session_context_index_cards(
                 session_id, source_start_sequence, source_end_sequence, generation
             )",
        "CREATE INDEX IF NOT EXISTS idx_session_context_cards_parent
             ON session_context_index_cards(session_id, parent_card_id)",
        "UPDATE session_recovery_manifest AS manifest
            SET event_cursor=COALESCE((
                    SELECT MAX(sequence) + 1 FROM session_events
                     WHERE session_id=manifest.session_id
                ), 0),
                latest_checkpoint_sequence=(
                    SELECT MAX(sequence) FROM session_events
                     WHERE session_id=manifest.session_id
                       AND event_type='SessionDomainEvent'
                       AND event_json::jsonb ->> 'kind'=
                           'memory.semantic_checkpoint.created'
                ),
                latest_checkpoint_event_id=(
                    SELECT event_json::jsonb ->> 'event_id' FROM session_events
                     WHERE session_id=manifest.session_id
                       AND event_type='SessionDomainEvent'
                       AND event_json::jsonb ->> 'kind'=
                           'memory.semantic_checkpoint.created'
                     ORDER BY sequence DESC LIMIT 1
                )",
        "CREATE OR REPLACE FUNCTION cowd_session_activation_event_trigger()
         RETURNS TRIGGER LANGUAGE plpgsql AS $$
         BEGIN
             UPDATE session_recovery_manifest
                SET event_cursor=GREATEST(event_cursor, NEW.sequence + 1),
                    latest_checkpoint_sequence=CASE
                        WHEN NEW.event_type='SessionDomainEvent'
                         AND NEW.event_json::jsonb ->> 'kind'=
                             'memory.semantic_checkpoint.created'
                        THEN NEW.sequence ELSE latest_checkpoint_sequence END,
                    latest_checkpoint_event_id=CASE
                        WHEN NEW.event_type='SessionDomainEvent'
                         AND NEW.event_json::jsonb ->> 'kind'=
                             'memory.semantic_checkpoint.created'
                        THEN NEW.event_json::jsonb ->> 'event_id'
                        ELSE latest_checkpoint_event_id END,
                    last_activity_ms=GREATEST(last_activity_ms, NEW.created_at_ms),
                    manifest_revision=manifest_revision + 1
              WHERE session_id=NEW.session_id;
             RETURN NEW;
         END
         $$",
        "DROP TRIGGER IF EXISTS session_activation_event_insert ON session_events",
        "CREATE TRIGGER session_activation_event_insert
             AFTER INSERT ON session_events FOR EACH ROW
             EXECUTE FUNCTION cowd_session_activation_event_trigger()",
        "CREATE OR REPLACE FUNCTION cowd_session_context_index_trigger()
         RETURNS TRIGGER LANGUAGE plpgsql AS $$
         DECLARE
             target_session_id TEXT;
             target_sequence BIGINT;
             target_operation TEXT;
             target_time BIGINT;
         BEGIN
             target_session_id := CASE WHEN TG_OP='DELETE' THEN OLD.session_id ELSE NEW.session_id END;
             target_sequence := 0;
             target_operation := 'reconcile';
             target_time := CASE WHEN TG_OP='DELETE' THEN OLD.created_at_ms ELSE NEW.created_at_ms END;
             IF NOT EXISTS (
                 SELECT 1 FROM session_records
                  WHERE session_id=target_session_id
             ) THEN
                 IF TG_OP='DELETE' THEN
                     RETURN OLD;
                 END IF;
                 RETURN NEW;
             END IF;
             INSERT INTO session_context_index_outbox(
                 session_id, source_sequence, operation, status,
                 created_at_ms, updated_at_ms
             ) VALUES (
                 target_session_id, target_sequence, target_operation, 'pending',
                 target_time, target_time
             )
             ON CONFLICT(session_id, source_sequence, operation) DO UPDATE
                 SET status='pending',
                     updated_at_ms=GREATEST(
                         session_context_index_outbox.updated_at_ms,
                         EXCLUDED.updated_at_ms
                     );
             UPDATE session_recovery_manifest
                SET index_pending=TRUE,
                    manifest_revision=manifest_revision + 1
              WHERE session_id=target_session_id;
             IF TG_OP='DELETE' THEN
                 RETURN OLD;
             END IF;
             RETURN NEW;
         END
         $$",
        "DROP TRIGGER IF EXISTS session_context_index_message_change ON session_messages",
        "CREATE TRIGGER session_context_index_message_change
             AFTER INSERT OR UPDATE OR DELETE ON session_messages FOR EACH ROW
             EXECUTE FUNCTION cowd_session_context_index_trigger()",
    ],
}, PostgresMigrationSpec {
    id: "session.0012.checkpoint-index-outbox",
    domain: SESSION_DOMAIN,
    version: 12,
    description: "enqueue context index reconciliation with semantic checkpoints",
    statements: &[
        "CREATE OR REPLACE FUNCTION cowd_session_activation_event_trigger()
         RETURNS TRIGGER LANGUAGE plpgsql AS $$
         BEGIN
             UPDATE session_recovery_manifest
                SET event_cursor=GREATEST(event_cursor, NEW.sequence + 1),
                    latest_checkpoint_sequence=CASE
                        WHEN NEW.event_type='SessionDomainEvent'
                         AND NEW.event_json::jsonb ->> 'kind'=
                             'memory.semantic_checkpoint.created'
                        THEN NEW.sequence ELSE latest_checkpoint_sequence END,
                    latest_checkpoint_event_id=CASE
                        WHEN NEW.event_type='SessionDomainEvent'
                         AND NEW.event_json::jsonb ->> 'kind'=
                             'memory.semantic_checkpoint.created'
                        THEN NEW.event_json::jsonb ->> 'event_id'
                        ELSE latest_checkpoint_event_id END,
                    index_pending=CASE
                        WHEN NEW.event_type='SessionDomainEvent'
                         AND NEW.event_json::jsonb ->> 'kind'=
                             'memory.semantic_checkpoint.created'
                        THEN TRUE ELSE index_pending END,
                    last_activity_ms=GREATEST(last_activity_ms, NEW.created_at_ms),
                    manifest_revision=manifest_revision + 1
              WHERE session_id=NEW.session_id;
             IF NEW.event_type='SessionDomainEvent'
                AND NEW.event_json::jsonb ->> 'kind'=
                    'memory.semantic_checkpoint.created' THEN
                 INSERT INTO session_context_index_outbox(
                     session_id, source_sequence, operation, status,
                     created_at_ms, updated_at_ms
                 ) VALUES (
                     NEW.session_id, 0, 'reconcile', 'pending',
                     NEW.created_at_ms, NEW.created_at_ms
                 )
                 ON CONFLICT(session_id, source_sequence, operation) DO UPDATE
                     SET status='pending',
                         updated_at_ms=GREATEST(
                             session_context_index_outbox.updated_at_ms,
                             EXCLUDED.updated_at_ms
                         );
             END IF;
             RETURN NEW;
         END
         $$",
    ],
}, PostgresMigrationSpec {
    id: "session.0013.catalog-and-runtime-indexes",
    domain: SESSION_DOMAIN,
    version: 13,
    description: "index owner-scoped catalog pages and batched runtime recovery",
    statements: &[
        "CREATE INDEX IF NOT EXISTS idx_session_records_owner_activity
            ON session_records(
                (metadata_json::jsonb ->> 'owner_principal_id'),
                last_activity DESC,
                session_id ASC
            )",
        "CREATE INDEX IF NOT EXISTS idx_session_runtime_outbox_session_activity
            ON session_runtime_outbox(
                session_id,
                updated_at_ms DESC,
                sequence DESC,
                request_id DESC
            )",
        "CREATE INDEX IF NOT EXISTS idx_session_domain_global_kind
            ON session_events(
                (event_json::jsonb ->> 'kind'),
                session_id,
                sequence
            )
            WHERE event_type='SessionDomainEvent'",
    ],
}, PostgresMigrationSpec {
    id: "session.0014.mutable-presence-projection",
    domain: SESSION_DOMAIN,
    version: 14,
    description: "replace append-only lifecycle snapshots with mutable presence projection",
    statements: &[
        "CREATE TABLE IF NOT EXISTS session_presence_projection (
            session_id TEXT PRIMARY KEY REFERENCES session_records(session_id) ON DELETE CASCADE,
            state TEXT NOT NULL,
            attachments_json JSONB NOT NULL,
            next_sequence BIGINT NOT NULL,
            revision BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL,
            CHECK (jsonb_typeof(attachments_json) = 'array')
        )",
        "DROP TRIGGER IF EXISTS session_recovery_lifecycle_event_change ON session_events",
        "DELETE FROM session_events WHERE event_type='session.lifecycle.v1'",
        "UPDATE session_recovery_manifest AS manifest
            SET active_writer_or_attachment=COALESCE(
                    jsonb_array_length(presence.attachments_json) > 0,
                    FALSE
                )
           FROM session_records AS record
           LEFT JOIN session_presence_projection AS presence
             ON presence.session_id=record.session_id
          WHERE manifest.session_id=record.session_id",
        "CREATE OR REPLACE FUNCTION cowd_session_recovery_presence_trigger()
         RETURNS TRIGGER
         LANGUAGE plpgsql
         AS $$
         DECLARE
             target_session_id TEXT;
             active BOOLEAN;
             observed_at BIGINT;
         BEGIN
             IF TG_OP='DELETE' THEN
                 target_session_id := OLD.session_id;
                 active := FALSE;
                 observed_at := OLD.updated_at_ms;
             ELSE
                 target_session_id := NEW.session_id;
                 active := jsonb_array_length(NEW.attachments_json) > 0;
                 observed_at := NEW.updated_at_ms;
             END IF;
             UPDATE session_recovery_manifest
                SET active_writer_or_attachment=active,
                    last_activity_ms=GREATEST(last_activity_ms, observed_at),
                    manifest_revision=manifest_revision + 1
              WHERE session_id=target_session_id;
             RETURN COALESCE(NEW, OLD);
         END
         $$",
        "DROP TRIGGER IF EXISTS session_recovery_presence_change
             ON session_presence_projection",
        "CREATE TRIGGER session_recovery_presence_change
             AFTER INSERT OR UPDATE OR DELETE ON session_presence_projection
              FOR EACH ROW EXECUTE FUNCTION cowd_session_recovery_presence_trigger()",
    ],
}, PostgresMigrationSpec {
    id: "session.0015.remove-redundant-message-sequence-index",
    domain: SESSION_DOMAIN,
    version: 15,
    description: "remove the message sequence index duplicated by the unique constraint",
    statements: &["DROP INDEX IF EXISTS idx_session_messages_session_sequence"],
}];

#[derive(Clone, Debug)]
pub struct PostgresSessionStore {
    executor: PostgresExecutor,
}

impl PostgresSessionStore {
    pub fn new(executor: PostgresExecutor) -> session::SessionResult<Self> {
        prepare_legacy_session_usage_for_migration(&executor)?;
        executor
            .apply_migrations(SESSION_DOMAIN, SESSION_MIGRATIONS)
            .map_err(storage_error)?;
        Ok(Self { executor })
    }

    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> session::SessionResult<Self> {
        PostgresExecutor::connect(config, resolver)
            .map_err(storage_error)
            .and_then(Self::new)
    }

    #[must_use]
    pub fn executor(&self) -> &PostgresExecutor {
        &self.executor
    }

    pub fn create_session(&self, session: &SessionRecord) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
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

    pub fn get_session(&self, session_id: &str) -> session::SessionResult<Option<SessionRecord>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(SESSION_SELECT_BY_ID, &[&session_id])
            .map_err(postgres_error)?
            .map(|row| row_to_session(&row))
            .transpose()
    }

    pub fn get_sessions_by_ids(
        &self,
        session_ids: &[String],
    ) -> session::SessionResult<Vec<SessionRecord>> {
        if session_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT session_id, platform, chat_id, user_id, model,
                        created_at, last_activity, message_count, reset_policy, metadata_json,
                        input_tokens, output_tokens, estimated_cost_usd, status
                   FROM session_records
                  WHERE session_id = ANY($1)
                  ORDER BY session_id ASC",
                &[&session_ids],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_session)
            .collect()
    }

    pub fn get_session_recovery_manifest(
        &self,
        session_id: &str,
    ) -> session::SessionResult<Option<SessionRecoveryManifest>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT session_id, durable_cursor, event_cursor, history_revision,
                        transcript_messages, transcript_bytes,
                        latest_checkpoint_sequence, latest_checkpoint_event_id,
                        index_generation, indexed_through_sequence, index_card_count,
                        index_pending,
                        in_flight_turn,
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

    pub fn get_session_presence_projection(
        &self,
        session_id: &str,
    ) -> session::SessionResult<Option<SessionPresenceProjection>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT session_id,state,attachments_json::text,next_sequence,revision,updated_at_ms
                   FROM session_presence_projection WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .map(|row| {
                Ok(SessionPresenceProjection {
                    session_id: row.try_get(0).map_err(postgres_error)?,
                    state: row.try_get(1).map_err(postgres_error)?,
                    attachments_json: row.try_get(2).map_err(postgres_error)?,
                    next_sequence: from_i64(
                        row.try_get(3).map_err(postgres_error)?,
                        "presence next sequence",
                    )?,
                    revision: i64_to_u64(
                        row.try_get(4).map_err(postgres_error)?,
                        "presence revision",
                    )?,
                    updated_at_ms: i64_to_u64(
                        row.try_get(5).map_err(postgres_error)?,
                        "presence updated time",
                    )?,
                })
            })
            .transpose()
    }

    pub fn upsert_session_presence_projection(
        &self,
        projection: &SessionPresenceProjection,
    ) -> session::SessionResult<()> {
        let next_sequence = to_i64(projection.next_sequence, "presence next sequence")?;
        let revision = to_u64_i64(projection.revision, "presence revision")?;
        let updated_at_ms = to_u64_i64(projection.updated_at_ms, "presence updated time")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "INSERT INTO session_presence_projection(
                     session_id,state,attachments_json,next_sequence,revision,updated_at_ms
                 ) VALUES ($1,$2,$3::text::jsonb,$4,$5,$6)
                 ON CONFLICT(session_id) DO UPDATE SET
                     state=EXCLUDED.state,
                     attachments_json=EXCLUDED.attachments_json,
                     next_sequence=EXCLUDED.next_sequence,
                     revision=EXCLUDED.revision,
                     updated_at_ms=EXCLUDED.updated_at_ms",
                &[
                    &projection.session_id,
                    &projection.state,
                    &projection.attachments_json,
                    &next_sequence,
                    &revision,
                    &updated_at_ms,
                ],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn compare_and_upsert_session_presence_projection(
        &self,
        projection: &SessionPresenceProjection,
        expected_revision: Option<u64>,
    ) -> session::SessionResult<bool> {
        let next_sequence = to_i64(projection.next_sequence, "presence next sequence")?;
        let revision = to_u64_i64(projection.revision, "presence revision")?;
        let updated_at_ms = to_u64_i64(projection.updated_at_ms, "presence updated time")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let changed = match expected_revision {
            Some(expected_revision) => {
                let expected_revision =
                    to_u64_i64(expected_revision, "presence expected revision")?;
                connection.execute(
                    "UPDATE session_presence_projection
                        SET state=$2,
                            attachments_json=$3::text::jsonb,
                            next_sequence=$4,
                            revision=$5,
                            updated_at_ms=$6
                      WHERE session_id=$1 AND revision=$7",
                    &[
                        &projection.session_id,
                        &projection.state,
                        &projection.attachments_json,
                        &next_sequence,
                        &revision,
                        &updated_at_ms,
                        &expected_revision,
                    ],
                )
            }
            None => connection.execute(
                "INSERT INTO session_presence_projection(
                     session_id,state,attachments_json,next_sequence,revision,updated_at_ms
                 ) VALUES ($1,$2,$3::text::jsonb,$4,$5,$6)
                 ON CONFLICT(session_id) DO NOTHING",
                &[
                    &projection.session_id,
                    &projection.state,
                    &projection.attachments_json,
                    &next_sequence,
                    &revision,
                    &updated_at_ms,
                ],
            ),
        }
        .map_err(postgres_error)?;
        Ok(changed == 1)
    }

    pub fn delete_session_presence_projection(
        &self,
        session_id: &str,
    ) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "DELETE FROM session_presence_projection WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn get_session_recovery_manifests_by_ids(
        &self,
        session_ids: &[String],
    ) -> session::SessionResult<Vec<SessionRecoveryManifest>> {
        if session_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT session_id, durable_cursor, event_cursor, history_revision,
                        transcript_messages, transcript_bytes,
                        latest_checkpoint_sequence, latest_checkpoint_event_id,
                        index_generation, indexed_through_sequence, index_card_count,
                        index_pending, in_flight_turn, pending_approval,
                        active_writer_or_attachment, mission_agent_team_continuation,
                        last_activity_ms, manifest_revision
                   FROM session_recovery_manifest
                  WHERE session_id = ANY($1)
                  ORDER BY session_id ASC",
                &[&session_ids],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_recovery_manifest)
            .collect()
    }

    pub fn rebuild_session_recovery_manifest(
        &self,
        session_id: &str,
        now_ms: u64,
    ) -> session::SessionResult<Option<SessionRecoveryManifest>> {
        let mut connection = self.executor.checkout_background().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let exists = transaction
            .query_one(
                "SELECT EXISTS(
                     SELECT 1 FROM session_records WHERE session_id=$1
                 )",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .get::<_, bool>(0);
        if !exists {
            transaction.commit().map_err(postgres_error)?;
            return Ok(None);
        }
        transaction
            .execute(
                "SELECT cowd_refresh_session_recovery_manifest($1, TRUE)",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        let now_ms = to_u64_i64(now_ms, "manifest rebuild time")?;
        transaction
            .execute(
                "UPDATE session_recovery_manifest
                    SET event_cursor=COALESCE((
                            SELECT MAX(sequence)+1 FROM session_events
                             WHERE session_id=$1
                        ),0),
                        latest_checkpoint_sequence=(
                            SELECT MAX(sequence) FROM session_events
                             WHERE session_id=$1
                               AND event_type='SessionDomainEvent'
                               AND event_json::jsonb ->> 'kind'=
                                   'memory.semantic_checkpoint.created'
                        ),
                        latest_checkpoint_event_id=(
                            SELECT event_json::jsonb ->> 'event_id'
                              FROM session_events
                             WHERE session_id=$1
                               AND event_type='SessionDomainEvent'
                               AND event_json::jsonb ->> 'kind'=
                                   'memory.semantic_checkpoint.created'
                             ORDER BY sequence DESC LIMIT 1
                        ),
                        index_generation=COALESCE((
                            SELECT MAX(generation)
                              FROM session_context_index_cards
                             WHERE session_id=$1
                        ),0),
                        indexed_through_sequence=(
                            SELECT MAX(source_end_sequence)
                              FROM session_context_index_cards
                             WHERE session_id=$1
                        ),
                        index_card_count=COALESCE((
                            SELECT COUNT(*) FROM session_context_index_cards
                             WHERE session_id=$1
                        ),0),
                        index_pending=EXISTS(
                            SELECT 1 FROM session_messages WHERE session_id=$1
                        ) OR EXISTS(
                            SELECT 1 FROM session_events
                             WHERE session_id=$1
                               AND event_type='SessionDomainEvent'
                               AND event_json::jsonb ->> 'kind'=
                                   'memory.semantic_checkpoint.created'
                        ),
                        last_activity_ms=GREATEST(last_activity_ms,$2),
                        manifest_revision=manifest_revision+1
                  WHERE session_id=$1",
                &[&session_id, &now_ms],
            )
            .map_err(postgres_error)?;
        transaction
            .execute(
                "INSERT INTO session_context_index_outbox(
                     session_id,source_sequence,operation,status,
                     created_at_ms,updated_at_ms
                 )
                 SELECT $1,0,'reconcile','pending',$2,$2
                  WHERE EXISTS(
                      SELECT 1 FROM session_messages WHERE session_id=$1
                  )
                 ON CONFLICT(session_id,source_sequence,operation) DO UPDATE
                     SET status='pending',
                         updated_at_ms=GREATEST(
                             session_context_index_outbox.updated_at_ms,
                             EXCLUDED.updated_at_ms
                         )",
                &[&session_id, &now_ms],
            )
            .map_err(postgres_error)?;
        transaction.commit().map_err(postgres_error)?;
        self.get_session_recovery_manifest(session_id)
    }

    pub fn list_active_session_recovery_manifests(
        &self,
        offset: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionRecoveryManifest>> {
        let offset = to_i64(offset, "recovery manifest offset")?;
        let limit = to_i64(limit.max(1), "recovery manifest limit")?;
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT manifest.session_id, manifest.durable_cursor,
                        manifest.event_cursor, manifest.history_revision,
                        manifest.transcript_messages, manifest.transcript_bytes,
                        manifest.latest_checkpoint_sequence,
                        manifest.latest_checkpoint_event_id,
                        manifest.index_generation,
                        manifest.indexed_through_sequence,
                        manifest.index_card_count,
                        manifest.index_pending,
                        manifest.in_flight_turn,
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

    pub fn list_required_session_recovery_manifests(
        &self,
        offset: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionRecoveryManifest>> {
        let offset = to_i64(offset, "required recovery manifest offset")?;
        let limit = to_i64(limit.max(1), "required recovery manifest limit")?;
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT manifest.session_id, manifest.durable_cursor,
                        manifest.event_cursor, manifest.history_revision,
                        manifest.transcript_messages, manifest.transcript_bytes,
                        manifest.latest_checkpoint_sequence,
                        manifest.latest_checkpoint_event_id,
                        manifest.index_generation,
                        manifest.indexed_through_sequence,
                        manifest.index_card_count,
                        manifest.index_pending,
                        manifest.in_flight_turn, manifest.pending_approval,
                        manifest.active_writer_or_attachment,
                        manifest.mission_agent_team_continuation,
                        manifest.last_activity_ms, manifest.manifest_revision
                   FROM session_recovery_manifest AS manifest
                   JOIN session_records AS record
                     ON record.session_id=manifest.session_id
                  WHERE record.status='active'
                    AND (
                        manifest.in_flight_turn
                        OR manifest.pending_approval
                        OR manifest.mission_agent_team_continuation
                    )
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
    ) -> session::SessionResult<SessionRecoveryManifest> {
        let column = match signal {
            SessionRecoverySignal::PendingApproval => "pending_approval",
            SessionRecoverySignal::ActiveWriterOrAttachment => "active_writer_or_attachment",
            SessionRecoverySignal::MissionAgentTeamContinuation => {
                "mission_agent_team_continuation"
            }
        };
        let observed_at_ms = to_u64_i64(observed_at_ms, "recovery observed_at_ms")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let statement = format!(
            "UPDATE session_recovery_manifest
                SET {column}=$2,
                    last_activity_ms=GREATEST(last_activity_ms, $3),
                    manifest_revision=manifest_revision + 1
              WHERE session_id=$1
          RETURNING session_id, durable_cursor, event_cursor, history_revision,
                    transcript_messages, transcript_bytes,
                    latest_checkpoint_sequence, latest_checkpoint_event_id,
                    index_generation, indexed_through_sequence, index_card_count,
                    index_pending, in_flight_turn, pending_approval,
                    active_writer_or_attachment, mission_agent_team_continuation,
                    last_activity_ms, manifest_revision"
        );
        connection
            .query_opt(&statement, &[&session_id, &active, &observed_at_ms])
            .map_err(postgres_error)?
            .map(|row| row_to_recovery_manifest(&row))
            .transpose()?
            .ok_or_else(|| {
                session::SessionError::Store(format!(
                    "session recovery manifest `{session_id}` does not exist"
                ))
            })
    }

    pub fn update_session(&self, session: &SessionRecord) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
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

    pub fn upsert_session(&self, session: &SessionRecord) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
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

    pub fn delete_session(&self, session_id: &str) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "DELETE FROM session_records WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn mark_session_closed(&self, session_id: &str) -> session::SessionResult<()> {
        let now_at = chrono::Utc::now();
        let now = now_at.to_rfc3339();
        let now_ms = now_at.timestamp_millis().max(0);
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
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

    pub fn list_sessions(&self) -> session::SessionResult<Vec<SessionRecord>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
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
    ) -> session::SessionResult<Vec<SessionRecord>> {
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
    ) -> session::SessionResult<Vec<SessionRecord>> {
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
    ) -> session::SessionResult<SessionListPage> {
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
        let owner_principal_id = options
            .owner_principal_id
            .filter(|value| !value.trim().is_empty());
        let visible_session_ids = options.visible_session_ids;
        let unrestricted = options.unrestricted;
        let include_deleted = options.include_deleted;
        let limit = i64::try_from(options.limit.clamp(1, 500))
            .map_err(|_| session::SessionError::Store("session page limit overflow".to_string()))?;
        let offset = i64::try_from(options.offset).map_err(|_| {
            session::SessionError::Store("session page offset overflow".to_string())
        })?;
        let where_clause = "WHERE ($1::text IS NULL OR to_tsvector('simple',
                coalesce(platform, '') || ' ' || coalesce(chat_id, '') || ' ' ||
                coalesce(user_id, '') || ' ' || coalesce(metadata_json, ''))
                @@ websearch_to_tsquery('simple', $1)
                OR platform ILIKE '%' || $1 || '%' OR chat_id ILIKE '%' || $1 || '%')
             AND ($2::text IS NULL OR status = $2)
             AND ($3::text IS NULL OR model = $3)
             AND ($6::boolean
                  OR metadata_json::jsonb ->> 'owner_principal_id' = $4
                  OR session_id = ANY($5::text[]))
             AND ($2::text IS NOT NULL OR $7::boolean
                  OR status NOT IN ('deleted', 'deleting'))";
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let total: i64 = connection
            .query_one(
                &format!("SELECT COUNT(*) FROM session_records {where_clause}"),
                &[
                    &query,
                    &status,
                    &model,
                    &owner_principal_id,
                    &visible_session_ids,
                    &unrestricted,
                    &include_deleted,
                ],
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
                      ORDER BY {sort} {order}, session_id ASC LIMIT $8 OFFSET $9"
                ),
                &[
                    &query,
                    &status,
                    &model,
                    &owner_principal_id,
                    &visible_session_ids,
                    &unrestricted,
                    &include_deleted,
                    &limit,
                    &offset,
                ],
            )
            .map_err(postgres_error)?;
        let records = rows
            .iter()
            .map(row_to_session)
            .collect::<session::SessionResult<_>>()?;
        Ok(SessionListPage {
            records,
            total: usize::try_from(total).map_err(|_| {
                session::SessionError::Store("session page count overflow".to_string())
            })?,
        })
    }

    pub fn session_usage_summary(
        &self,
        recent_limit: usize,
    ) -> session::SessionResult<SessionUsageSummary> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let totals = connection
            .query_one(
                "SELECT COUNT(*),COALESCE(SUM(message_count),0),
                        COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),
                        COALESCE(SUM(estimated_cost_usd),0)
                   FROM session_records
                  WHERE status NOT IN ('deleted','deleting')",
                &[],
            )
            .map_err(postgres_error)?;
        let load_buckets =
            |connection: &mut PostgresConnection,
             column: &str|
             -> session::SessionResult<BTreeMap<String, SessionUsageBucket>> {
                let rows = connection
                    .query(
                        &format!(
                            "SELECT COALESCE(NULLIF(BTRIM({column}),''),'unknown'),COUNT(*),
                                COALESCE(SUM(message_count),0),COALESCE(SUM(input_tokens),0),
                                COALESCE(SUM(output_tokens),0),
                                COALESCE(SUM(estimated_cost_usd),0)
                           FROM session_records
                          WHERE status NOT IN ('deleted','deleting')
                          GROUP BY 1 ORDER BY 1"
                        ),
                        &[],
                    )
                    .map_err(postgres_error)?;
                rows.iter()
                    .map(|row| {
                        let count = row.try_get::<_, i64>(1).map_err(postgres_error)?;
                        Ok((
                            row.try_get(0).map_err(postgres_error)?,
                            SessionUsageBucket {
                                session_count: usize::try_from(count).map_err(|_| {
                                    session::SessionError::Store(
                                        "usage bucket session count overflow".to_string(),
                                    )
                                })?,
                                message_count: row.try_get(2).map_err(postgres_error)?,
                                input_tokens: row.try_get(3).map_err(postgres_error)?,
                                output_tokens: row.try_get(4).map_err(postgres_error)?,
                                estimated_cost_usd: row.try_get(5).map_err(postgres_error)?,
                            },
                        ))
                    })
                    .collect()
            };
        let session_count_i64 = totals.try_get::<_, i64>(0).map_err(postgres_error)?;
        let by_platform = load_buckets(&mut connection, "platform")?;
        let by_model = load_buckets(&mut connection, "model")?;
        drop(connection);
        let recent_sessions = self
            .list_sessions_page(&SessionListOptions {
                unrestricted: true,
                include_deleted: false,
                sort: "last_activity",
                order: "desc",
                limit: recent_limit.clamp(1, 200),
                ..SessionListOptions::default()
            })?
            .records;
        Ok(SessionUsageSummary {
            session_count: usize::try_from(session_count_i64).map_err(|_| {
                session::SessionError::Store("usage session count overflow".to_string())
            })?,
            message_count: totals.try_get(1).map_err(postgres_error)?,
            input_tokens: totals.try_get(2).map_err(postgres_error)?,
            output_tokens: totals.try_get(3).map_err(postgres_error)?,
            estimated_cost_usd: totals.try_get(4).map_err(postgres_error)?,
            by_platform,
            by_model,
            recent_sessions,
        })
    }

    pub fn discover_browsable_sessions(
        &self,
        current_session_id: &str,
        query: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> session::SessionResult<SessionListPage> {
        let query = query.map(str::trim).filter(|query| !query.is_empty());
        let limit = i64::try_from(limit.clamp(1, 100)).map_err(|_| {
            session::SessionError::Store("Session discovery limit overflow".to_string())
        })?;
        let offset = i64::try_from(offset).map_err(|_| {
            session::SessionError::Store("Session discovery offset overflow".to_string())
        })?;
        let authority_clause = r"
            FROM session_records s
            JOIN session_records current ON current.session_id=$1
           WHERE s.status NOT IN ('deleted', 'deleting')
             AND (
                    s.session_id=current.session_id
                 OR (
                        NULLIF(current.metadata_json::jsonb ->> 'workspace_root', '') IS NOT NULL
                    AND s.metadata_json::jsonb ->> 'workspace_root'
                        = current.metadata_json::jsonb ->> 'workspace_root'
                    AND (
                           (
                               NULLIF(current.metadata_json::jsonb ->> 'owner_principal_id', '') IS NOT NULL
                           AND s.metadata_json::jsonb ->> 'owner_principal_id'
                               = current.metadata_json::jsonb ->> 'owner_principal_id'
                           )
                        OR (
                               NULLIF(current.metadata_json::jsonb ->> 'owner_principal_id', '') IS NULL
                           AND NULLIF(current.user_id, '') IS NOT NULL
                           AND s.platform=current.platform
                           AND s.user_id=current.user_id
                           )
                       )
                    )
                 )
             AND (
                    $2::text IS NULL
                 OR to_tsvector('simple',
                        coalesce(s.session_id, '') || ' ' || coalesce(s.platform, '') || ' ' ||
                        coalesce(s.chat_id, '') || ' ' || coalesce(s.metadata_json, ''))
                    @@ websearch_to_tsquery('simple', $2)
                 OR s.session_id ILIKE '%' || $2 || '%'
                 OR s.platform ILIKE '%' || $2 || '%'
                 OR s.chat_id ILIKE '%' || $2 || '%'
                 OR coalesce(s.metadata_json, '') ILIKE '%' || $2 || '%'
                 OR EXISTS (
                        SELECT 1
                          FROM session_messages m
                         WHERE m.session_id=s.session_id
                           AND to_tsvector('simple',
                               coalesce(m.role, '') || ' ' || coalesce(m.content_json, '') || ' ' ||
                               coalesce(m.tool_name, ''))
                               @@ websearch_to_tsquery('simple', $2)
                    )
                 )";
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let total: i64 = connection
            .query_one(
                &format!("SELECT COUNT(*) {authority_clause}"),
                &[&current_session_id, &query],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let rows = connection
            .query(
                &format!(
                    r"SELECT s.session_id, s.platform, s.chat_id, s.user_id, s.model,
                              s.created_at, s.last_activity, s.message_count, s.reset_policy,
                              s.metadata_json, s.input_tokens, s.output_tokens,
                              s.estimated_cost_usd, s.status
                         {authority_clause}
                        ORDER BY s.last_activity DESC, s.session_id ASC
                        LIMIT $3 OFFSET $4"
                ),
                &[&current_session_id, &query, &limit, &offset],
            )
            .map_err(postgres_error)?;
        let records = rows
            .iter()
            .map(row_to_session)
            .collect::<session::SessionResult<Vec<_>>>()?;
        Ok(SessionListPage {
            records,
            total: usize::try_from(total).map_err(|_| {
                session::SessionError::Store("Session discovery count overflow".to_string())
            })?,
        })
    }

    pub fn search_sessions(
        &self,
        query: &str,
        platform: Option<&str>,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionSearchResult>> {
        let limit = i64::try_from(limit.clamp(1, 500)).map_err(|_| {
            session::SessionError::Store("session search limit overflow".to_string())
        })?;
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
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

    pub fn associate_memory(
        &self,
        session_id: &str,
        memory_id: &str,
    ) -> session::SessionResult<()> {
        let created_at = chrono::Utc::now().to_rfc3339();
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "INSERT INTO session_memory_associations(session_id, memory_id, created_at)
                 VALUES ($1,$2,$3) ON CONFLICT(session_id, memory_id) DO NOTHING",
                &[&session_id, &memory_id, &created_at],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn get_session_memories(&self, session_id: &str) -> session::SessionResult<Vec<String>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
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
    ) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "DELETE FROM session_memory_associations WHERE session_id=$1 AND memory_id=$2",
                &[&session_id, &memory_id],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn insert_message(&self, message: &SessionMessage) -> session::SessionResult<()> {
        let sequence = to_i64(message.sequence, "message sequence")?;
        let blocks_count = to_i64(message.blocks_count, "message blocks")?;
        let created_at_ms = i64::try_from(message.created_at_ms)
            .map_err(|_| session::SessionError::Store("message time overflow".to_string()))?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
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
    ) -> session::SessionResult<Vec<SessionMessage>> {
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
    ) -> session::SessionResult<Vec<SessionMessage>> {
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

    pub fn get_messages_in_ranges(
        &self,
        session_id: &str,
        ranges: &[(usize, usize)],
        limit: usize,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        let limit = to_i64(limit.clamp(1, 2_048), "message range limit")?;
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        for &(start, end) in ranges.iter().take(128) {
            if start >= end {
                continue;
            }
            starts.push(to_i64(start, "message range start")?);
            ends.push(to_i64(end, "message range end")?);
        }
        if starts.is_empty() {
            return Ok(Vec::new());
        }
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT stable_message_id, session_id, sequence, role, content_json,
                        blocks_count, tool_use_id, tool_name, token_usage_json, created_at_ms
                   FROM session_messages AS message
                  WHERE session_id=$1
                    AND EXISTS (
                        SELECT 1
                          FROM unnest($2::BIGINT[], $3::BIGINT[])
                               AS selected(start_sequence, end_sequence)
                         WHERE message.sequence >= selected.start_sequence
                           AND message.sequence < selected.end_sequence
                    )
                  ORDER BY sequence ASC
                  LIMIT $4",
                &[&session_id, &starts, &ends, &limit],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_message)
            .collect()
    }

    pub fn get_message_by_stable_id(
        &self,
        session_id: &str,
        stable_message_id: &str,
    ) -> session::SessionResult<Option<SessionMessage>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT stable_message_id, session_id, sequence, role, content_json,
                        blocks_count, tool_use_id, tool_name, token_usage_json,
                        created_at_ms
                   FROM session_messages
                  WHERE session_id=$1 AND stable_message_id=$2",
                &[&session_id, &stable_message_id],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_message(&row))
            .transpose()
    }

    pub fn get_message_by_sequence(
        &self,
        session_id: &str,
        sequence: usize,
    ) -> session::SessionResult<Option<SessionMessage>> {
        let sequence = to_i64(sequence, "message sequence")?;
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT stable_message_id, session_id, sequence, role, content_json,
                        blocks_count, tool_use_id, tool_name, token_usage_json,
                        created_at_ms
                   FROM session_messages
                  WHERE session_id=$1 AND sequence=$2",
                &[&session_id, &sequence],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_message(&row))
            .transpose()
    }

    pub fn get_message_metadata_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionMessageMetadata>> {
        let from_sequence = to_i64(from_sequence, "message sequence")?;
        let limit = to_i64(limit.clamp(1, 2_048), "message metadata limit")?;
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT stable_message_id, session_id, sequence, role,
                        blocks_count, tool_use_id, tool_name, created_at_ms,
                        octet_length(content_json)::BIGINT
                   FROM session_messages
                  WHERE session_id=$1 AND sequence >= $2
                  ORDER BY sequence ASC
                  LIMIT $3",
                &[&session_id, &from_sequence, &limit],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_message_metadata)
            .collect()
    }

    pub fn get_context_index_cards(
        &self,
        session_id: &str,
        limit: usize,
    ) -> session::SessionResult<Vec<ContextIndexCard>> {
        let limit = to_i64(limit.clamp(1, 2_048), "context index card limit")?;
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT card_id, parent_card_id, session_id,
                        source_start_sequence, source_end_sequence,
                        source_message_count, source_digest, summary, scope,
                        authority, generation, created_at_ms, updated_at_ms
                   FROM session_context_index_cards
                  WHERE session_id=$1
                  ORDER BY
                      CASE WHEN parent_card_id IS NULL THEN 0 ELSE 1 END,
                      source_start_sequence DESC
                  LIMIT $2",
                &[&session_id, &limit],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_context_index_card)
            .collect()
    }

    pub fn reconcile_session_context_index(
        &self,
        session_id: &str,
        card_span: usize,
        parent_span: usize,
        now_ms: u64,
    ) -> session::SessionResult<ContextIndexCoverage> {
        let mut connection = self.executor.checkout_background().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        let messages = transaction
            .query(
                "SELECT stable_message_id, session_id, sequence, role, content_json,
                        blocks_count, tool_use_id, tool_name, token_usage_json,
                        created_at_ms
                   FROM session_messages
                  WHERE session_id=$1
                  ORDER BY sequence ASC",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_message)
            .collect::<session::SessionResult<Vec<_>>>()?;
        let current_generation: i64 = transaction
            .query_one(
                "SELECT index_generation FROM session_recovery_manifest
                  WHERE session_id=$1 FOR UPDATE",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let generation =
            i64_to_u64(current_generation, "context index generation")?.saturating_add(1);
        let cards = build_context_index_cards(
            session_id,
            &messages,
            card_span,
            parent_span,
            generation,
            now_ms,
        );
        transaction
            .execute(
                "DELETE FROM session_context_index_cards WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        for card in &cards {
            transaction
                .execute(
                    "INSERT INTO session_context_index_cards(
                         card_id, parent_card_id, session_id,
                         source_start_sequence, source_end_sequence,
                         source_message_count, source_digest, summary, scope,
                         authority, generation, created_at_ms, updated_at_ms
                     ) VALUES (
                         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
                     )",
                    &[
                        &card.card_id,
                        &card.parent_card_id,
                        &card.session_id,
                        &to_i64(card.source_start_sequence, "card source start")?,
                        &to_i64(card.source_end_sequence, "card source end")?,
                        &to_i64(card.source_message_count, "card source count")?,
                        &card.source_digest,
                        &card.summary,
                        &card.scope,
                        &card.authority,
                        &to_u64_i64(card.generation, "card generation")?,
                        &to_u64_i64(card.created_at_ms, "card created time")?,
                        &to_u64_i64(card.updated_at_ms, "card updated time")?,
                    ],
                )
                .map_err(postgres_error)?;
        }
        let indexed_through_sequence = messages.last().map(|message| message.sequence);
        transaction
            .execute(
                "UPDATE session_recovery_manifest
                    SET index_generation=$2,
                        indexed_through_sequence=$3,
                        index_card_count=$4,
                        index_pending=FALSE,
                        manifest_revision=manifest_revision + 1
                  WHERE session_id=$1",
                &[
                    &session_id,
                    &to_u64_i64(generation, "context index generation")?,
                    &indexed_through_sequence
                        .map(|value| to_i64(value, "indexed through sequence"))
                        .transpose()?,
                    &to_i64(cards.len(), "context card count")?,
                ],
            )
            .map_err(postgres_error)?;
        transaction
            .execute(
                "UPDATE session_context_index_outbox
                    SET status='completed', attempts=attempts + 1,
                        updated_at_ms=$2
                  WHERE session_id=$1 AND status!='completed'",
                &[
                    &session_id,
                    &to_u64_i64(now_ms, "context index update time")?,
                ],
            )
            .map_err(postgres_error)?;
        transaction.commit().map_err(postgres_error)?;
        let leaf_cards = cards
            .iter()
            .filter(|card| card.parent_card_id.is_some() || cards.len() == 1)
            .collect::<Vec<_>>();
        let covered_messages = leaf_cards
            .iter()
            .map(|card| card.source_message_count)
            .sum();
        Ok(ContextIndexCoverage {
            session_id: session_id.to_string(),
            source_messages: messages.len(),
            covered_messages,
            card_count: cards.len(),
            indexed_through_sequence,
            generation,
            complete: covered_messages == messages.len(),
            source_digest: context_index_source_digest(&messages),
            card_digest: context_index_card_digest(&cards),
        })
    }

    pub fn get_message_count(&self, session_id: &str) -> session::SessionResult<usize> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let count: i64 = connection
            .query_one(
                "SELECT COUNT(*) FROM session_messages WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        usize::try_from(count)
            .map_err(|_| session::SessionError::Store("message count overflow".to_string()))
    }

    pub fn delete_messages_from(
        &self,
        session_id: &str,
        from_sequence: usize,
    ) -> session::SessionResult<usize> {
        let from_sequence = to_i64(from_sequence, "message sequence")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let deleted = connection
            .execute(
                "DELETE FROM session_messages WHERE session_id=$1 AND sequence >= $2",
                &[&session_id, &from_sequence],
            )
            .map_err(postgres_error)?;
        Ok(deleted as usize)
    }

    pub fn get_all_messages(
        &self,
        session_id: &str,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        self.query_messages(
            "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                    tool_use_id, tool_name, token_usage_json, created_at_ms
               FROM session_messages WHERE session_id=$1 ORDER BY sequence ASC",
            &[&session_id],
        )
    }

    pub fn insert_messages_batch(&self, messages: &[SessionMessage]) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        for message in messages {
            insert_message_tx(&mut transaction, message)?;
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(())
    }

    pub fn copy_session_messages_at_cutoff(
        &self,
        source_session_id: &str,
        target_session_id: &str,
        source_message_count: usize,
    ) -> session::SessionResult<usize> {
        if source_session_id.trim().is_empty()
            || target_session_id.trim().is_empty()
            || source_session_id == target_session_id
        {
            return Err(session::SessionError::Store(
                "branch copy requires distinct non-empty source and target sessions".to_string(),
            ));
        }
        let cutoff = to_i64(source_message_count, "branch cutoff")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        // Lock in stable lexical order so concurrent reciprocal branch requests
        // cannot deadlock.
        let (first, second) = if source_session_id < target_session_id {
            (source_session_id, target_session_id)
        } else {
            (target_session_id, source_session_id)
        };
        let rows = transaction
            .query(
                "SELECT session_id FROM session_records
                  WHERE session_id IN ($1,$2)
                  ORDER BY session_id FOR UPDATE",
                &[&first, &second],
            )
            .map_err(postgres_error)?;
        if rows.len() != 2 {
            return Err(session::SessionError::Store(
                "branch source and target sessions must both exist".to_string(),
            ));
        }
        let target_count: i64 = transaction
            .query_one(
                "SELECT COUNT(*) FROM session_messages WHERE session_id=$1",
                &[&target_session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        if target_count != 0 {
            return Err(session::SessionError::Store(format!(
                "branch target `{target_session_id}` already contains messages"
            )));
        }
        let copied = transaction
            .execute(
                "INSERT INTO session_messages(
                     stable_message_id,session_id,sequence,role,content_json,blocks_count,
                     tool_use_id,tool_name,token_usage_json,created_at_ms
                 )
                 SELECT 'branch:' || $2 || ':' || stable_message_id,
                        $2,sequence,role,content_json,blocks_count,
                        tool_use_id,tool_name,token_usage_json,created_at_ms
                   FROM session_messages
                  WHERE session_id=$1 AND sequence < $3
                  ORDER BY sequence",
                &[&source_session_id, &target_session_id, &cutoff],
            )
            .map_err(postgres_error)?;
        let last_created_at: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(created_at_ms),0)
                   FROM session_messages WHERE session_id=$1",
                &[&target_session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        refresh_session_message_summary_tx(
            &mut transaction,
            target_session_id,
            i64_to_u64(last_created_at.max(0), "branch message time")?,
        )?;
        refresh_session_usage_summary_tx(&mut transaction, target_session_id)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(copied as usize)
    }

    pub fn branch_session_at_cutoff(
        &self,
        request: &SessionBranchRequest,
    ) -> session::SessionResult<SessionBranchResult> {
        validate_mission_request(&request.mission_outbox)?;
        if request.operation_id.trim().is_empty()
            || request.source_session_id.trim().is_empty()
            || request.target.session_id.trim().is_empty()
            || request.source_session_id == request.target.session_id
            || request.mission_outbox.session_id != request.target.session_id
        {
            return Err(session::SessionError::Store(
                "branch requires distinct source/target identities and a target-bound mission intent"
                    .to_string(),
            ));
        }

        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let source = transaction
            .query_opt(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&request.source_session_id],
            )
            .map_err(postgres_error)?;
        if source.is_none() {
            return Err(session::SessionError::Store(format!(
                "branch source `{}` does not exist",
                request.source_session_id
            )));
        }
        if let Some(existing) =
            query_branch_activation_tx(&mut transaction, &request.operation_id, true)?
        {
            if existing.source_session_id != request.source_session_id
                || existing.target_session_id != request.target.session_id
                || existing.source_message_count != request.source_message_count
            {
                return Err(session::SessionError::Store(format!(
                    "branch operation `{}` is bound to another source/cutoff/target",
                    request.operation_id
                )));
            }
            let target = transaction
                .query_opt(
                    "SELECT session_id,platform,chat_id,user_id,model,created_at,last_activity,
                            message_count,reset_policy,metadata_json,input_tokens,output_tokens,
                            estimated_cost_usd,status
                       FROM session_records WHERE session_id=$1",
                    &[&existing.target_session_id],
                )
                .map_err(postgres_error)?
                .map(|row| row_to_session(&row))
                .transpose()?
                .ok_or_else(|| {
                    session::SessionError::Store(format!(
                        "branch operation `{}` lost target `{}`",
                        existing.operation_id, existing.target_session_id
                    ))
                })?;
            let copied_message_count =
                usize::try_from(target.message_count.max(0)).map_err(|_| {
                    session::SessionError::Store(
                        "branch target message count exceeds usize".to_string(),
                    )
                })?;
            transaction.commit().map_err(postgres_error)?;
            return Ok(SessionBranchResult {
                target,
                copied_message_count,
                source_message_count: existing.source_message_count,
                activation: existing,
            });
        }
        let target_exists: bool = transaction
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM session_records WHERE session_id=$1)",
                &[&request.target.session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        if target_exists {
            return Err(session::SessionError::Store(format!(
                "branch target `{}` already exists",
                request.target.session_id
            )));
        }

        let source_count: i64 = transaction
            .query_one(
                "SELECT COUNT(*) FROM session_messages WHERE session_id=$1",
                &[&request.source_session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let source_count = from_i64(source_count, "branch source message count")?;
        let cutoff = request.source_message_count;
        if cutoff > source_count {
            return Err(session::SessionError::Store(format!(
                "branch cutoff {cutoff} exceeds source message count {source_count}"
            )));
        }
        let cutoff_i64 = to_i64(cutoff, "branch cutoff")?;

        transaction
            .execute(
                "INSERT INTO session_records(
                     session_id,platform,chat_id,user_id,model,created_at,last_activity,
                     message_count,reset_policy,metadata_json,input_tokens,output_tokens,
                     estimated_cost_usd,status,created_at_ms,updated_at_ms
                 ) VALUES($1,$2,$3,$4,$5,$6,$7,0,$8,$9,0,0,0,$10,
                     cowd_safe_session_epoch_ms($6),cowd_safe_session_epoch_ms($7))",
                &[
                    &request.target.session_id,
                    &request.target.platform,
                    &request.target.chat_id,
                    &request.target.user_id,
                    &request.target.model,
                    &request.target.created_at,
                    &request.target.last_activity,
                    &request.target.reset_policy,
                    &request.target.metadata_json,
                    &request.target.status,
                ],
            )
            .map_err(postgres_error)?;
        let copied = transaction
            .execute(
                "INSERT INTO session_messages(
                     stable_message_id,session_id,sequence,role,content_json,blocks_count,
                     tool_use_id,tool_name,token_usage_json,created_at_ms
                 )
                 SELECT 'branch:' || $2 || ':' || stable_message_id,
                        $2,sequence,role,content_json,blocks_count,
                        tool_use_id,tool_name,token_usage_json,created_at_ms
                   FROM session_messages
                  WHERE session_id=$1 AND sequence < $3
                  ORDER BY sequence",
                &[
                    &request.source_session_id,
                    &request.target.session_id,
                    &cutoff_i64,
                ],
            )
            .map_err(postgres_error)?;
        let copied = usize::try_from(copied).map_err(|_| {
            session::SessionError::Store("branch copied message count exceeds usize".to_string())
        })?;
        let last_created_at: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(created_at_ms),0)
                   FROM session_messages WHERE session_id=$1",
                &[&request.target.session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        refresh_session_message_summary_tx(
            &mut transaction,
            &request.target.session_id,
            i64_to_u64(last_created_at.max(0), "branch message time")?,
        )?;
        refresh_session_usage_summary_tx(&mut transaction, &request.target.session_id)?;
        insert_mission_outbox_tx(&mut transaction, &request.mission_outbox)?;

        for (session_id, event_type, event_json) in [
            (
                request.source_session_id.as_str(),
                "SessionBranched",
                request.source_event_json.as_str(),
            ),
            (
                request.target.session_id.as_str(),
                "BranchCreated",
                request.target_event_json.as_str(),
            ),
        ] {
            let sequence: i64 = transaction
                .query_one(
                    "SELECT COALESCE(MAX(sequence) + 1, 0)
                       FROM session_events WHERE session_id=$1",
                    &[&session_id],
                )
                .map_err(postgres_error)?
                .try_get(0)
                .map_err(postgres_error)?;
            let stored_sequence = from_i64(sequence, "branch event sequence")?;
            let event = SessionEvent {
                session_id: session_id.to_string(),
                event_type: event_type.to_string(),
                event_json: event_json.to_string(),
                sequence: stored_sequence,
                created_at_ms: request.created_at_ms,
            };
            let allocated_json = event_json_with_allocated_sequence(&event, stored_sequence)?;
            transaction
                .execute(
                    "INSERT INTO session_events(
                         session_id,sequence,event_type,event_json,created_at_ms
                     ) VALUES($1,$2,$3,$4,$5)",
                    &[
                        &session_id,
                        &sequence,
                        &event_type,
                        &allocated_json,
                        &to_u64_i64(request.created_at_ms, "branch event time")?,
                    ],
                )
                .map_err(postgres_error)?;
        }
        transaction
            .execute(
                "INSERT INTO session_branch_activations(
                     operation_id,source_session_id,target_session_id,source_message_count,
                     phase,created_at_ms,updated_at_ms,last_error,revision
                 ) VALUES($1,$2,$3,$4,'branch_committed',$5,$5,NULL,0)",
                &[
                    &request.operation_id,
                    &request.source_session_id,
                    &request.target.session_id,
                    &cutoff_i64,
                    &to_u64_i64(request.created_at_ms, "branch activation time")?,
                ],
            )
            .map_err(postgres_error)?;
        let activation =
            query_branch_activation_tx(&mut transaction, &request.operation_id, false)?
                .ok_or_else(|| {
                    session::SessionError::Store(
                        "branch transaction produced no activation receipt".to_string(),
                    )
                })?;
        transaction.commit().map_err(postgres_error)?;

        let mut target = request.target.clone();
        target.message_count = i64::try_from(copied).map_err(|_| {
            session::SessionError::Store("branch message count exceeds i64".to_string())
        })?;
        Ok(SessionBranchResult {
            target,
            copied_message_count: copied,
            source_message_count: cutoff,
            activation,
        })
    }

    pub fn get_session_branch_activation(
        &self,
        operation_id: &str,
    ) -> session::SessionResult<Option<SessionBranchActivation>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT operation_id,source_session_id,target_session_id,
                        source_message_count,phase,created_at_ms,updated_at_ms,
                        last_error,revision
                   FROM session_branch_activations WHERE operation_id=$1",
                &[&operation_id],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_branch_activation(&row))
            .transpose()
    }

    pub fn list_recoverable_session_branch_activations(
        &self,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionBranchActivation>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT operation_id,source_session_id,target_session_id,
                        source_message_count,phase,created_at_ms,updated_at_ms,
                        last_error,revision
                   FROM session_branch_activations
                  WHERE phase != 'activated'
                  ORDER BY updated_at_ms ASC,operation_id ASC LIMIT $1",
                &[&to_i64(limit.max(1), "branch activation recovery limit")?],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_branch_activation)
            .collect()
    }

    pub fn transition_session_branch_activation(
        &self,
        transition: &SessionBranchActivationTransition,
    ) -> session::SessionResult<SessionBranchActivation> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current = query_branch_activation_tx(&mut transaction, &transition.operation_id, true)?
            .ok_or_else(|| {
                session::SessionError::Store(format!(
                    "Session branch activation `{}` does not exist",
                    transition.operation_id
                ))
            })?;
        transition.validate(&current)?;
        let changed = transaction
            .execute(
                "UPDATE session_branch_activations
                    SET phase=$1,updated_at_ms=$2,last_error=$3,revision=revision+1
                  WHERE operation_id=$4 AND phase=$5 AND revision=$6",
                &[
                    &transition.next_phase.as_str(),
                    &to_u64_i64(
                        transition.updated_at_ms,
                        "branch activation transition time",
                    )?,
                    &transition.error,
                    &transition.operation_id,
                    &transition.expected_phase.as_str(),
                    &to_u64_i64(transition.expected_revision, "branch activation revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "Session branch activation `{}` changed during transition",
                transition.operation_id
            )));
        }
        let activation =
            query_branch_activation_tx(&mut transaction, &transition.operation_id, false)?
                .ok_or_else(|| {
                    session::SessionError::Store(format!(
                        "Session branch activation `{}` disappeared after transition",
                        transition.operation_id
                    ))
                })?;
        transaction.commit().map_err(postgres_error)?;
        Ok(activation)
    }

    pub fn commit_terminal_transcript_if_fenced(
        &self,
        request: &SessionTerminalTranscriptCommit,
    ) -> session::SessionResult<SessionTerminalTranscriptReceipt> {
        validate_terminal_transcript(
            &request.terminal_message_id,
            &request.ingress_message_id,
            &request.session_id,
            &request.messages,
        )?;
        validate_terminal_commit(request)?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let admission = query_input_admission_tx(&mut transaction, &request.session_id, true)?
            .ok_or_else(|| {
                session::SessionError::StaleExecutionFence(format!(
                    "session `{}` no longer exists",
                    request.session_id
                ))
            })?;
        let current = runtime_outbox_for_update(&mut transaction, &request.fence.request_id)?;
        if current.status == SessionRuntimeInputStatus::Completed
            && current.runtime_commit_cursor == Some(request.runtime_commit_cursor)
        {
            if current.session_id != request.session_id
                || current.message_id != request.ingress_message_id
                || current.turn_id != request.turn_id
                || current.sequence != request.fence.input_sequence
                || current.session_generation != request.fence.session_generation
                || current.claim_owner.as_deref() != Some(request.fence.claim_owner.as_str())
                || current.claim_token.as_deref() != Some(request.fence.claim_token.as_str())
                || current.claim_fence_epoch != Some(request.fence.claim_fence_epoch)
            {
                return Err(session::SessionError::StaleExecutionFence(format!(
                    "completed input `{}` identity does not match terminal replay",
                    request.fence.request_id
                )));
            }
            let messages = load_committed_terminal_transcript_tx(
                &mut transaction,
                &request.terminal_message_id,
                &request.messages,
            )?;
            transaction.commit().map_err(postgres_error)?;
            return Ok(SessionTerminalTranscriptReceipt {
                messages,
                inserted: false,
                input: current,
            });
        }
        let fence_valid = current.session_id == request.session_id
            && current.message_id == request.ingress_message_id
            && current.turn_id == request.turn_id
            && current.sequence == request.fence.input_sequence
            && current.status == SessionRuntimeInputStatus::Running
            && current.session_generation == request.fence.session_generation
            && admission.generation == request.fence.session_generation
            && admission.open
            && current.claim_owner.as_deref() == Some(request.fence.claim_owner.as_str())
            && current.claim_token.as_deref() == Some(request.fence.claim_token.as_str())
            && current.claim_fence_epoch == Some(request.fence.claim_fence_epoch)
            && current
                .claim_expires_at_ms
                .is_some_and(|expires| expires > request.created_at_ms);
        if !fence_valid {
            return Err(session::SessionError::StaleExecutionFence(format!(
                "request={} generation={} claim_fence_epoch={} current_status={:?} current_revision={}",
                request.fence.request_id,
                request.fence.session_generation,
                request.fence.claim_fence_epoch,
                current.status,
                current.revision
            )));
        }
        let newest_pending_sequence = transaction
            .query_one(
                "SELECT MAX(sequence)
                   FROM session_runtime_outbox
                  WHERE session_id=$1 AND session_generation=$2
                    AND sequence>$3
                    AND status NOT IN (
                      'rejected_duplicate','rejected_policy','completed',
                      'supplemented','failed','cancelled','expired'
                    )
                    AND decision IN (
                      'supplement_current_turn',
                      'interrupt_and_replan',
                      'control_or_approval'
                    )",
                &[
                    &request.session_id,
                    &to_u64_i64(request.fence.session_generation, "session generation")?,
                    &to_i64(request.fence.input_sequence, "input sequence")?,
                ],
            )
            .map_err(postgres_error)?
            .try_get::<_, Option<i64>>(0)
            .map_err(postgres_error)?
            .map(|value| value.max(0) as usize);
        if newest_pending_sequence
            .is_some_and(|sequence| sequence > request.consumed_input_sequence)
        {
            return Err(session::SessionError::StaleExecutionFence(format!(
                "terminal input cursor {} is behind pending Session input {}",
                request.consumed_input_sequence,
                newest_pending_sequence.unwrap_or_default()
            )));
        }
        let consumed_rows = transaction
            .query(
                "SELECT request_id
                   FROM session_runtime_outbox
                  WHERE session_id=$1 AND session_generation=$2
                    AND sequence>$3 AND sequence<=$4
                    AND status IN ('accepted','classified','queued','reclassified')
                    AND decision IN (
                      'supplement_current_turn',
                      'interrupt_and_replan',
                      'control_or_approval'
                    )
                  ORDER BY sequence ASC
                  FOR UPDATE",
                &[
                    &request.session_id,
                    &to_u64_i64(request.fence.session_generation, "session generation")?,
                    &to_i64(request.fence.input_sequence, "input sequence")?,
                    &to_i64(request.consumed_input_sequence, "consumed input sequence")?,
                ],
            )
            .map_err(postgres_error)?;
        for row in consumed_rows {
            let request_id = row.try_get::<_, String>(0).map_err(postgres_error)?;
            let before = runtime_outbox_tx(&mut transaction, &request_id)?.ok_or_else(|| {
                session::SessionError::Store(format!(
                    "consumed Session input `{request_id}` disappeared during terminal commit"
                ))
            })?;
            let changed = transaction
                .execute(
                    "UPDATE session_runtime_outbox
                        SET status='supplemented',terminal_at_ms=$1,
                            claim_owner=NULL,claim_token=NULL,
                            claim_fence_epoch=NULL,claim_expires_at_ms=NULL,
                            failure_class=NULL,last_error=NULL,
                            updated_at_ms=$1,revision=revision+1
                      WHERE request_id=$2 AND revision=$3
                        AND status IN ('accepted','classified','queued','reclassified')",
                    &[
                        &to_u64_i64(request.created_at_ms, "terminal commit time")?,
                        &request_id,
                        &to_u64_i64(before.revision, "input revision")?,
                    ],
                )
                .map_err(postgres_error)?;
            if changed != 1 {
                return Err(session::SessionError::StaleExecutionFence(format!(
                    "consumed Session input `{request_id}` changed during terminal commit"
                )));
            }
            let supplemented =
                runtime_outbox_tx(&mut transaction, &request_id)?.ok_or_else(|| {
                    session::SessionError::Store(format!(
                        "supplemented Session input `{request_id}` disappeared"
                    ))
                })?;
            append_runtime_history_tx(
                &mut transaction,
                &supplemented,
                "terminal_input_cursor_commit",
                Some(&request.fence.claim_owner),
                Some(before.revision),
                before.status,
                SessionRuntimeInputStatus::Supplemented,
                None,
                request.created_at_ms,
            )?;
            append_input_timeline_event_tx(
                &mut transaction,
                &request_from_outbox(&supplemented),
                &supplemented.session_id,
                supplemented.sequence,
                SessionRuntimeInputStatus::Supplemented.timeline_event_kind(),
                SessionRuntimeInputStatus::Supplemented,
                Some(&request.fence.claim_owner),
                None,
                request.created_at_ms,
            )?;
        }
        let (messages, inserted) = append_terminal_transcript_tx(
            &mut transaction,
            &request.terminal_message_id,
            &request.ingress_message_id,
            &request.session_id,
            &request.messages,
            request.created_at_ms,
        )?;
        let terminal_status = SessionRuntimeInputStatus::Completed.as_str();
        let changed = transaction
            .execute(
                "UPDATE session_runtime_outbox
                    SET status=$1,runtime_commit_cursor=$2,
                        claim_expires_at_ms=NULL,terminal_at_ms=$3,
                        failure_class=NULL,last_error=NULL,updated_at_ms=$3,revision=revision+1
                  WHERE request_id=$4 AND sequence=$5 AND status='running'
                    AND session_generation=$6
                    AND claim_owner=$7 AND claim_token=$8
                    AND claim_fence_epoch=$9 AND revision=$10",
                &[
                    &terminal_status,
                    &to_u64_i64(request.runtime_commit_cursor, "runtime commit cursor")?,
                    &to_u64_i64(request.created_at_ms, "terminal commit time")?,
                    &request.fence.request_id,
                    &to_i64(request.fence.input_sequence, "input sequence")?,
                    &to_u64_i64(request.fence.session_generation, "session generation")?,
                    &request.fence.claim_owner,
                    &request.fence.claim_token,
                    &to_u64_i64(request.fence.claim_fence_epoch, "claim fence epoch")?,
                    &to_u64_i64(current.revision, "input revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::StaleExecutionFence(format!(
                "input `{}` changed during terminal commit",
                request.fence.request_id
            )));
        }
        let completed = runtime_outbox_tx(&mut transaction, &request.fence.request_id)?
            .ok_or_else(|| {
                session::SessionError::Store(format!(
                    "completed input `{}` disappeared",
                    request.fence.request_id
                ))
            })?;
        append_runtime_history_tx(
            &mut transaction,
            &completed,
            "terminal_commit",
            Some(&request.fence.claim_owner),
            Some(current.revision),
            SessionRuntimeInputStatus::Running,
            SessionRuntimeInputStatus::Completed,
            None,
            request.created_at_ms,
        )?;
        append_input_timeline_event_tx(
            &mut transaction,
            &request_from_outbox(&completed),
            &completed.session_id,
            completed.sequence,
            SessionRuntimeInputStatus::Completed.timeline_event_kind(),
            SessionRuntimeInputStatus::Completed,
            Some(&request.fence.claim_owner),
            None,
            request.created_at_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(SessionTerminalTranscriptReceipt {
            messages,
            inserted,
            input: completed,
        })
    }

    pub fn search_messages(
        &self,
        query: &str,
        session_id: Option<&str>,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionMessage>> {
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
    ) -> session::SessionResult<Vec<SessionMessage>> {
        if session_ids.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let scope = serde_json::to_string(session_ids).map_err(|error| {
            session::SessionError::Store(format!("encode search session scope: {error}"))
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

    pub fn search_messages_visible(
        &self,
        query: &str,
        owner_principal_id: Option<&str>,
        visible_session_ids: &[String],
        unrestricted: bool,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        let limit = to_i64(limit.clamp(1, 500), "message search limit")?;
        self.query_messages(
            "SELECT message.stable_message_id, message.session_id, message.sequence,
                    message.role, message.content_json, message.blocks_count,
                    message.tool_use_id, message.tool_name,
                    message.token_usage_json, message.created_at_ms
               FROM session_messages AS message
               JOIN session_records AS session ON session.session_id=message.session_id
              WHERE session.status NOT IN ('deleted','deleting')
                AND ($4::boolean
                     OR session.metadata_json::jsonb ->> 'owner_principal_id'=$2
                     OR session.session_id=ANY($3::text[]))
                AND (to_tsvector('simple',
                         coalesce(message.role,'') || ' ' ||
                         coalesce(message.content_json,'') || ' ' ||
                         coalesce(message.tool_name,''))
                     @@ websearch_to_tsquery('simple', $1)
                     OR message.content_json ILIKE '%' || $1 || '%')
              ORDER BY message.created_at_ms DESC,message.session_id,message.sequence
              LIMIT $5",
            &[
                &query,
                &owner_principal_id,
                &visible_session_ids,
                &unrestricted,
                &limit,
            ],
        )
    }

    pub fn append_event(&self, event: &SessionEvent) -> session::SessionResult<()> {
        let sequence = to_i64(event.sequence, "event sequence")?;
        let created_at_ms = i64::try_from(event.created_at_ms)
            .map_err(|_| session::SessionError::Store("event time overflow".to_string()))?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
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
    ) -> session::SessionResult<Vec<SessionEvent>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let session_id = events[0].session_id.as_str();
        if session_id.trim().is_empty() || events.iter().any(|event| event.session_id != session_id)
        {
            return Err(session::SessionError::Store(
                "session event batch must have one non-empty session id".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
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
                    session::SessionError::Store("event batch index overflow".to_string())
                })?)
                .ok_or_else(|| {
                    session::SessionError::Store("event sequence overflow".to_string())
                })?;
            let created_at_ms = i64::try_from(event.created_at_ms)
                .map_err(|_| session::SessionError::Store("event time overflow".to_string()))?;
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
    ) -> session::SessionResult<SessionEvent> {
        self.append_events_allocating_sequence(std::slice::from_ref(event))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                session::SessionError::Store("event allocation returned no row".to_string())
            })
    }

    pub fn append_session_domain_event_if_absent_allocating_sequence(
        &self,
        event: &SessionEvent,
        event_id: &str,
    ) -> session::SessionResult<(SessionEvent, bool)> {
        if event.event_type != session::SESSION_DOMAIN_EVENT_TYPE || event_id.trim().is_empty() {
            return Err(session::SessionError::Store(
                "idempotent domain append requires SessionDomainEvent and a non-empty event_id"
                    .to_string(),
            ));
        }
        let encoded_event_id = serde_json::from_str::<serde_json::Value>(&event.event_json)
            .ok()
            .and_then(|value| {
                value
                    .get("event_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .ok_or_else(|| {
                session::SessionError::Store(
                    "idempotent domain append requires event_json.event_id".to_string(),
                )
            })?;
        if encoded_event_id != event_id {
            return Err(session::SessionError::Store(
                "idempotent domain append event_id does not match event_json".to_string(),
            ));
        }

        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&event.session_id],
            )
            .map_err(postgres_error)?;
        if let Some(row) = transaction
            .query_opt(
                "SELECT session_id, event_type, event_json, sequence, created_at_ms
                   FROM session_events
                  WHERE session_id=$1
                    AND event_type=$2
                    AND event_json::jsonb ->> 'event_id'=$3
                  LIMIT 1",
                &[
                    &event.session_id,
                    &session::SESSION_DOMAIN_EVENT_TYPE,
                    &event_id,
                ],
            )
            .map_err(postgres_error)?
        {
            let existing = row_to_event(&row)?;
            if !SessionDomainEvent::semantically_equivalent(&existing, event).map_err(|error| {
                session::SessionError::Store(format!(
                    "failed to compare idempotent session-domain event content: {error}"
                ))
            })? {
                return Err(session::SessionError::IdempotencyConflict {
                    namespace: "session_domain_event",
                    key: event_id.to_string(),
                });
            }
            transaction.commit().map_err(postgres_error)?;
            return Ok((existing, true));
        }

        let sequence: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id=$1",
                &[&event.session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
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
                    &to_u64_i64(event.created_at_ms, "event time")?,
                ],
            )
            .map_err(postgres_error)?;
        transaction.commit().map_err(postgres_error)?;
        let mut stored = event.clone();
        stored.sequence = stored_sequence;
        stored.event_json = event_json;
        Ok((stored, false))
    }

    pub fn get_session_domain_event_by_id(
        &self,
        session_id: &str,
        event_id: &str,
    ) -> session::SessionResult<Option<SessionEvent>> {
        if event_id.trim().is_empty() {
            return Ok(None);
        }
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT session_id, event_type, event_json, sequence, created_at_ms
                   FROM session_events
                  WHERE session_id=$1
                    AND event_type=$2
                    AND event_json::jsonb ->> 'event_id'=$3
                  LIMIT 1",
                &[&session_id, &session::SESSION_DOMAIN_EVENT_TYPE, &event_id],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_event(&row))
            .transpose()
    }

    pub fn append_events_allocating_sequence_if_checkpoint_absent(
        &self,
        events: &[SessionEvent],
        checkpoint_id: &str,
    ) -> session::SessionResult<Option<Vec<SessionEvent>>> {
        if events.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let session_id = events[0].session_id.as_str();
        if session_id.trim().is_empty() || events.iter().any(|event| event.session_id != session_id)
        {
            return Err(session::SessionError::Store(
                "atomic session event batch must contain one non-empty session_id".to_string(),
            ));
        }
        if checkpoint_id.trim().is_empty() {
            return Err(session::SessionError::Store(
                "checkpoint-aware event batch requires a non-empty checkpoint_id".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
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
                    session::SessionError::Store("event batch offset overflow".to_string())
                })?)
                .ok_or_else(|| {
                    session::SessionError::Store("event sequence overflow".to_string())
                })?;
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
    ) -> session::SessionResult<Option<SessionEvent>> {
        if event.event_type != "ContextEnvelope" {
            return self.append_event_allocating_sequence(event).map(Some);
        }
        let envelope_id = context_envelope_id(&event.event_json)?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
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
    ) -> session::SessionResult<bool> {
        self.append_context_envelope_event_if_absent_allocating_sequence(event)
            .map(|stored| stored.is_some())
    }

    pub fn get_events(
        &self,
        session_id: &str,
        from_seq: usize,
    ) -> session::SessionResult<Vec<SessionEvent>> {
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
    ) -> session::SessionResult<Vec<SessionEvent>> {
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
    ) -> session::SessionResult<Vec<SessionEvent>> {
        self.get_events_by_type_limited(
            session_id,
            session::SESSION_DOMAIN_EVENT_TYPE,
            from_seq,
            limit,
        )
    }

    pub fn count_session_domain_timeline_from(
        &self,
        session_id: &str,
        from_seq: usize,
    ) -> session::SessionResult<usize> {
        self.count_events_by_type_from(session_id, session::SESSION_DOMAIN_EVENT_TYPE, from_seq)
    }

    pub fn get_session_domain_events_by_kind_limited(
        &self,
        session_id: &str,
        kind: &str,
        from_seq: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionEvent>> {
        self.query_events(
            "SELECT session_id, event_type, event_json, sequence, created_at_ms
               FROM session_events
              WHERE session_id=$1
                AND event_type=$2
                AND event_json::jsonb ->> 'kind'=$3
                AND sequence >= $4
              ORDER BY sequence ASC
              LIMIT $5",
            &[
                &session_id,
                &session::SESSION_DOMAIN_EVENT_TYPE,
                &kind,
                &to_i64(from_seq, "event sequence")?,
                &to_i64(limit, "event limit")?,
            ],
        )
    }

    pub fn get_latest_session_domain_event_by_kind(
        &self,
        session_id: &str,
        kind: &str,
    ) -> session::SessionResult<Option<SessionEvent>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT session_id, event_type, event_json, sequence, created_at_ms
                   FROM session_events
                  WHERE session_id=$1
                    AND event_type=$2
                    AND event_json::jsonb ->> 'kind'=$3
                  ORDER BY sequence DESC
                  LIMIT 1",
                &[&session_id, &session::SESSION_DOMAIN_EVENT_TYPE, &kind],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_event(&row))
            .transpose()
    }

    pub fn count_session_domain_events_by_kind_from(
        &self,
        session_id: &str,
        kind: &str,
        from_seq: usize,
    ) -> session::SessionResult<usize> {
        self.count_events_sql(
            "SELECT COUNT(*) FROM session_events
              WHERE session_id=$1
                AND event_type=$2
                AND event_json::jsonb ->> 'kind'=$3
                AND sequence >= $4",
            &[
                &session_id,
                &session::SESSION_DOMAIN_EVENT_TYPE,
                &kind,
                &to_i64(from_seq, "event sequence")?,
            ],
        )
    }

    pub fn has_session_domain_event_kind(&self, kind: &str) -> session::SessionResult<bool> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_one(
                "SELECT EXISTS(
                    SELECT 1 FROM session_events
                     WHERE event_type=$1
                       AND event_json::jsonb ->> 'kind'=$2
                     LIMIT 1
                )",
                &[&session::SESSION_DOMAIN_EVENT_TYPE, &kind],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)
    }

    pub fn has_session_with_domain_event_kinds(
        &self,
        kinds: &[String],
    ) -> session::SessionResult<bool> {
        if kinds.is_empty() {
            return Ok(false);
        }
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let required = to_i64(kinds.len(), "event kind count")?;
        connection
            .query_one(
                "SELECT EXISTS(
                    SELECT session_id
                      FROM session_events
                     WHERE event_type=$1
                       AND event_json::jsonb ->> 'kind'=ANY($2::text[])
                     GROUP BY session_id
                    HAVING COUNT(DISTINCT event_json::jsonb ->> 'kind') >= $3
                     LIMIT 1
                )",
                &[&session::SESSION_DOMAIN_EVENT_TYPE, &kinds, &required],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)
    }

    pub fn get_events_by_type_limited(
        &self,
        session_id: &str,
        event_type: &str,
        from_seq: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionEvent>> {
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
    ) -> session::SessionResult<usize> {
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
    ) -> session::SessionResult<usize> {
        self.count_events_sql("SELECT COUNT(*) FROM session_events WHERE session_id=$1 AND event_type=$2 AND sequence >= $3", &[&session_id, &event_type, &to_i64(from_seq, "event sequence")?])
    }

    pub fn get_context_event_by_envelope_id(
        &self,
        envelope_id: &str,
    ) -> session::SessionResult<Option<SessionEvent>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection.query_opt(
            "SELECT session_id, event_type, event_json, sequence, created_at_ms FROM session_events
             WHERE event_type='ContextEnvelope' AND COALESCE(event_json::jsonb #>> '{envelope,id}', event_json::jsonb ->> 'envelope_id')=$1
             ORDER BY created_at_ms DESC LIMIT 1",
            &[&envelope_id],
        ).map_err(postgres_error)?.map(|row| row_to_event(&row)).transpose()
    }

    pub fn next_event_sequence(&self, session_id: &str) -> session::SessionResult<usize> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
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
    ) -> session::SessionResult<usize> {
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
    ) -> session::SessionResult<usize> {
        self.delete_events_sql(
            "DELETE FROM session_events WHERE session_id=$1 AND event_type=$2 AND sequence >= $3",
            &[
                &session_id,
                &event_type,
                &to_i64(from_sequence, "event sequence")?,
            ],
        )
    }

    pub fn save_snapshot(&self, snapshot: &SessionSnapshot) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
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
    ) -> session::SessionResult<Option<SessionSnapshot>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection.query_opt(
            "SELECT session_id,event_idx,messages_json,created_at_ms FROM session_snapshots WHERE session_id=$1 ORDER BY event_idx DESC LIMIT 1",
            &[&session_id],
        ).map_err(postgres_error)?.map(|row| row_to_snapshot(&row)).transpose()
    }

    pub fn prune_before(&self, cutoff_iso8601: &str) -> session::SessionResult<usize> {
        let mut connection = self.executor.checkout_background().map_err(storage_error)?;
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
    ) -> session::SessionResult<SessionMissionOutboxRecord> {
        validate_mission_request(request)?;
        if request.session_id != session.session_id {
            return Err(session::SessionError::Store(
                "session/mission outbox session identity does not match record".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        upsert_session_tx(&mut transaction, session)?;
        let record = insert_mission_outbox_tx(&mut transaction, request)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    pub fn plan_session_lifecycle(
        &self,
        plan: &SessionLifecyclePlan,
    ) -> session::SessionResult<SessionLifecycleIntent> {
        if plan.operation_id.trim().is_empty()
            || plan.session_id.trim().is_empty()
            || plan.expected_generation == 0
        {
            return Err(session::SessionError::Store(
                "Session lifecycle plan requires non-empty identities and a positive generation"
                    .to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        // Session is the aggregate lock root for both lifecycle and input rows.
        let admission = query_input_admission_tx(&mut transaction, &plan.session_id, true)?
            .ok_or_else(|| {
                session::SessionError::Store(format!("session `{}` not found", plan.session_id))
            })?;
        if let Some(existing) =
            query_lifecycle_intent_tx(&mut transaction, &plan.operation_id, true)?
        {
            if existing.session_id == plan.session_id
                && existing.disposition == plan.disposition
                && existing.expected_generation == plan.expected_generation
            {
                transaction.commit().map_err(postgres_error)?;
                return Ok(existing);
            }
            return Err(session::SessionError::Store(format!(
                "Session lifecycle operation `{}` is bound to another identity",
                plan.operation_id
            )));
        }
        if admission.generation != plan.expected_generation || !admission.open {
            return Err(session::SessionError::Store(format!(
                "Session lifecycle plan `{}` expected open generation {}, found generation {} open={}",
                plan.operation_id,
                plan.expected_generation,
                admission.generation,
                admission.open
            )));
        }
        let created_at_ms = to_u64_i64(plan.created_at_ms, "lifecycle plan time")?;
        transaction
            .execute(
                "INSERT INTO session_lifecycle_intents(
                     operation_id,session_id,disposition,phase,last_stable_phase,
                     expected_generation,created_at_ms,updated_at_ms,last_error,revision
                 ) VALUES($1,$2,$3,'planned','planned',$4,$5,$5,NULL,0)",
                &[
                    &plan.operation_id,
                    &plan.session_id,
                    &plan.disposition.as_str(),
                    &to_u64_i64(plan.expected_generation, "lifecycle expected generation")?,
                    &created_at_ms,
                ],
            )
            .map_err(postgres_error)?;
        let intent = query_lifecycle_intent_tx(&mut transaction, &plan.operation_id, false)?
            .ok_or_else(|| {
                session::SessionError::Store(
                    "Session lifecycle plan produced no readable row".to_string(),
                )
            })?;
        transaction.commit().map_err(postgres_error)?;
        Ok(intent)
    }

    pub fn get_session_lifecycle_intent(
        &self,
        operation_id: &str,
    ) -> session::SessionResult<Option<SessionLifecycleIntent>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT operation_id,session_id,disposition,phase,last_stable_phase,
                        expected_generation,created_at_ms,updated_at_ms,last_error,revision
                   FROM session_lifecycle_intents WHERE operation_id=$1",
                &[&operation_id],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_lifecycle_intent(&row))
            .transpose()
    }

    pub fn list_recoverable_session_lifecycle_intents(
        &self,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionLifecycleIntent>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT operation_id,session_id,disposition,phase,last_stable_phase,
                        expected_generation,created_at_ms,updated_at_ms,last_error,revision
                   FROM session_lifecycle_intents
                  WHERE phase != 'unloaded'
                  ORDER BY updated_at_ms ASC,operation_id ASC LIMIT $1",
                &[&to_i64(limit.max(1), "lifecycle recovery limit")?],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_lifecycle_intent)
            .collect()
    }

    pub fn fence_session_lifecycle(
        &self,
        request: &SessionLifecycleFenceRequest,
    ) -> session::SessionResult<SessionLifecycleIntent> {
        if request.actor.trim().is_empty()
            || request.reason.trim().is_empty()
            || request.transitional_status.trim().is_empty()
        {
            return Err(session::SessionError::Store(
                "Session lifecycle fence requires actor, reason, and transitional status"
                    .to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current =
            query_lifecycle_intent_tx(&mut transaction, &request.transition.operation_id, true)?
                .ok_or_else(|| {
                    session::SessionError::Store(format!(
                        "Session lifecycle intent `{}` does not exist",
                        request.transition.operation_id
                    ))
                })?;
        request.transition.validate(&current)?;
        if request.transition.next_phase != SessionLifecyclePhase::AdmissionFenced
            || request.event.session_id != current.session_id
        {
            return Err(session::SessionError::Store(
                "Session lifecycle fence identity or phase is invalid".to_string(),
            ));
        }
        let admission = query_input_admission_tx(&mut transaction, &current.session_id, true)?
            .ok_or_else(|| {
                session::SessionError::Store(format!("session `{}` not found", current.session_id))
            })?;
        if admission.generation != current.expected_generation || !admission.open {
            return Err(session::SessionError::Store(format!(
                "Session lifecycle fence `{}` lost generation authority",
                current.operation_id
            )));
        }
        let active = transaction
            .query(
                "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,
                        session_generation,decision,target_turn_id,classification_json,status,
                        runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                        claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                        updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch
                   FROM session_runtime_outbox
                  WHERE session_id=$1 AND session_generation=$2
                    AND status IN (
                        'accepted','classified','queued','claimed',
                        'running','reclassified','blocked'
                    )
                  ORDER BY sequence ASC,request_id ASC FOR UPDATE",
                &[
                    &current.session_id,
                    &to_u64_i64(current.expected_generation, "lifecycle generation")?,
                ],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_runtime_outbox)
            .collect::<session::SessionResult<Vec<_>>>()?;
        let next_generation = current.expected_generation.checked_add(1).ok_or_else(|| {
            session::SessionError::Store("Session generation overflow".to_string())
        })?;
        let updated_at_ms = to_u64_i64(request.transition.updated_at_ms, "lifecycle fence time")?;
        let updated_at = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(updated_at_ms)
            .unwrap_or_else(chrono::Utc::now)
            .to_rfc3339();
        let changed = transaction
            .execute(
                "UPDATE session_records
                    SET input_generation=$1,input_admission_open=FALSE,status=$2,
                        last_activity=$3,updated_at_ms=GREATEST(updated_at_ms,$4)
                  WHERE session_id=$5 AND input_generation=$6
                    AND input_admission_open=TRUE",
                &[
                    &to_u64_i64(next_generation, "next Session generation")?,
                    &request.transitional_status,
                    &updated_at,
                    &updated_at_ms,
                    &current.session_id,
                    &to_u64_i64(current.expected_generation, "lifecycle generation")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "Session lifecycle fence `{}` changed during admission close",
                current.operation_id
            )));
        }
        for before in active {
            let changed = transaction
                .execute(
                    "UPDATE session_runtime_outbox
                        SET status='expired',claim_owner=NULL,claim_token=NULL,
                            claim_fence_epoch=NULL,
                            claim_expires_at_ms=NULL,last_error=$1,terminal_at_ms=$2,
                            updated_at_ms=$2,revision=revision+1
                      WHERE request_id=$3 AND session_generation=$4 AND revision=$5",
                    &[
                        &request.reason,
                        &updated_at_ms,
                        &before.request_id,
                        &to_u64_i64(current.expected_generation, "lifecycle input generation")?,
                        &to_u64_i64(before.revision, "lifecycle input revision")?,
                    ],
                )
                .map_err(postgres_error)?;
            if changed != 1 {
                return Err(session::SessionError::Store(format!(
                    "Session lifecycle fence lost input `{}`",
                    before.request_id
                )));
            }
            let mut expired = before.clone();
            expired.status = SessionRuntimeInputStatus::Expired;
            expired.claim_owner = None;
            expired.claim_token = None;
            expired.claim_expires_at_ms = None;
            expired.last_error = Some(request.reason.clone());
            expired.terminal_at_ms = Some(request.transition.updated_at_ms);
            expired.updated_at_ms = request.transition.updated_at_ms;
            expired.revision = before.revision.saturating_add(1);
            append_runtime_history_tx(
                &mut transaction,
                &expired,
                "lifecycle_fence",
                Some(&request.actor),
                Some(before.revision),
                before.status,
                SessionRuntimeInputStatus::Expired,
                Some(&request.reason),
                request.transition.updated_at_ms,
            )?;
        }
        let closed = SessionInputAdmission {
            session_id: current.session_id.clone(),
            generation: next_generation,
            open: false,
        };
        append_admission_timeline_event_tx(
            &mut transaction,
            &current.session_id,
            current.expected_generation,
            &closed,
            &request.actor,
            &request.reason,
            request.transition.updated_at_ms,
        )?;
        append_allocated_event_tx(&mut transaction, &request.event)?;
        let intent = transition_lifecycle_intent_tx(&mut transaction, &request.transition)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(intent)
    }

    pub fn transition_session_lifecycle(
        &self,
        transition: &SessionLifecycleTransition,
    ) -> session::SessionResult<SessionLifecycleIntent> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let intent = transition_lifecycle_intent_tx(&mut transaction, transition)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(intent)
    }

    pub fn commit_session_lifecycle_tombstone(
        &self,
        request: &SessionLifecycleTombstoneRequest,
    ) -> session::SessionResult<SessionLifecycleIntent> {
        validate_mission_request(&request.mission_outbox)?;
        if request.mission_outbox.operation != SessionMissionOutboxOperation::Close {
            return Err(session::SessionError::Store(
                "Session tombstone requires a close Mission outbox intent".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current =
            query_lifecycle_intent_tx(&mut transaction, &request.transition.operation_id, true)?
                .ok_or_else(|| {
                    session::SessionError::Store(format!(
                        "Session lifecycle intent `{}` does not exist",
                        request.transition.operation_id
                    ))
                })?;
        request.transition.validate(&current)?;
        if request.transition.next_phase != SessionLifecyclePhase::TombstoneCommitted
            || request.record.session_id != current.session_id
            || request.mission_outbox.session_id != current.session_id
            || request.event.session_id != current.session_id
        {
            return Err(session::SessionError::Store(
                "Session lifecycle tombstone identity or phase is invalid".to_string(),
            ));
        }
        query_input_admission_tx(&mut transaction, &current.session_id, true)?.ok_or_else(
            || session::SessionError::Store(format!("session `{}` not found", current.session_id)),
        )?;
        let changed = transaction
            .execute(
                "UPDATE session_records SET
                     platform=$2,chat_id=$3,user_id=$4,model=$5,last_activity=$6,
                     message_count=$7,reset_policy=$8,metadata_json=$9,input_tokens=$10,
                     output_tokens=$11,estimated_cost_usd=$12,status=$13,updated_at_ms=$14
                   WHERE session_id=$1 AND input_generation=$15
                     AND input_admission_open=FALSE",
                &[
                    &request.record.session_id,
                    &request.record.platform,
                    &request.record.chat_id,
                    &request.record.user_id,
                    &request.record.model,
                    &request.record.last_activity,
                    &request.record.message_count,
                    &request.record.reset_policy,
                    &request.record.metadata_json,
                    &request.record.input_tokens,
                    &request.record.output_tokens,
                    &request.record.estimated_cost_usd,
                    &request.record.status,
                    &to_u64_i64(request.transition.updated_at_ms, "lifecycle tombstone time")?,
                    &to_u64_i64(
                        current.expected_generation.saturating_add(1),
                        "fenced Session generation",
                    )?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "Session lifecycle tombstone `{}` lost fenced Session authority",
                current.operation_id
            )));
        }
        insert_mission_outbox_tx(&mut transaction, &request.mission_outbox)?;
        append_allocated_event_tx(&mut transaction, &request.event)?;
        let intent = transition_lifecycle_intent_tx(&mut transaction, &request.transition)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(intent)
    }

    pub fn delete_session_with_mission_outbox(
        &self,
        request: &SessionMissionOutboxRequest,
    ) -> session::SessionResult<bool> {
        validate_mission_request(request)?;
        if request.operation != SessionMissionOutboxOperation::Close {
            return Err(session::SessionError::Store(
                "session deletion requires a close mission outbox operation".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
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
    ) -> session::SessionResult<Option<SessionMissionOutboxRecord>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
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
    ) -> session::SessionResult<Vec<SessionMissionOutboxRecord>> {
        if workspace_key.trim().is_empty() || worker_id.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let now = to_u64_i64(now_ms, "mission outbox clock")?;
        let lease_expires = now
            .checked_add(to_u64_i64(lease_ms, "mission outbox lease")?)
            .ok_or_else(|| {
                session::SessionError::Store("mission outbox lease overflow".to_string())
            })?;
        let limit = to_i64(limit.min(500), "mission outbox limit")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
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
    ) -> session::SessionResult<SessionMissionOutboxRecord> {
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
    ) -> session::SessionResult<SessionMissionOutboxRecord> {
        let now = to_u64_i64(now_ms, "mission outbox clock")?;
        let retry_at = to_u64_i64(retry_at_ms, "mission outbox retry")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
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

    #[allow(clippy::too_many_arguments)]
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
    ) -> session::SessionResult<SessionMissionOutboxRecord> {
        let now = to_u64_i64(now_ms, "mission outbox clock")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let existing = mission_outbox_for_update(&mut transaction, request_id)?;
        assert_mission_lease(&existing, worker_id, expected_revision, now_ms)?;
        let status = OutboxStatus::parse(next_status)
            .map_err(|error| session::SessionError::Store(error.to_string()))?;
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
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        validate_runtime_request(message, request)?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&message.session_id],
            )
            .map_err(postgres_error)?;
        if let Some(existing) = runtime_outbox_tx(&mut transaction, &request.request_id)? {
            if existing.input_id == request.input_id
                && existing.turn_id == request.turn_id
                && existing.message_id == request.message_id
                && existing.session_id == message.session_id
                && existing.sequence == message.sequence
                && existing.session_generation == request.session_generation
                && existing.decision == request.decision
                && existing.target_turn_id == request.target_turn_id
            {
                transaction.commit().map_err(postgres_error)?;
                return Ok(existing);
            }
            return Err(session::SessionError::Store(format!(
                "outbox request_id `{}` is already bound to another message",
                request.request_id
            )));
        }
        require_input_admission_tx(
            &mut transaction,
            &message.session_id,
            request.session_generation,
        )?;
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
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        if session_id.trim().is_empty()
            || role.trim().is_empty()
            || request.input_id.trim().is_empty()
        {
            return Err(session::SessionError::Store(
                "ingress outbox requires non-empty session, role and request identities"
                    .to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        if let Some(existing) = runtime_outbox_tx(&mut transaction, &request.request_id)? {
            if existing.input_id == request.input_id
                && existing.session_id == session_id
                && existing.message_id == request.message_id
                && existing.turn_id == request.turn_id
                && existing.session_generation == request.session_generation
                && existing.decision == request.decision
                && existing.target_turn_id == request.target_turn_id
            {
                transaction.commit().map_err(postgres_error)?;
                return Ok(existing);
            }
            return Err(session::SessionError::Store(format!(
                "outbox request `{}` conflicts with its committed ingress",
                request.request_id
            )));
        }
        validate_runtime_input_request(request)?;
        require_input_admission_tx(&mut transaction, session_id, request.session_generation)?;
        let sequence: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(sequence), -1) + 1 FROM session_messages WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let content_json = content_json.unwrap_or("[]");
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
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        if worker_id.trim().is_empty() || lease_ms == 0 || limit == 0 {
            return Err(session::SessionError::Store(
                "outbox claim requires worker_id, positive lease and positive limit".to_string(),
            ));
        }
        let now = to_u64_i64(now_ms, "runtime outbox clock")?;
        let expires = now
            .checked_add(to_u64_i64(lease_ms, "runtime outbox lease")?)
            .ok_or_else(|| {
                session::SessionError::Store("runtime outbox lease overflow".to_string())
            })?;
        let limit = to_i64(limit.min(500), "runtime outbox limit")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let rows = transaction.query(
            "WITH ranked AS (
                 SELECT o.request_id,o.status,o.session_id,o.session_generation,o.sequence,
                        o.next_attempt_at_ms,o.claim_expires_at_ms,
                        ROW_NUMBER() OVER (
                            PARTITION BY o.session_id,o.session_generation
                            ORDER BY o.sequence ASC,o.request_id ASC
                        ) AS session_rank
                   FROM session_runtime_outbox o
                   JOIN session_records s ON s.session_id=o.session_id
                  WHERE o.status IN (
                            'accepted','classified','queued','claimed','running','reclassified'
                        )
                    AND o.session_generation=s.input_generation
                    AND s.input_admission_open=TRUE
             ), candidates AS (
                 SELECT o.request_id,o.status AS previous_status
                   FROM session_runtime_outbox o
                   JOIN ranked r ON r.request_id=o.request_id
                  WHERE r.session_rank=1
                    AND (
                        (r.status IN ('queued','reclassified') AND r.next_attempt_at_ms <= $1)
                        OR (r.status IN ('claimed','running') AND r.claim_expires_at_ms <= $1)
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM session_runtime_outbox held
                         WHERE held.session_id=r.session_id
                           AND held.session_generation=r.session_generation
                           AND held.request_id<>r.request_id
                           AND held.status IN ('claimed','running')
                           AND held.claim_expires_at_ms > $1
                    )
                  ORDER BY r.next_attempt_at_ms ASC,r.sequence ASC,r.request_id ASC
                  FOR UPDATE OF o SKIP LOCKED LIMIT $2
             ), updated AS (
                 UPDATE session_runtime_outbox o
                    SET status='claimed',attempts=o.attempts+1,claim_owner=$3,
                        claim_token=o.request_id || ':' || (o.revision+1)::text
                            || ':' || $1::text || ':' || $3,
                        claim_fence_epoch=o.revision+1,
                        claim_expires_at_ms=$4,updated_at_ms=$1,revision=o.revision+1
                   FROM candidates c WHERE o.request_id=c.request_id
                 RETURNING o.*,c.previous_status
             )
             SELECT input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,
                    decision,target_turn_id,classification_json,status,runtime_commit_cursor,attempts,
                    next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,
                    last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,
                    runtime_options_json,claim_fence_epoch,previous_status
               FROM updated
              ORDER BY next_attempt_at_ms ASC,sequence ASC,request_id ASC",
            &[&now,&limit,&worker_id,&expires],
        ).map_err(postgres_error)?;
        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let record = row_to_runtime_outbox(&row)?;
            let previous: String = row.try_get(25).map_err(postgres_error)?;
            let previous = parse_runtime_status(&previous)?;
            append_runtime_history_tx(
                &mut transaction,
                &record,
                if previous.holds_claim() {
                    "reclaim"
                } else {
                    "claim"
                },
                Some(worker_id),
                None,
                previous,
                SessionRuntimeInputStatus::Claimed,
                record.claim_token.as_deref(),
                now_ms,
            )?;
            claimed.push(record);
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(claimed)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn mark_session_runtime_outbox_running(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current = runtime_outbox_for_update(&mut transaction, request_id)?;
        assert_runtime_lease(
            &mut transaction,
            &current,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[SessionRuntimeInputStatus::Claimed],
        )?;
        let changed = transaction
            .execute(
                "UPDATE session_runtime_outbox
                SET status='running',updated_at_ms=$1,revision=revision+1
              WHERE request_id=$2 AND status='claimed' AND session_generation=$3
                AND claim_owner=$4 AND claim_token=$5 AND revision=$6",
                &[
                    &to_u64_i64(now_ms, "runtime clock")?,
                    &request_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &worker_id,
                    &claim_token,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "outbox `{request_id}` changed during running transition"
            )));
        }
        let record = runtime_outbox_for_update(&mut transaction, request_id)?;
        append_runtime_history_tx(
            &mut transaction,
            &record,
            "start",
            Some(worker_id),
            Some(expected_revision),
            current.status,
            SessionRuntimeInputStatus::Running,
            None,
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn ack_session_runtime_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        terminal_status: SessionRuntimeInputStatus,
        runtime_commit_cursor: u64,
        now_ms: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        if !matches!(
            terminal_status,
            SessionRuntimeInputStatus::Completed
                | SessionRuntimeInputStatus::Supplemented
                | SessionRuntimeInputStatus::Cancelled
        ) {
            return Err(session::SessionError::Store(
                "ack terminal status must be completed, supplemented, or cancelled".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current = runtime_outbox_for_update(&mut transaction, request_id)?;
        assert_runtime_lease(
            &mut transaction,
            &current,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[SessionRuntimeInputStatus::Running],
        )?;
        let changed = transaction
            .execute(
                "UPDATE session_runtime_outbox
                SET status=$1,runtime_commit_cursor=$2,claim_owner=NULL,claim_token=NULL,
                    claim_fence_epoch=NULL,
                    claim_expires_at_ms=NULL,terminal_at_ms=$3,failure_class=NULL,last_error=NULL,
                    updated_at_ms=$3,revision=revision+1
              WHERE request_id=$4 AND status='running' AND session_generation=$5
                AND claim_owner=$6 AND claim_token=$7 AND revision=$8",
                &[
                    &terminal_status.as_str(),
                    &to_u64_i64(runtime_commit_cursor, "runtime cursor")?,
                    &to_u64_i64(now_ms, "runtime clock")?,
                    &request_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &worker_id,
                    &claim_token,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "outbox `{request_id}` changed during terminal transition"
            )));
        }
        let record = runtime_outbox_for_update(&mut transaction, request_id)?;
        append_runtime_history_tx(
            &mut transaction,
            &record,
            "ack",
            Some(worker_id),
            Some(expected_revision),
            current.status,
            terminal_status,
            None,
            now_ms,
        )?;
        append_input_timeline_event_tx(
            &mut transaction,
            &request_from_outbox(&record),
            &record.session_id,
            record.sequence,
            "session.input.terminal.v1",
            terminal_status,
            Some(worker_id),
            None,
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn renew_session_runtime_outbox_lease(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        now_ms: u64,
        lease_ms: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        if lease_ms == 0 {
            return Err(session::SessionError::Store(
                "outbox lease renewal requires a positive lease".to_string(),
            ));
        }
        let now = to_u64_i64(now_ms, "runtime outbox clock")?;
        let expires = now
            .checked_add(to_u64_i64(lease_ms, "runtime outbox lease")?)
            .ok_or_else(|| {
                session::SessionError::Store("runtime outbox lease overflow".to_string())
            })?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let existing = runtime_outbox_for_update(&mut transaction, request_id)?;
        assert_runtime_lease(
            &mut transaction,
            &existing,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[
                SessionRuntimeInputStatus::Claimed,
                SessionRuntimeInputStatus::Running,
            ],
        )?;
        let changed = transaction
            .execute(
                "UPDATE session_runtime_outbox
                SET claim_expires_at_ms=$1,updated_at_ms=$2,revision=revision+1
              WHERE request_id=$3 AND status IN ('claimed','running')
                AND session_generation=$4 AND claim_owner=$5
                AND claim_token=$6 AND revision=$7",
                &[
                    &expires,
                    &now,
                    &request_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &worker_id,
                    &claim_token,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "outbox lease for `{request_id}` changed during renewal"
            )));
        }
        let record = runtime_outbox_for_update(&mut transaction, request_id)?;
        append_runtime_history_tx(
            &mut transaction,
            &record,
            "renew_lease",
            Some(worker_id),
            Some(expected_revision),
            existing.status,
            existing.status,
            None,
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn fail_session_runtime_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        failure_class: OutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let existing = runtime_outbox_for_update(&mut transaction, request_id)?;
        assert_runtime_lease(
            &mut transaction,
            &existing,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[
                SessionRuntimeInputStatus::Claimed,
                SessionRuntimeInputStatus::Running,
            ],
        )?;
        let retry = failure_class == OutboxFailureClass::Retryable
            && existing.attempts < max_attempts.max(1);
        let next = if retry {
            SessionRuntimeInputStatus::Queued
        } else if matches!(
            failure_class,
            OutboxFailureClass::AuthorizationBlocked | OutboxFailureClass::CorruptPayload
        ) {
            SessionRuntimeInputStatus::Blocked
        } else {
            SessionRuntimeInputStatus::Failed
        };
        let now = to_u64_i64(now_ms, "runtime outbox clock")?;
        let retry_at = if retry {
            to_u64_i64(retry_at_ms, "runtime outbox retry")?
        } else {
            now
        };
        let row = transaction
            .query_one(
                "UPDATE session_runtime_outbox
                SET status=$1,next_attempt_at_ms=$2,claim_owner=NULL,claim_token=NULL,
                    claim_fence_epoch=NULL,
                    claim_expires_at_ms=NULL,terminal_at_ms=$3,failure_class=$4,last_error=$5,
                    updated_at_ms=$6,revision=revision+1
              WHERE request_id=$7 AND status IN ('claimed','running')
                AND session_generation=$8 AND claim_owner=$9
                AND claim_token=$10 AND revision=$11
            RETURNING input_id,request_id,turn_id,message_id,session_id,sequence,
                      session_generation,decision,target_turn_id,classification_json,status,
                      runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                      claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                      updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch",
                &[
                    &next.as_str(),
                    &retry_at,
                    &if next == SessionRuntimeInputStatus::Failed {
                        Some(now)
                    } else {
                        None
                    },
                    &failure_class.as_str(),
                    &error,
                    &now,
                    &request_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &worker_id,
                    &claim_token,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        let record = row_to_runtime_outbox(&row)?;
        append_runtime_history_tx(
            &mut transaction,
            &record,
            if retry {
                "retry"
            } else if next == SessionRuntimeInputStatus::Blocked {
                "block"
            } else {
                "fail"
            },
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

    #[allow(clippy::too_many_arguments)]
    pub fn requeue_claimed_session_runtime_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        decision: InputRoutingDecision,
        target_turn_id: Option<&str>,
        classification_json: Option<&str>,
        reason: &str,
        now_ms: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        let validation = SessionRuntimeOutboxRequest {
            input_id: "validation".to_string(),
            request_id: request_id.to_string(),
            turn_id: "validation".to_string(),
            message_id: "validation".to_string(),
            session_generation,
            decision,
            target_turn_id: target_turn_id.map(str::to_string),
            classification_json: classification_json.map(str::to_string),
            created_at_ms: now_ms,
            runtime_options_json: None,
        };
        validate_runtime_input_request(&validation)?;
        if worker_id.trim().is_empty() || claim_token.trim().is_empty() || reason.trim().is_empty()
        {
            return Err(session::SessionError::Store(
                "claimed input requeue requires worker, claim token, and reason".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current = runtime_outbox_for_update(&mut transaction, request_id)?;
        assert_runtime_lease(
            &mut transaction,
            &current,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[
                SessionRuntimeInputStatus::Claimed,
                SessionRuntimeInputStatus::Running,
            ],
        )?;
        let changed = transaction
            .execute(
                "UPDATE session_runtime_outbox
                SET decision=$1,target_turn_id=$2,classification_json=$3,status='reclassified',
                    next_attempt_at_ms=$4,claim_owner=NULL,claim_token=NULL,
                    claim_fence_epoch=NULL,
                    claim_expires_at_ms=NULL,failure_class=NULL,last_error=NULL,terminal_at_ms=NULL,
                    updated_at_ms=$4,revision=revision+1
              WHERE request_id=$5 AND session_generation=$6 AND claim_owner=$7
                AND claim_token=$8 AND revision=$9 AND status IN ('claimed','running')",
                &[
                    &input_decision_as_str(decision),
                    &target_turn_id,
                    &classification_json,
                    &to_u64_i64(now_ms, "runtime clock")?,
                    &request_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &worker_id,
                    &claim_token,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "outbox `{request_id}` changed during claimed requeue"
            )));
        }
        let updated = runtime_outbox_for_update(&mut transaction, request_id)?;
        append_runtime_history_tx(
            &mut transaction,
            &updated,
            "owner_reclassify_requeue",
            Some(worker_id),
            Some(expected_revision),
            current.status,
            SessionRuntimeInputStatus::Reclassified,
            Some(reason),
            now_ms,
        )?;
        append_input_timeline_event_tx(
            &mut transaction,
            &request_from_outbox(&updated),
            &updated.session_id,
            updated.sequence,
            "session.input.reclassified.v1",
            updated.status,
            Some(worker_id),
            Some(reason),
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(updated)
    }

    pub fn retry_blocked_session_runtime_outbox(
        &self,
        request_id: &str,
        session_generation: u64,
        expected_revision: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(session::SessionError::Store(
                "manual outbox retry requires actor and reason".to_string(),
            ));
        }
        let now = to_u64_i64(now_ms, "runtime outbox clock")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let existing = runtime_outbox_for_update(&mut transaction, request_id)?;
        require_input_admission_tx(&mut transaction, &existing.session_id, session_generation)?;
        if existing.status != SessionRuntimeInputStatus::Blocked
            || existing.session_generation != session_generation
            || existing.revision != expected_revision
        {
            return Err(session::SessionError::Store(format!(
                "outbox `{request_id}` is not blocked at revision {expected_revision}"
            )));
        }
        let row = transaction
            .query_one(
                "UPDATE session_runtime_outbox
                SET status='queued',next_attempt_at_ms=$1,claim_owner=NULL,claim_token=NULL,
                    claim_fence_epoch=NULL,
                    claim_expires_at_ms=NULL,failure_class=NULL,last_error=NULL,
                    terminal_at_ms=NULL,updated_at_ms=$1,revision=revision+1
              WHERE request_id=$2 AND session_generation=$3 AND revision=$4 AND status='blocked'
            RETURNING input_id,request_id,turn_id,message_id,session_id,sequence,
                      session_generation,decision,target_turn_id,classification_json,status,
                      runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                      claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                      updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch",
                &[
                    &now,
                    &request_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        let record = row_to_runtime_outbox(&row)?;
        append_runtime_history_tx(
            &mut transaction,
            &record,
            "manual_retry",
            Some(actor),
            Some(expected_revision),
            SessionRuntimeInputStatus::Blocked,
            SessionRuntimeInputStatus::Queued,
            Some(reason),
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    pub fn cancel_session_runtime_outbox(
        &self,
        input_id: &str,
        session_generation: u64,
        expected_revision: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(session::SessionError::Store(
                "session input cancellation requires actor and reason".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current = runtime_outbox_by_input_id_for_update(&mut transaction, input_id)?;
        if current.session_generation != session_generation
            || current.revision != expected_revision
            || current.status.is_terminal()
        {
            return Err(session::SessionError::Store(format!(
                "session input `{input_id}` cannot be cancelled at generation {session_generation} revision {expected_revision}"
            )));
        }
        let changed = transaction
            .execute(
                "UPDATE session_runtime_outbox
                SET status='cancelled',claim_owner=NULL,claim_token=NULL,
                    claim_fence_epoch=NULL,
                    claim_expires_at_ms=NULL,last_error=$1,terminal_at_ms=$2,
                    updated_at_ms=$2,revision=revision+1
              WHERE input_id=$3 AND session_generation=$4 AND revision=$5
                AND status NOT IN ('completed','supplemented','failed','cancelled','expired')",
                &[
                    &reason,
                    &to_u64_i64(now_ms, "runtime clock")?,
                    &input_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "session input `{input_id}` changed during cancellation"
            )));
        }
        let updated = runtime_outbox_by_input_id_for_update(&mut transaction, input_id)?;
        append_runtime_history_tx(
            &mut transaction,
            &updated,
            "cancel",
            Some(actor),
            Some(expected_revision),
            current.status,
            SessionRuntimeInputStatus::Cancelled,
            Some(reason),
            now_ms,
        )?;
        append_input_timeline_event_tx(
            &mut transaction,
            &request_from_outbox(&updated),
            &updated.session_id,
            updated.sequence,
            "session.input.cancelled.v1",
            updated.status,
            Some(actor),
            Some(reason),
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(updated)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reclassify_session_runtime_outbox(
        &self,
        input_id: &str,
        session_generation: u64,
        expected_revision: u64,
        decision: InputRoutingDecision,
        target_turn_id: Option<&str>,
        classification_json: Option<&str>,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        let validation = SessionRuntimeOutboxRequest {
            input_id: input_id.to_string(),
            request_id: "validation".to_string(),
            turn_id: "validation".to_string(),
            message_id: "validation".to_string(),
            session_generation,
            decision,
            target_turn_id: target_turn_id.map(str::to_string),
            classification_json: classification_json.map(str::to_string),
            created_at_ms: now_ms,
            runtime_options_json: None,
        };
        validate_runtime_input_request(&validation)?;
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(session::SessionError::Store(
                "session input reclassification requires actor and reason".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current = runtime_outbox_by_input_id_for_update(&mut transaction, input_id)?;
        require_input_admission_tx(&mut transaction, &current.session_id, session_generation)?;
        if current.session_generation != session_generation
            || current.revision != expected_revision
            || !matches!(
                current.status,
                SessionRuntimeInputStatus::Accepted
                    | SessionRuntimeInputStatus::Classified
                    | SessionRuntimeInputStatus::Queued
                    | SessionRuntimeInputStatus::Reclassified
                    | SessionRuntimeInputStatus::Blocked
            )
        {
            return Err(session::SessionError::Store(format!(
                "session input `{input_id}` is not reclassifiable at generation {session_generation} revision {expected_revision}"
            )));
        }
        let changed = transaction
            .execute(
                "UPDATE session_runtime_outbox
                SET decision=$1,target_turn_id=$2,classification_json=$3,status='reclassified',
                    next_attempt_at_ms=$4,failure_class=NULL,last_error=NULL,terminal_at_ms=NULL,
                    claim_owner=NULL,claim_token=NULL,claim_fence_epoch=NULL,
                    claim_expires_at_ms=NULL,
                    updated_at_ms=$4,revision=revision+1
              WHERE input_id=$5 AND session_generation=$6 AND revision=$7
                AND status IN ('accepted','classified','queued','reclassified','blocked')",
                &[
                    &input_decision_as_str(decision),
                    &target_turn_id,
                    &classification_json,
                    &to_u64_i64(now_ms, "runtime clock")?,
                    &input_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "session input `{input_id}` changed during reclassification"
            )));
        }
        let updated = runtime_outbox_by_input_id_for_update(&mut transaction, input_id)?;
        append_runtime_history_tx(
            &mut transaction,
            &updated,
            "reclassify",
            Some(actor),
            Some(expected_revision),
            current.status,
            SessionRuntimeInputStatus::Reclassified,
            Some(reason),
            now_ms,
        )?;
        append_input_timeline_event_tx(
            &mut transaction,
            &request_from_outbox(&updated),
            &updated.session_id,
            updated.sequence,
            "session.input.reclassified.v1",
            updated.status,
            Some(actor),
            Some(reason),
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(updated)
    }

    pub fn get_session_input_admission(
        &self,
        session_id: &str,
    ) -> session::SessionResult<Option<SessionInputAdmission>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT session_id,input_generation,input_admission_open
                   FROM session_records WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .map(|row| {
                Ok(SessionInputAdmission {
                    session_id: row.try_get(0).map_err(postgres_error)?,
                    generation: i64_to_u64(
                        row.try_get(1).map_err(postgres_error)?,
                        "session input generation",
                    )?,
                    open: row.try_get(2).map_err(postgres_error)?,
                })
            })
            .transpose()
    }

    pub fn close_session_input_admission(
        &self,
        session_id: &str,
        expected_generation: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> session::SessionResult<SessionInputAdmission> {
        self.advance_session_input_generation(
            session_id,
            expected_generation,
            false,
            actor,
            reason,
            now_ms,
        )
    }

    pub fn advance_session_input_generation(
        &self,
        session_id: &str,
        expected_generation: u64,
        open: bool,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> session::SessionResult<SessionInputAdmission> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(session::SessionError::Store(
                "session generation advance requires actor and reason".to_string(),
            ));
        }
        let next_generation = expected_generation.checked_add(1).ok_or_else(|| {
            session::SessionError::Store("session generation overflow".to_string())
        })?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current =
            query_input_admission_tx(&mut transaction, session_id, true)?.ok_or_else(|| {
                session::SessionError::Store(format!("session `{session_id}` not found"))
            })?;
        if current.generation != expected_generation {
            return Err(session::SessionError::Store(format!(
                "session `{session_id}` generation changed from expected {expected_generation}"
            )));
        }
        let rows = transaction
            .query(
                "SELECT request_id,status,revision
               FROM session_runtime_outbox
              WHERE session_id=$1 AND session_generation=$2
                AND status IN (
                    'accepted','classified','queued','claimed','running','reclassified','blocked'
                )
              ORDER BY sequence ASC,request_id ASC FOR UPDATE",
                &[
                    &session_id,
                    &to_u64_i64(expected_generation, "session generation")?,
                ],
            )
            .map_err(postgres_error)?;
        let changed = transaction
            .execute(
                "UPDATE session_records
                SET input_generation=$1,input_admission_open=$2,
                    updated_at_ms=GREATEST(updated_at_ms,$3)
              WHERE session_id=$4 AND input_generation=$5",
                &[
                    &to_u64_i64(next_generation, "session generation")?,
                    &open,
                    &to_u64_i64(now_ms, "runtime clock")?,
                    &session_id,
                    &to_u64_i64(expected_generation, "session generation")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "session `{session_id}` generation changed during advance"
            )));
        }
        for row in rows {
            let request_id: String = row.try_get(0).map_err(postgres_error)?;
            let previous =
                parse_runtime_status(&row.try_get::<_, String>(1).map_err(postgres_error)?)?;
            let revision = i64_to_u64(row.try_get(2).map_err(postgres_error)?, "runtime revision")?;
            let expired = transaction
                .query_one(
                    "UPDATE session_runtime_outbox
                    SET status='expired',claim_owner=NULL,claim_token=NULL,
                        claim_fence_epoch=NULL,
                        claim_expires_at_ms=NULL,last_error=$1,terminal_at_ms=$2,
                        updated_at_ms=$2,revision=revision+1
                  WHERE request_id=$3 AND session_generation=$4 AND revision=$5
                RETURNING input_id,request_id,turn_id,message_id,session_id,sequence,
                          session_generation,decision,target_turn_id,classification_json,status,
                          runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                          claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                          updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch",
                    &[
                        &reason,
                        &to_u64_i64(now_ms, "runtime clock")?,
                        &request_id,
                        &to_u64_i64(expected_generation, "session generation")?,
                        &to_u64_i64(revision, "runtime revision")?,
                    ],
                )
                .map_err(postgres_error)?;
            let expired = row_to_runtime_outbox(&expired)?;
            append_runtime_history_tx(
                &mut transaction,
                &expired,
                "generation_expire",
                Some(actor),
                Some(revision),
                previous,
                SessionRuntimeInputStatus::Expired,
                Some(reason),
                now_ms,
            )?;
        }
        let admission =
            query_input_admission_tx(&mut transaction, session_id, false)?.ok_or_else(|| {
                session::SessionError::Store(format!(
                    "session `{session_id}` disappeared after generation advance"
                ))
            })?;
        append_admission_timeline_event_tx(
            &mut transaction,
            session_id,
            expected_generation,
            &admission,
            actor,
            reason,
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(admission)
    }

    pub fn get_session_runtime_outbox(
        &self,
        request_id: &str,
    ) -> session::SessionResult<Option<SessionRuntimeOutboxRecord>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(RUNTIME_OUTBOX_SELECT, &[&request_id])
            .map_err(postgres_error)?
            .map(|row| row_to_runtime_outbox(&row))
            .transpose()
    }

    pub fn get_session_runtime_outbox_by_input_id(
        &self,
        input_id: &str,
    ) -> session::SessionResult<Option<SessionRuntimeOutboxRecord>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,
                        session_generation,decision,target_turn_id,classification_json,status,
                        runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                        claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                        updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch
                   FROM session_runtime_outbox WHERE input_id=$1",
                &[&input_id],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_runtime_outbox(&row))
            .transpose()
    }

    pub fn session_runtime_outbox_for_session(
        &self,
        session_id: &str,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        self.query_runtime_outbox(
            "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,
                    session_generation,decision,target_turn_id,classification_json,status,
                    runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                    claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                    updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch FROM session_runtime_outbox
              WHERE session_id=$1 ORDER BY updated_at_ms DESC,sequence DESC,request_id DESC LIMIT $2",
            &[&session_id,&to_i64(limit.clamp(1,500), "runtime outbox limit")?],
        )
    }

    pub fn session_runtime_outbox_for_sessions(
        &self,
        session_ids: &[String],
        per_session_limit: usize,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        if session_ids.is_empty() {
            return Ok(Vec::new());
        }
        self.query_runtime_outbox(
            "WITH ranked AS (
                 SELECT input_id,request_id,turn_id,message_id,session_id,sequence,
                        session_generation,decision,target_turn_id,classification_json,status,
                        runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                        claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                        updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,
                        ROW_NUMBER() OVER (
                            PARTITION BY session_id
                            ORDER BY updated_at_ms DESC,sequence DESC,request_id DESC
                        ) AS row_number
                   FROM session_runtime_outbox
                  WHERE session_id = ANY($1::text[])
             )
             SELECT input_id,request_id,turn_id,message_id,session_id,sequence,
                    session_generation,decision,target_turn_id,classification_json,status,
                    runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                    claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                    updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch
               FROM ranked
              WHERE row_number <= $2
              ORDER BY session_id ASC,updated_at_ms DESC,sequence DESC,request_id DESC",
            &[
                &session_ids,
                &to_i64(
                    per_session_limit.clamp(1, 500),
                    "runtime outbox per-session limit",
                )?,
            ],
        )
    }

    pub fn active_session_runtime_outbox(
        &self,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        self.query_runtime_outbox(
            "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,
                    session_generation,decision,target_turn_id,classification_json,status,
                    runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                    claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                    updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch FROM session_runtime_outbox
              WHERE status NOT IN ('completed','supplemented','failed','cancelled','expired')
              ORDER BY updated_at_ms DESC,sequence DESC,request_id DESC LIMIT $1",
            &[&to_i64(limit.clamp(1, 500), "runtime outbox limit")?],
        )
    }

    pub fn blocked_session_runtime_outbox(
        &self,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        self.query_runtime_outbox(
            "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,
                    session_generation,decision,target_turn_id,classification_json,status,
                    runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                    claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                    updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch FROM session_runtime_outbox
              WHERE status='blocked' ORDER BY updated_at_ms ASC,sequence ASC,request_id ASC LIMIT $1",
            &[&to_i64(limit.clamp(1,500), "runtime outbox limit")?],
        )
    }

    pub fn session_runtime_outbox_health(
        &self,
    ) -> session::SessionResult<SessionRuntimeOutboxHealth> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
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
            match parse_runtime_status(&status)? {
                SessionRuntimeInputStatus::Accepted => health.accepted = count,
                SessionRuntimeInputStatus::Classified => health.classified = count,
                SessionRuntimeInputStatus::Queued => health.queued = count,
                SessionRuntimeInputStatus::RejectedDuplicate => {
                    health.rejected_duplicate = count;
                }
                SessionRuntimeInputStatus::RejectedPolicy => {
                    health.rejected_policy = count;
                }
                SessionRuntimeInputStatus::Claimed => health.claimed = count,
                SessionRuntimeInputStatus::Running => health.running = count,
                SessionRuntimeInputStatus::Reclassified => health.reclassified = count,
                SessionRuntimeInputStatus::Completed => health.completed = count,
                SessionRuntimeInputStatus::Supplemented => health.supplemented = count,
                SessionRuntimeInputStatus::Failed => health.failed = count,
                SessionRuntimeInputStatus::Blocked => health.blocked = count,
                SessionRuntimeInputStatus::Cancelled => health.cancelled = count,
                SessionRuntimeInputStatus::Expired => health.expired = count,
            }
        }
        health.runnable_depth = health
            .accepted
            .saturating_add(health.classified)
            .saturating_add(health.queued)
            .saturating_add(health.claimed)
            .saturating_add(health.running)
            .saturating_add(health.reclassified);
        health.oldest_runnable_created_at_ms = connection
            .query_one(
                "SELECT MIN(created_at_ms) FROM session_runtime_outbox
                  WHERE status IN ('accepted','classified','queued','claimed','running','reclassified')",
                &[],
            )
            .map_err(postgres_error)?
            .try_get::<_, Option<i64>>(0)
            .map_err(postgres_error)?
            .map(|value| value.max(0) as u64);
        Ok(health)
    }

    /// Export every normalized PG table in canonical SQL order. This is a
    /// cutover-only API; normal request handling stays on the selected owner.
    pub fn export_migration_snapshot(&self) -> session::SessionResult<SessionMigrationSnapshot> {
        let mut connection = self.executor.checkout_background().map_err(storage_error)?;
        let sessions = connection
            .query("SELECT session_id,platform,chat_id,user_id,model,created_at,last_activity,message_count,reset_policy,metadata_json,input_tokens,output_tokens,estimated_cost_usd,status FROM session_records ORDER BY session_id", &[])
            .map_err(postgres_error)?
            .iter()
            .map(row_to_session)
            .collect::<session::SessionResult<_>>()
            .map_err(|error| migration_export_error("session_records", error))?;
        let input_admissions = connection
            .query(
                "SELECT session_id,input_generation,input_admission_open
                   FROM session_records ORDER BY session_id",
                &[],
            )
            .map_err(postgres_error)?
            .iter()
            .map(|row| {
                Ok(SessionInputAdmission {
                    session_id: row.try_get(0).map_err(postgres_error)?,
                    generation: i64_to_u64(
                        row.try_get(1).map_err(postgres_error)?,
                        "session input generation",
                    )?,
                    open: row.try_get(2).map_err(postgres_error)?,
                })
            })
            .collect::<session::SessionResult<Vec<_>>>()?;
        let lifecycle_intents = connection
            .query(
                "SELECT operation_id,session_id,disposition,phase,last_stable_phase,
                        expected_generation,created_at_ms,updated_at_ms,last_error,revision
                   FROM session_lifecycle_intents ORDER BY operation_id",
                &[],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_lifecycle_intent)
            .collect::<session::SessionResult<Vec<_>>>()?;
        let branch_activations = connection
            .query(
                "SELECT operation_id,source_session_id,target_session_id,source_message_count,
                        phase,created_at_ms,updated_at_ms,last_error,revision
                   FROM session_branch_activations ORDER BY operation_id",
                &[],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_branch_activation)
            .collect::<session::SessionResult<Vec<_>>>()?;
        let associations = connection.query("SELECT session_id,memory_id,created_at FROM session_memory_associations ORDER BY session_id,memory_id",&[]).map_err(postgres_error)?.iter().map(|row| Ok(SessionMemoryAssociation { session_id: row.try_get(0).map_err(postgres_error)?, memory_id: row.try_get(1).map_err(postgres_error)?, created_at: row.try_get(2).map_err(postgres_error)?})).collect::<session::SessionResult<_>>()?;
        let messages = connection.query("SELECT stable_message_id,session_id,sequence,role,content_json,blocks_count,tool_use_id,tool_name,token_usage_json,created_at_ms FROM session_messages ORDER BY session_id,sequence",&[]).map_err(postgres_error)?.iter().map(row_to_message).collect::<session::SessionResult<_>>()?;
        let events = connection.query("SELECT session_id,event_type,event_json,sequence,created_at_ms FROM session_events ORDER BY session_id,sequence",&[]).map_err(postgres_error)?.iter().map(row_to_event).collect::<session::SessionResult<_>>()?;
        let checkpoints = connection.query("SELECT session_id,checkpoint_id FROM session_event_checkpoints ORDER BY session_id,checkpoint_id",&[]).map_err(postgres_error)?.iter().map(|row| Ok(SessionEventCheckpoint {session_id: row.try_get(0).map_err(postgres_error)?,checkpoint_id: row.try_get(1).map_err(postgres_error)?})).collect::<session::SessionResult<_>>()?;
        let snapshots = connection.query("SELECT session_id,event_idx,messages_json,created_at_ms FROM session_snapshots ORDER BY session_id,event_idx",&[]).map_err(postgres_error)?.iter().map(row_to_snapshot).collect::<session::SessionResult<_>>()?;
        let runtime_outbox = connection
            .query("SELECT input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,decision,target_turn_id,classification_json,status,runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch FROM session_runtime_outbox ORDER BY request_id",&[])
            .map_err(postgres_error)?
            .iter()
            .map(row_to_runtime_outbox)
            .collect::<session::SessionResult<_>>()
            .map_err(|error| migration_export_error("session_runtime_outbox", error))?;
        let mission_outbox = connection
            .query("SELECT request_id,session_id,title,workspace_key,operation,status,attempts,next_attempt_at_ms,claim_owner,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms FROM session_mission_outbox ORDER BY request_id",&[])
            .map_err(postgres_error)?
            .iter()
            .map(row_to_mission_outbox)
            .collect::<session::SessionResult<_>>()
            .map_err(|error| migration_export_error("session_mission_outbox", error))?;
        let runtime_history = pg_history_rows(&mut connection, "session_runtime_outbox_history")?;
        let mission_history = pg_history_rows(&mut connection, "session_mission_outbox_history")?;
        Ok(SessionMigrationSnapshot {
            schema_version: 5,
            sessions,
            input_admissions,
            lifecycle_intents,
            branch_activations,
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
    ) -> session::SessionResult<()> {
        if snapshot.schema_version != 5 {
            return Err(session::SessionError::Store(format!(
                "unsupported session migration schema {}",
                snapshot.schema_version
            )));
        }
        let existing = self.export_migration_snapshot()?;
        if !snapshot_is_empty(&existing) {
            if existing.canonical_digest()? == snapshot.canonical_digest()? {
                return Ok(());
            }
            return Err(session::SessionError::Store(
                "refusing divergent non-empty PostgreSQL session target".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_background().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        for session in &snapshot.sessions {
            upsert_session_tx(&mut transaction, session)?;
        }
        for admission in &snapshot.input_admissions {
            let changed = transaction
                .execute(
                    "UPDATE session_records
                        SET input_generation=$1,input_admission_open=$2
                      WHERE session_id=$3",
                    &[
                        &to_u64_i64(admission.generation, "session input generation")?,
                        &admission.open,
                        &admission.session_id,
                    ],
                )
                .map_err(postgres_error)?;
            if changed != 1 {
                return Err(session::SessionError::Store(format!(
                    "session admission `{}` has no imported owner",
                    admission.session_id
                )));
            }
        }
        for intent in &snapshot.lifecycle_intents {
            import_lifecycle_intent_tx(&mut transaction, intent)?;
        }
        for activation in &snapshot.branch_activations {
            import_branch_activation_tx(&mut transaction, activation)?;
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
    ) -> session::SessionResult<Vec<SessionRecord>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
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
    ) -> session::SessionResult<Vec<SessionMessage>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
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
    ) -> session::SessionResult<Vec<SessionEvent>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
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
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
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
    ) -> session::SessionResult<usize> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
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
    ) -> session::SessionResult<usize> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
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
) -> session::SessionResult<()> {
    let mut connection = executor.checkout_background().map_err(storage_error)?;
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
    "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,
            decision,target_turn_id,classification_json,status,runtime_commit_cursor,attempts,
            next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,last_error,
            revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch
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
) -> session::SessionResult<()> {
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

fn validate_mission_request(request: &SessionMissionOutboxRequest) -> session::SessionResult<()> {
    if request.request_id.trim().is_empty()
        || request.session_id.trim().is_empty()
        || request.title.trim().is_empty()
        || request.workspace_key.trim().is_empty()
    {
        return Err(session::SessionError::Store(
            "mission outbox requires non-empty request, session, title and workspace identities"
                .to_string(),
        ));
    }
    Ok(())
}

fn insert_mission_outbox_tx(
    transaction: &mut PostgresTransaction<'_>,
    request: &SessionMissionOutboxRequest,
) -> session::SessionResult<SessionMissionOutboxRecord> {
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
        return Err(session::SessionError::Store(format!(
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

fn row_to_mission_outbox(row: &Row) -> session::SessionResult<SessionMissionOutboxRecord> {
    let operation: String = row.try_get(4).map_err(postgres_error)?;
    let status: String = row.try_get(5).map_err(postgres_error)?;
    let failure: Option<String> = row.try_get(10).map_err(postgres_error)?;
    Ok(SessionMissionOutboxRecord {
        request_id: row.try_get(0).map_err(postgres_error)?,
        session_id: row.try_get(1).map_err(postgres_error)?,
        title: row.try_get(2).map_err(postgres_error)?,
        workspace_key: row.try_get(3).map_err(postgres_error)?,
        operation: SessionMissionOutboxOperation::parse(&operation)
            .map_err(|error| session::SessionError::Store(error.to_string()))?,
        status: OutboxStatus::parse(&status)
            .map_err(|error| session::SessionError::Store(error.to_string()))?,
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
                    .map_err(|error| session::SessionError::Store(error.to_string()))
            })
            .transpose()?,
        last_error: row.try_get(11).map_err(postgres_error)?,
        revision: i64_to_u64(row.try_get(12).map_err(postgres_error)?, "mission revision")?,
        created_at_ms: i64_to_u64(
            row.try_get(13).map_err(postgres_error)?,
            "mission created time",
        )?,
        updated_at_ms: i64_to_u64(
            row.try_get(14).map_err(postgres_error)?,
            "mission updated time",
        )?,
    })
}

fn input_decision_as_str(decision: InputRoutingDecision) -> &'static str {
    match decision {
        InputRoutingDecision::StartNewTurn => "start_new_turn",
        InputRoutingDecision::SupplementCurrentTurn => "supplement_current_turn",
        InputRoutingDecision::InterruptAndReplan => "interrupt_and_replan",
        InputRoutingDecision::EnqueueNextStep => "enqueue_next_step",
        InputRoutingDecision::SpawnSubtask => "spawn_subtask",
        InputRoutingDecision::RouteCrossSession => "route_cross_session",
        InputRoutingDecision::CreateNewSession => "create_new_session",
        InputRoutingDecision::ControlOrApproval => "control_or_approval",
        InputRoutingDecision::RejectDuplicate => "reject_duplicate",
        InputRoutingDecision::RejectPolicy => "reject_policy",
    }
}

fn parse_input_decision(value: &str) -> session::SessionResult<InputRoutingDecision> {
    match value {
        "start_new_turn" => Ok(InputRoutingDecision::StartNewTurn),
        "supplement_current_turn" => Ok(InputRoutingDecision::SupplementCurrentTurn),
        "interrupt_and_replan" => Ok(InputRoutingDecision::InterruptAndReplan),
        "enqueue_next_step" => Ok(InputRoutingDecision::EnqueueNextStep),
        "spawn_subtask" => Ok(InputRoutingDecision::SpawnSubtask),
        "route_cross_session" => Ok(InputRoutingDecision::RouteCrossSession),
        "create_new_session" => Ok(InputRoutingDecision::CreateNewSession),
        "control_or_approval" => Ok(InputRoutingDecision::ControlOrApproval),
        "reject_duplicate" => Ok(InputRoutingDecision::RejectDuplicate),
        "reject_policy" => Ok(InputRoutingDecision::RejectPolicy),
        other => Err(session::SessionError::Store(format!(
            "unknown session input decision `{other}`"
        ))),
    }
}

fn decision_requires_target_turn(decision: InputRoutingDecision) -> bool {
    matches!(
        decision,
        InputRoutingDecision::SupplementCurrentTurn
            | InputRoutingDecision::InterruptAndReplan
            | InputRoutingDecision::ControlOrApproval
    )
}

fn parse_runtime_status(value: &str) -> session::SessionResult<SessionRuntimeInputStatus> {
    match value {
        "accepted" => Ok(SessionRuntimeInputStatus::Accepted),
        "classified" => Ok(SessionRuntimeInputStatus::Classified),
        "queued" | "pending" | "retry_scheduled" => Ok(SessionRuntimeInputStatus::Queued),
        "rejected_duplicate" => Ok(SessionRuntimeInputStatus::RejectedDuplicate),
        "rejected_policy" => Ok(SessionRuntimeInputStatus::RejectedPolicy),
        "claimed" => Ok(SessionRuntimeInputStatus::Claimed),
        "running" => Ok(SessionRuntimeInputStatus::Running),
        "reclassified" => Ok(SessionRuntimeInputStatus::Reclassified),
        "completed" | "materialized" => Ok(SessionRuntimeInputStatus::Completed),
        "supplemented" => Ok(SessionRuntimeInputStatus::Supplemented),
        "failed" => Ok(SessionRuntimeInputStatus::Failed),
        "blocked" | "blocked_materialization" => Ok(SessionRuntimeInputStatus::Blocked),
        "cancelled" => Ok(SessionRuntimeInputStatus::Cancelled),
        "expired" => Ok(SessionRuntimeInputStatus::Expired),
        other => Err(session::SessionError::Store(format!(
            "unknown session runtime input status `{other}`"
        ))),
    }
}

fn row_to_lifecycle_intent(row: &Row) -> session::SessionResult<SessionLifecycleIntent> {
    Ok(SessionLifecycleIntent {
        operation_id: row.try_get(0).map_err(postgres_error)?,
        session_id: row.try_get(1).map_err(postgres_error)?,
        disposition: session::SessionCloseDisposition::parse(
            &row.try_get::<_, String>(2).map_err(postgres_error)?,
        )?,
        phase: SessionLifecyclePhase::parse(&row.try_get::<_, String>(3).map_err(postgres_error)?)?,
        last_stable_phase: SessionLifecyclePhase::parse(
            &row.try_get::<_, String>(4).map_err(postgres_error)?,
        )?,
        expected_generation: i64_to_u64(
            row.try_get(5).map_err(postgres_error)?,
            "lifecycle expected generation",
        )?,
        created_at_ms: i64_to_u64(
            row.try_get(6).map_err(postgres_error)?,
            "lifecycle created time",
        )?,
        updated_at_ms: i64_to_u64(
            row.try_get(7).map_err(postgres_error)?,
            "lifecycle updated time",
        )?,
        last_error: row.try_get(8).map_err(postgres_error)?,
        revision: i64_to_u64(
            row.try_get(9).map_err(postgres_error)?,
            "lifecycle revision",
        )?,
    })
}

fn query_lifecycle_intent_tx(
    transaction: &mut PostgresTransaction<'_>,
    operation_id: &str,
    lock: bool,
) -> session::SessionResult<Option<SessionLifecycleIntent>> {
    let suffix = if lock { " FOR UPDATE" } else { "" };
    transaction
        .query_opt(
            &format!(
                "SELECT operation_id,session_id,disposition,phase,last_stable_phase,
                        expected_generation,created_at_ms,updated_at_ms,last_error,revision
                   FROM session_lifecycle_intents WHERE operation_id=$1{suffix}"
            ),
            &[&operation_id],
        )
        .map_err(postgres_error)?
        .map(|row| row_to_lifecycle_intent(&row))
        .transpose()
}

fn transition_lifecycle_intent_tx(
    transaction: &mut PostgresTransaction<'_>,
    transition: &SessionLifecycleTransition,
) -> session::SessionResult<SessionLifecycleIntent> {
    let current = query_lifecycle_intent_tx(transaction, &transition.operation_id, true)?
        .ok_or_else(|| {
            session::SessionError::Store(format!(
                "Session lifecycle intent `{}` does not exist",
                transition.operation_id
            ))
        })?;
    transition.validate(&current)?;
    let last_stable_phase = if transition.next_phase == SessionLifecyclePhase::Failed {
        current.last_stable_phase
    } else {
        transition.next_phase
    };
    let changed = transaction
        .execute(
            "UPDATE session_lifecycle_intents
                SET phase=$1,last_stable_phase=$2,updated_at_ms=$3,last_error=$4,
                    revision=revision+1
              WHERE operation_id=$5 AND phase=$6 AND revision=$7",
            &[
                &transition.next_phase.as_str(),
                &last_stable_phase.as_str(),
                &to_u64_i64(transition.updated_at_ms, "lifecycle transition time")?,
                &transition.error,
                &transition.operation_id,
                &transition.expected_phase.as_str(),
                &to_u64_i64(transition.expected_revision, "lifecycle revision")?,
            ],
        )
        .map_err(postgres_error)?;
    if changed != 1 {
        return Err(session::SessionError::Store(format!(
            "Session lifecycle intent `{}` changed during transition",
            transition.operation_id
        )));
    }
    query_lifecycle_intent_tx(transaction, &transition.operation_id, false)?.ok_or_else(|| {
        session::SessionError::Store(format!(
            "Session lifecycle intent `{}` disappeared after transition",
            transition.operation_id
        ))
    })
}

fn row_to_branch_activation(row: &Row) -> session::SessionResult<SessionBranchActivation> {
    Ok(SessionBranchActivation {
        operation_id: row.try_get(0).map_err(postgres_error)?,
        source_session_id: row.try_get(1).map_err(postgres_error)?,
        target_session_id: row.try_get(2).map_err(postgres_error)?,
        source_message_count: from_i64(
            row.try_get(3).map_err(postgres_error)?,
            "branch source cutoff",
        )?,
        phase: SessionBranchActivationPhase::parse(
            &row.try_get::<_, String>(4).map_err(postgres_error)?,
        )?,
        created_at_ms: i64_to_u64(
            row.try_get(5).map_err(postgres_error)?,
            "branch activation created time",
        )?,
        updated_at_ms: i64_to_u64(
            row.try_get(6).map_err(postgres_error)?,
            "branch activation updated time",
        )?,
        last_error: row.try_get(7).map_err(postgres_error)?,
        revision: i64_to_u64(
            row.try_get(8).map_err(postgres_error)?,
            "branch activation revision",
        )?,
    })
}

fn query_branch_activation_tx(
    transaction: &mut PostgresTransaction<'_>,
    operation_id: &str,
    lock: bool,
) -> session::SessionResult<Option<SessionBranchActivation>> {
    let suffix = if lock { " FOR UPDATE" } else { "" };
    transaction
        .query_opt(
            &format!(
                "SELECT operation_id,source_session_id,target_session_id,
                        source_message_count,phase,created_at_ms,updated_at_ms,
                        last_error,revision
                   FROM session_branch_activations WHERE operation_id=$1{suffix}"
            ),
            &[&operation_id],
        )
        .map_err(postgres_error)?
        .map(|row| row_to_branch_activation(&row))
        .transpose()
}

fn append_allocated_event_tx(
    transaction: &mut PostgresTransaction<'_>,
    event: &SessionEvent,
) -> session::SessionResult<SessionEvent> {
    let sequence: i64 = transaction
        .query_one(
            "SELECT COALESCE(MAX(sequence),-1)+1
               FROM session_events WHERE session_id=$1",
            &[&event.session_id],
        )
        .map_err(postgres_error)?
        .try_get(0)
        .map_err(postgres_error)?;
    let sequence_usize = from_i64(sequence, "Session event sequence")?;
    let event_json = event_json_with_allocated_sequence(event, sequence_usize)?;
    transaction
        .execute(
            "INSERT INTO session_events(
                 session_id,sequence,event_type,event_json,created_at_ms
             ) VALUES($1,$2,$3,$4,$5)",
            &[
                &event.session_id,
                &sequence,
                &event.event_type,
                &event_json,
                &to_u64_i64(event.created_at_ms, "Session event time")?,
            ],
        )
        .map_err(postgres_error)?;
    let mut stored = event.clone();
    stored.sequence = sequence_usize;
    stored.event_json = event_json;
    Ok(stored)
}

fn query_input_admission_tx(
    transaction: &mut PostgresTransaction<'_>,
    session_id: &str,
    lock: bool,
) -> session::SessionResult<Option<SessionInputAdmission>> {
    let suffix = if lock { " FOR UPDATE" } else { "" };
    transaction
        .query_opt(
            &format!(
                "SELECT session_id,input_generation,input_admission_open
                   FROM session_records WHERE session_id=$1{suffix}"
            ),
            &[&session_id],
        )
        .map_err(postgres_error)?
        .map(|row| {
            Ok(SessionInputAdmission {
                session_id: row.try_get(0).map_err(postgres_error)?,
                generation: i64_to_u64(
                    row.try_get(1).map_err(postgres_error)?,
                    "session input generation",
                )?,
                open: row.try_get(2).map_err(postgres_error)?,
            })
        })
        .transpose()
}

fn require_input_admission_tx(
    transaction: &mut PostgresTransaction<'_>,
    session_id: &str,
    generation: u64,
) -> session::SessionResult<()> {
    // Callers that allocate transcript sequence lock the Session owner first.
    // Worker transitions already hold the outbox row, so taking the Session
    // row lock here would invert the generation-advance lock order.
    let admission = query_input_admission_tx(transaction, session_id, false)?
        .ok_or_else(|| session::SessionError::Store(format!("session `{session_id}` not found")))?;
    if !admission.open {
        return Err(session::SessionError::Store(format!(
            "session `{session_id}` input admission is closed"
        )));
    }
    if admission.generation != generation {
        return Err(session::SessionError::Store(format!(
            "session `{session_id}` generation mismatch: expected {generation}, current {}",
            admission.generation
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_input_timeline_event_tx(
    transaction: &mut PostgresTransaction<'_>,
    request: &SessionRuntimeOutboxRequest,
    session_id: &str,
    input_sequence: usize,
    kind: &str,
    status: SessionRuntimeInputStatus,
    actor: Option<&str>,
    reason: Option<&str>,
    created_at_ms: u64,
) -> session::SessionResult<()> {
    let sequence: i64 = transaction
        .query_one(
            "SELECT COALESCE(MAX(sequence),-1)+1
               FROM session_events WHERE session_id=$1",
            &[&session_id],
        )
        .map_err(postgres_error)?
        .try_get(0)
        .map_err(postgres_error)?;
    let mut refs = vec![
        SessionDomainRef {
            ref_type: "session_input".to_string(),
            id: request.input_id.clone(),
            label: None,
        },
        SessionDomainRef {
            ref_type: "message".to_string(),
            id: request.message_id.clone(),
            label: None,
        },
        SessionDomainRef {
            ref_type: "turn".to_string(),
            id: request.turn_id.clone(),
            label: None,
        },
    ];
    if let Some(target_turn_id) = request.target_turn_id.as_ref() {
        refs.push(SessionDomainRef {
            ref_type: "target_turn".to_string(),
            id: target_turn_id.clone(),
            label: None,
        });
    }
    let mut event = SessionDomainEvent::new(
        session_id,
        from_i64(sequence, "session input event sequence")?,
        SessionDomainScope::Message,
        kind,
        serde_json::json!({
            "input_id": request.input_id,
            "request_id": request.request_id,
            "message_id": request.message_id,
            "turn_id": request.turn_id,
            "input_sequence": input_sequence,
            "session_generation": request.session_generation,
            "decision": input_decision_as_str(request.decision),
            "target_turn_id": request.target_turn_id,
            "classification": request.classification_json,
            "actor": actor,
            "reason": reason,
        }),
        created_at_ms,
    );
    event.event_id = format!(
        "session-input:{}:{}:{kind}",
        request.request_id, request.session_generation
    );
    event.correlation_id = Some(request.request_id.clone());
    event.status = Some(status.as_str().to_string());
    event.refs = refs;
    let stored = event.to_session_event().map_err(|error| {
        session::SessionError::Store(format!("session input event encode failed: {error}"))
    })?;
    transaction
        .execute(
            "INSERT INTO session_events(
                 session_id,event_type,event_json,sequence,created_at_ms
             ) VALUES($1,$2,$3,$4,$5)",
            &[
                &stored.session_id,
                &stored.event_type,
                &stored.event_json,
                &to_i64(stored.sequence, "session input event sequence")?,
                &to_u64_i64(stored.created_at_ms, "session input event time")?,
            ],
        )
        .map_err(postgres_error)?;
    Ok(())
}

fn append_admission_timeline_event_tx(
    transaction: &mut PostgresTransaction<'_>,
    session_id: &str,
    previous_generation: u64,
    admission: &SessionInputAdmission,
    actor: &str,
    reason: &str,
    created_at_ms: u64,
) -> session::SessionResult<()> {
    let sequence: i64 = transaction
        .query_one(
            "SELECT COALESCE(MAX(sequence),-1)+1
               FROM session_events WHERE session_id=$1",
            &[&session_id],
        )
        .map_err(postgres_error)?
        .try_get(0)
        .map_err(postgres_error)?;
    let kind = if admission.open {
        "session.input.generation.advanced.v1"
    } else {
        "session.input.admission.closed.v1"
    };
    let mut event = SessionDomainEvent::new(
        session_id,
        from_i64(sequence, "session admission event sequence")?,
        SessionDomainScope::Session,
        kind,
        serde_json::json!({
            "previous_generation": previous_generation,
            "generation": admission.generation,
            "admission_open": admission.open,
            "actor": actor,
            "reason": reason,
        }),
        created_at_ms,
    );
    event.event_id = format!(
        "session-input-admission:{session_id}:{}",
        admission.generation
    );
    event.status = Some(if admission.open { "open" } else { "closed" }.to_string());
    let stored = event.to_session_event().map_err(|error| {
        session::SessionError::Store(format!("session admission event encode failed: {error}"))
    })?;
    transaction
        .execute(
            "INSERT INTO session_events(
                 session_id,event_type,event_json,sequence,created_at_ms
             ) VALUES($1,$2,$3,$4,$5)",
            &[
                &stored.session_id,
                &stored.event_type,
                &stored.event_json,
                &to_i64(stored.sequence, "session admission event sequence")?,
                &to_u64_i64(stored.created_at_ms, "session admission event time")?,
            ],
        )
        .map_err(postgres_error)?;
    Ok(())
}

fn request_from_outbox(record: &SessionRuntimeOutboxRecord) -> SessionRuntimeOutboxRequest {
    SessionRuntimeOutboxRequest {
        input_id: record.input_id.clone(),
        request_id: record.request_id.clone(),
        turn_id: record.turn_id.clone(),
        message_id: record.message_id.clone(),
        session_generation: record.session_generation,
        decision: record.decision,
        target_turn_id: record.target_turn_id.clone(),
        classification_json: record.classification_json.clone(),
        created_at_ms: record.created_at_ms,
        runtime_options_json: record.runtime_options_json.clone(),
    }
}

fn validate_runtime_request(
    message: &SessionMessage,
    request: &SessionRuntimeOutboxRequest,
) -> session::SessionResult<()> {
    if request.input_id.trim().is_empty()
        || request.request_id.trim().is_empty()
        || request.turn_id.trim().is_empty()
        || request.message_id.trim().is_empty()
        || message.session_id.trim().is_empty()
        || request.session_generation == 0
    {
        return Err(session::SessionError::Store(
            "durable session input requires non-empty identities and a positive generation"
                .to_string(),
        ));
    }
    if request.message_id != message.stable_message_id {
        return Err(session::SessionError::Store(
            "runtime outbox message_id must equal the durable message identity".to_string(),
        ));
    }
    validate_runtime_input_request(request)?;
    Ok(())
}

fn validate_runtime_input_request(
    request: &SessionRuntimeOutboxRequest,
) -> session::SessionResult<()> {
    if request.input_id.trim().is_empty()
        || request.request_id.trim().is_empty()
        || request.turn_id.trim().is_empty()
        || request.message_id.trim().is_empty()
        || request.session_generation == 0
    {
        return Err(session::SessionError::Store(
            "durable session input requires non-empty identities and a positive generation"
                .to_string(),
        ));
    }
    if decision_requires_target_turn(request.decision)
        && request.target_turn_id.as_deref().is_none_or(str::is_empty)
    {
        return Err(session::SessionError::Store(format!(
            "decision `{}` requires target_turn_id",
            input_decision_as_str(request.decision)
        )));
    }
    Ok(())
}

fn insert_runtime_outbox_tx(
    transaction: &mut PostgresTransaction<'_>,
    message: &SessionMessage,
    request: &SessionRuntimeOutboxRequest,
) -> session::SessionResult<SessionRuntimeOutboxRecord> {
    let now = to_u64_i64(request.created_at_ms, "runtime outbox time")?;
    let row = transaction.query_one(
        "INSERT INTO session_runtime_outbox(
             input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,
             decision,target_turn_id,classification_json,status,attempts,next_attempt_at_ms,
             revision,created_at_ms,updated_at_ms,runtime_options_json
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'accepted',0,$11,0,$11,$11,$12)
         RETURNING input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,
                   decision,target_turn_id,classification_json,status,runtime_commit_cursor,attempts,
                   next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,
                   last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch",
        &[&request.input_id,&request.request_id,&request.turn_id,&request.message_id,
          &message.session_id,&to_i64(message.sequence, "message sequence")?,
          &to_u64_i64(request.session_generation, "session generation")?,
          &input_decision_as_str(request.decision),&request.target_turn_id,
          &request.classification_json,&now,&request.runtime_options_json],
    ).map_err(postgres_error)?;
    let accepted = row_to_runtime_outbox(&row)?;
    append_runtime_history_tx(
        transaction,
        &accepted,
        "accepted",
        None,
        None,
        SessionRuntimeInputStatus::Accepted,
        SessionRuntimeInputStatus::Accepted,
        None,
        request.created_at_ms,
    )?;
    append_input_timeline_event_tx(
        transaction,
        request,
        &message.session_id,
        message.sequence,
        "session.input.accepted.v1",
        SessionRuntimeInputStatus::Accepted,
        None,
        None,
        request.created_at_ms,
    )?;
    let row = transaction.query_one(
        "UPDATE session_runtime_outbox
            SET status='classified',revision=revision+1
          WHERE request_id=$1 AND revision=0
        RETURNING input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,
                  decision,target_turn_id,classification_json,status,runtime_commit_cursor,attempts,
                  next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,
                  last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch",
        &[&request.request_id],
    ).map_err(postgres_error)?;
    let classified = row_to_runtime_outbox(&row)?;
    append_runtime_history_tx(
        transaction,
        &classified,
        "classified",
        None,
        Some(accepted.revision),
        SessionRuntimeInputStatus::Accepted,
        SessionRuntimeInputStatus::Classified,
        request.classification_json.as_deref(),
        request.created_at_ms,
    )?;
    append_input_timeline_event_tx(
        transaction,
        request,
        &message.session_id,
        message.sequence,
        "session.input.classified.v1",
        SessionRuntimeInputStatus::Classified,
        None,
        None,
        request.created_at_ms,
    )?;
    let final_status = SessionRuntimeInputStatus::for_rejection(request.decision)
        .unwrap_or(SessionRuntimeInputStatus::Queued);
    let final_status_name = final_status.as_str();
    let terminal_at_ms = final_status
        .is_terminal()
        .then_some(to_u64_i64(request.created_at_ms, "runtime terminal time")?);
    let row = transaction.query_one(
        "UPDATE session_runtime_outbox
            SET status=$1,terminal_at_ms=$2,revision=revision+1
          WHERE request_id=$3 AND revision=$4
        RETURNING input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,
                  decision,target_turn_id,classification_json,status,runtime_commit_cursor,attempts,
                  next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,
                  last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch",
        &[
            &final_status_name,
            &terminal_at_ms,
            &request.request_id,
            &to_u64_i64(classified.revision, "classified revision")?,
        ],
    ).map_err(postgres_error)?;
    let finalized = row_to_runtime_outbox(&row)?;
    append_runtime_history_tx(
        transaction,
        &finalized,
        if final_status.is_terminal() {
            "rejected"
        } else {
            "queued"
        },
        None,
        Some(classified.revision),
        SessionRuntimeInputStatus::Classified,
        final_status,
        request.classification_json.as_deref(),
        request.created_at_ms,
    )?;
    append_input_timeline_event_tx(
        transaction,
        request,
        &message.session_id,
        message.sequence,
        final_status.timeline_event_kind(),
        final_status,
        None,
        request.classification_json.as_deref(),
        request.created_at_ms,
    )?;
    Ok(finalized)
}

fn row_to_runtime_outbox(row: &Row) -> session::SessionResult<SessionRuntimeOutboxRecord> {
    let decision: String = row.try_get(7).map_err(postgres_error)?;
    let status: String = row.try_get(10).map_err(postgres_error)?;
    let failure: Option<String> = row.try_get(17).map_err(postgres_error)?;
    Ok(SessionRuntimeOutboxRecord {
        input_id: row.try_get(0).map_err(postgres_error)?,
        request_id: row.try_get(1).map_err(postgres_error)?,
        turn_id: row.try_get(2).map_err(postgres_error)?,
        message_id: row.try_get(3).map_err(postgres_error)?,
        session_id: row.try_get(4).map_err(postgres_error)?,
        sequence: from_i64(
            row.try_get(5).map_err(postgres_error)?,
            "runtime message sequence",
        )?,
        session_generation: i64_to_u64(
            row.try_get(6).map_err(postgres_error)?,
            "session generation",
        )?,
        decision: parse_input_decision(&decision)?,
        target_turn_id: row.try_get(8).map_err(postgres_error)?,
        classification_json: row.try_get(9).map_err(postgres_error)?,
        status: parse_runtime_status(&status)?,
        runtime_commit_cursor: row
            .try_get::<_, Option<i64>>(11)
            .map_err(postgres_error)?
            .map(|value| i64_to_u64(value, "runtime cursor"))
            .transpose()?,
        attempts: i64_to_u32(row.try_get(12).map_err(postgres_error)?, "runtime attempts")?,
        next_attempt_at_ms: i64_to_u64(
            row.try_get(13).map_err(postgres_error)?,
            "runtime next attempt",
        )?,
        claim_owner: row.try_get(14).map_err(postgres_error)?,
        claim_token: row.try_get(15).map_err(postgres_error)?,
        claim_expires_at_ms: row
            .try_get::<_, Option<i64>>(16)
            .map_err(postgres_error)?
            .map(|value| i64_to_u64(value, "runtime lease"))
            .transpose()?,
        failure_class: failure
            .map(|value| {
                OutboxFailureClass::parse(&value)
                    .map_err(|error| session::SessionError::Store(error.to_string()))
            })
            .transpose()?,
        last_error: row.try_get(18).map_err(postgres_error)?,
        revision: i64_to_u64(row.try_get(19).map_err(postgres_error)?, "runtime revision")?,
        created_at_ms: i64_to_u64(
            row.try_get(20).map_err(postgres_error)?,
            "runtime created time",
        )?,
        updated_at_ms: i64_to_u64(
            row.try_get(21).map_err(postgres_error)?,
            "runtime updated time",
        )?,
        terminal_at_ms: row
            .try_get::<_, Option<i64>>(22)
            .map_err(postgres_error)?
            .map(|value| i64_to_u64(value, "runtime terminal time"))
            .transpose()?,
        runtime_options_json: row.try_get(23).map_err(postgres_error)?,
        claim_fence_epoch: row
            .try_get::<_, Option<i64>>(24)
            .map_err(postgres_error)?
            .map(|value| i64_to_u64(value, "runtime claim fence epoch"))
            .transpose()?,
    })
}

fn pg_history_rows(
    connection: &mut PostgresConnection,
    table: &str,
) -> session::SessionResult<Vec<SessionOutboxHistory>> {
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
        && snapshot.input_admissions.is_empty()
        && snapshot.lifecycle_intents.is_empty()
        && snapshot.branch_activations.is_empty()
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

fn import_lifecycle_intent_tx(
    transaction: &mut PostgresTransaction<'_>,
    intent: &SessionLifecycleIntent,
) -> session::SessionResult<()> {
    transaction
        .execute(
            "INSERT INTO session_lifecycle_intents(
                 operation_id,session_id,disposition,phase,last_stable_phase,
                 expected_generation,created_at_ms,updated_at_ms,last_error,revision
             ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
            &[
                &intent.operation_id,
                &intent.session_id,
                &intent.disposition.as_str(),
                &intent.phase.as_str(),
                &intent.last_stable_phase.as_str(),
                &to_u64_i64(intent.expected_generation, "lifecycle expected generation")?,
                &to_u64_i64(intent.created_at_ms, "lifecycle created time")?,
                &to_u64_i64(intent.updated_at_ms, "lifecycle updated time")?,
                &intent.last_error,
                &to_u64_i64(intent.revision, "lifecycle revision")?,
            ],
        )
        .map_err(postgres_error)?;
    Ok(())
}

fn import_branch_activation_tx(
    transaction: &mut PostgresTransaction<'_>,
    activation: &SessionBranchActivation,
) -> session::SessionResult<()> {
    transaction
        .execute(
            "INSERT INTO session_branch_activations(
                 operation_id,source_session_id,target_session_id,source_message_count,
                 phase,created_at_ms,updated_at_ms,last_error,revision
             ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            &[
                &activation.operation_id,
                &activation.source_session_id,
                &activation.target_session_id,
                &to_i64(
                    activation.source_message_count,
                    "branch activation source cutoff",
                )?,
                &activation.phase.as_str(),
                &to_u64_i64(activation.created_at_ms, "branch activation created time")?,
                &to_u64_i64(activation.updated_at_ms, "branch activation updated time")?,
                &activation.last_error,
                &to_u64_i64(activation.revision, "branch activation revision")?,
            ],
        )
        .map_err(postgres_error)?;
    Ok(())
}

fn import_runtime_outbox_tx(
    transaction: &mut PostgresTransaction<'_>,
    item: &SessionRuntimeOutboxRecord,
) -> session::SessionResult<()> {
    transaction.execute(
        "INSERT INTO session_runtime_outbox(
             input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,
             decision,target_turn_id,classification_json,status,runtime_commit_cursor,attempts,
             next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,last_error,
             revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)",
        &[&item.input_id,&item.request_id,&item.turn_id,&item.message_id,&item.session_id,
          &to_i64(item.sequence,"runtime sequence")?,
          &to_u64_i64(item.session_generation,"session generation")?,
          &input_decision_as_str(item.decision),&item.target_turn_id,&item.classification_json,
          &item.status.as_str(),
          &item.runtime_commit_cursor.map(|value| to_u64_i64(value,"runtime cursor")).transpose()?,
          &to_i64(item.attempts as usize,"runtime attempts")?,
          &to_u64_i64(item.next_attempt_at_ms,"runtime next")?,&item.claim_owner,&item.claim_token,
          &item.claim_expires_at_ms.map(|value|to_u64_i64(value,"runtime lease")).transpose()?,
          &item.failure_class.map(OutboxFailureClass::as_str),&item.last_error,
          &to_u64_i64(item.revision,"runtime revision")?,
          &to_u64_i64(item.created_at_ms,"runtime created")?,
          &to_u64_i64(item.updated_at_ms,"runtime updated")?,
          &item.terminal_at_ms.map(|value|to_u64_i64(value,"runtime terminal")).transpose()?,
          &item.runtime_options_json,
          &item.claim_fence_epoch.map(|value|to_u64_i64(value,"runtime claim fence epoch")).transpose()?],
    ).map_err(postgres_error)?;
    Ok(())
}

fn import_mission_outbox_tx(
    transaction: &mut PostgresTransaction<'_>,
    item: &SessionMissionOutboxRecord,
) -> session::SessionResult<()> {
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
) -> session::SessionResult<()> {
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
) -> session::SessionResult<SessionMigrationManifest> {
    let snapshot = export_sqlite_session_snapshot(source)?;
    let source_digest = snapshot.canonical_digest()?;
    target.import_migration_snapshot(&snapshot)?;
    if export_sqlite_session_snapshot(source)?.canonical_digest()? != source_digest {
        return Err(session::SessionError::Store(
            "session SQLite source changed during quiesced copy".to_string(),
        ));
    }
    let target_digest = target.export_migration_snapshot()?.canonical_digest()?;
    if target_digest != source_digest {
        return Err(session::SessionError::Store(
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
            .map_err(|error| session::SessionError::Store(error.to_string()))?;
    }
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(&manifest)
            .map_err(|error| session::SessionError::Store(error.to_string()))?,
    )
    .map_err(|error| session::SessionError::Store(error.to_string()))?;
    fs::rename(&temporary, path)
        .map_err(|error| session::SessionError::Store(error.to_string()))?;
    Ok(manifest)
}

fn runtime_outbox_tx(
    transaction: &mut PostgresTransaction<'_>,
    request_id: &str,
) -> session::SessionResult<Option<SessionRuntimeOutboxRecord>> {
    transaction
        .query_opt(RUNTIME_OUTBOX_SELECT, &[&request_id])
        .map_err(postgres_error)?
        .map(|row| row_to_runtime_outbox(&row))
        .transpose()
}

fn runtime_outbox_for_update(
    transaction: &mut PostgresTransaction<'_>,
    request_id: &str,
) -> session::SessionResult<SessionRuntimeOutboxRecord> {
    transaction
        .query_opt(
            "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,
                decision,target_turn_id,classification_json,status,runtime_commit_cursor,attempts,
                next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,
                last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch
           FROM session_runtime_outbox
          WHERE request_id=$1 FOR UPDATE",
            &[&request_id],
        )
        .map_err(postgres_error)?
        .map(|row| row_to_runtime_outbox(&row))
        .transpose()?
        .ok_or_else(|| {
            session::SessionError::Store(format!("session runtime outbox `{request_id}` not found"))
        })
}

fn runtime_outbox_by_input_id_for_update(
    transaction: &mut PostgresTransaction<'_>,
    input_id: &str,
) -> session::SessionResult<SessionRuntimeOutboxRecord> {
    transaction
        .query_opt(
            "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,
                decision,target_turn_id,classification_json,status,runtime_commit_cursor,attempts,
                next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,
                last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch
           FROM session_runtime_outbox
          WHERE input_id=$1 FOR UPDATE",
            &[&input_id],
        )
        .map_err(postgres_error)?
        .map(|row| row_to_runtime_outbox(&row))
        .transpose()?
        .ok_or_else(|| {
            session::SessionError::Store(format!("session input `{input_id}` not found"))
        })
}

#[allow(clippy::too_many_arguments)]
fn assert_runtime_lease(
    transaction: &mut PostgresTransaction<'_>,
    record: &SessionRuntimeOutboxRecord,
    worker_id: &str,
    session_generation: u64,
    claim_token: &str,
    expected_revision: u64,
    now_ms: u64,
    allowed: &[SessionRuntimeInputStatus],
) -> session::SessionResult<()> {
    let admission =
        query_input_admission_tx(transaction, &record.session_id, false)?.ok_or_else(|| {
            session::SessionError::Store(format!("session `{}` not found", record.session_id))
        })?;
    if !allowed.contains(&record.status)
        || record.session_generation != session_generation
        || admission.generation != session_generation
        || !admission.open
        || record.claim_owner.as_deref() != Some(worker_id)
        || record.claim_token.as_deref() != Some(claim_token)
        || record.revision != expected_revision
        || record
            .claim_expires_at_ms
            .is_none_or(|expires| expires <= now_ms)
    {
        return Err(session::SessionError::Store(
            "runtime outbox transition rejected by generation/token/lease/revision fencing"
                .to_string(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_runtime_history_tx(
    transaction: &mut PostgresTransaction<'_>,
    record: &SessionRuntimeOutboxRecord,
    action: &str,
    actor: Option<&str>,
    expected_revision: Option<u64>,
    previous_status: SessionRuntimeInputStatus,
    next_status: SessionRuntimeInputStatus,
    detail: Option<&str>,
    created_at_ms: u64,
) -> session::SessionResult<()> {
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
) -> session::SessionResult<SessionMissionOutboxRecord> {
    transaction.query_opt(
        "SELECT request_id,session_id,title,workspace_key,operation,status,attempts,next_attempt_at_ms,
                claim_owner,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms
           FROM session_mission_outbox WHERE request_id=$1 FOR UPDATE",
        &[&request_id],
    ).map_err(postgres_error)?.map(|row| row_to_mission_outbox(&row)).transpose()?
        .ok_or_else(|| session::SessionError::Store(format!("mission outbox `{request_id}` was not found")))
}

fn assert_mission_lease(
    record: &SessionMissionOutboxRecord,
    worker_id: &str,
    expected_revision: u64,
    now_ms: u64,
) -> session::SessionResult<()> {
    if record.status != OutboxStatus::Claimed
        || record.claim_owner.as_deref() != Some(worker_id)
        || record.revision != expected_revision
        || record
            .claim_expires_at_ms
            .is_none_or(|expires| expires < now_ms)
    {
        return Err(session::SessionError::Store(
            "mission outbox transition rejected by lease/revision fencing".to_string(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
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
) -> session::SessionResult<()> {
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

fn row_to_session(row: &Row) -> session::SessionResult<SessionRecord> {
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

fn row_to_recovery_manifest(row: &Row) -> session::SessionResult<SessionRecoveryManifest> {
    Ok(SessionRecoveryManifest {
        session_id: row.try_get(0).map_err(postgres_error)?,
        durable_cursor: i64_to_u64(
            row.try_get(1).map_err(postgres_error)?,
            "recovery durable cursor",
        )?,
        event_cursor: i64_to_u64(
            row.try_get(2).map_err(postgres_error)?,
            "recovery event cursor",
        )?,
        history_revision: i64_to_u64(
            row.try_get(3).map_err(postgres_error)?,
            "recovery history revision",
        )?,
        transcript_messages: i64_to_u64(
            row.try_get(4).map_err(postgres_error)?,
            "recovery transcript messages",
        )?,
        transcript_bytes: i64_to_u64(
            row.try_get(5).map_err(postgres_error)?,
            "recovery transcript bytes",
        )?,
        latest_checkpoint_sequence: row
            .try_get::<_, Option<i64>>(6)
            .map_err(postgres_error)?
            .map(|value| i64_to_u64(value, "latest checkpoint sequence"))
            .transpose()?,
        latest_checkpoint_event_id: row.try_get(7).map_err(postgres_error)?,
        index_generation: i64_to_u64(
            row.try_get(8).map_err(postgres_error)?,
            "context index generation",
        )?,
        indexed_through_sequence: row
            .try_get::<_, Option<i64>>(9)
            .map_err(postgres_error)?
            .map(|value| i64_to_u64(value, "indexed through sequence"))
            .transpose()?,
        index_card_count: i64_to_u64(
            row.try_get(10).map_err(postgres_error)?,
            "context index card count",
        )?,
        index_pending: row.try_get(11).map_err(postgres_error)?,
        in_flight_turn: row.try_get(12).map_err(postgres_error)?,
        pending_approval: row.try_get(13).map_err(postgres_error)?,
        active_writer_or_attachment: row.try_get(14).map_err(postgres_error)?,
        mission_agent_team_continuation: row.try_get(15).map_err(postgres_error)?,
        last_activity_ms: i64_to_u64(
            row.try_get(16).map_err(postgres_error)?,
            "recovery last activity",
        )?,
        manifest_revision: i64_to_u64(
            row.try_get(17).map_err(postgres_error)?,
            "recovery manifest revision",
        )?,
    })
}

fn validate_terminal_transcript(
    terminal_message_id: &str,
    ingress_message_id: &str,
    session_id: &str,
    messages: &[SessionMessage],
) -> session::SessionResult<()> {
    if terminal_message_id.trim().is_empty()
        || ingress_message_id.trim().is_empty()
        || session_id.trim().is_empty()
        || messages.is_empty()
        || messages
            .last()
            .is_none_or(|message| message.stable_message_id != terminal_message_id)
    {
        return Err(session::SessionError::InvalidArgument(
            "terminal transcript requires a non-empty session, ingress, terminal ID, and final row"
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
        return Err(session::SessionError::InvalidArgument(
            "terminal transcript contains an invalid message row".to_string(),
        ));
    }
    let unique_ids = messages
        .iter()
        .map(|message| message.stable_message_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if unique_ids.len() != messages.len() {
        return Err(session::SessionError::InvalidArgument(
            "terminal transcript contains duplicate stable message IDs".to_string(),
        ));
    }
    Ok(())
}

fn validate_terminal_commit(
    request: &SessionTerminalTranscriptCommit,
) -> session::SessionResult<()> {
    if request.turn_id.trim().is_empty()
        || request.runtime_commit_cursor == 0
        || request.consumed_input_sequence < request.fence.input_sequence
        || request.fence.request_id.trim().is_empty()
        || request.fence.session_generation == 0
        || request.fence.claim_owner.trim().is_empty()
        || request.fence.claim_token.trim().is_empty()
        || request.fence.claim_fence_epoch == 0
    {
        return Err(session::SessionError::InvalidArgument(
            "terminal commit requires complete turn, cursor and live execution fence identity"
                .to_string(),
        ));
    }
    Ok(())
}

fn append_terminal_transcript_tx(
    transaction: &mut PostgresTransaction<'_>,
    terminal_message_id: &str,
    ingress_message_id: &str,
    session_id: &str,
    messages: &[SessionMessage],
    created_at_ms: u64,
) -> session::SessionResult<(Vec<SessionMessage>, bool)> {
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
                session::SessionError::Store(format!(
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
                return Err(session::SessionError::Store(format!(
                    "terminal transcript message_id `{}` conflicts with committed content",
                    requested.stable_message_id
                )));
            }
            committed.push(existing);
        }
        committed.sort_by_key(|message| message.sequence);
        return Ok((committed, false));
    }
    if loaded.iter().any(Option::is_some) {
        return Err(session::SessionError::Store(format!(
            "terminal transcript `{terminal_message_id}` collides with existing intermediate rows"
        )));
    }
    transaction
        .query_opt(
            "SELECT sequence FROM session_messages
              WHERE stable_message_id=$1 AND session_id=$2 AND role='user'",
            &[&ingress_message_id, &session_id],
        )
        .map_err(postgres_error)?
        .ok_or_else(|| {
            session::SessionError::Store(format!(
                "terminal transcript ingress `{ingress_message_id}` is not committed"
            ))
        })?;
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
        insert_message_tx(transaction, &message)?;
        committed.push(message);
    }
    let last_created_at = committed
        .last()
        .map_or(created_at_ms, |message| message.created_at_ms);
    refresh_session_message_summary_tx(transaction, session_id, last_created_at)?;
    refresh_session_usage_summary_tx(transaction, session_id)?;
    Ok((committed, true))
}

fn load_committed_terminal_transcript_tx(
    transaction: &mut PostgresTransaction<'_>,
    terminal_message_id: &str,
    messages: &[SessionMessage],
) -> session::SessionResult<Vec<SessionMessage>> {
    let mut committed = Vec::with_capacity(messages.len());
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
            .transpose()?
            .ok_or_else(|| {
                session::SessionError::StaleExecutionFence(format!(
                    "completed terminal transcript `{terminal_message_id}` does not match replay"
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
            return Err(session::SessionError::StaleExecutionFence(format!(
                "completed terminal transcript `{terminal_message_id}` content does not match replay"
            )));
        }
        committed.push(existing);
    }
    if committed
        .windows(2)
        .any(|pair| pair[0].sequence >= pair[1].sequence)
    {
        return Err(session::SessionError::StaleExecutionFence(format!(
            "completed terminal transcript `{terminal_message_id}` order does not match replay"
        )));
    }
    if committed
        .last()
        .is_none_or(|message| message.stable_message_id != terminal_message_id)
    {
        return Err(session::SessionError::StaleExecutionFence(format!(
            "completed terminal transcript `{terminal_message_id}` identity does not match replay"
        )));
    }
    Ok(committed)
}

fn refresh_session_message_summary_tx(
    transaction: &mut PostgresTransaction<'_>,
    session_id: &str,
    activity_ms: u64,
) -> session::SessionResult<()> {
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
) -> session::SessionResult<()> {
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
) -> session::SessionResult<()> {
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

fn row_to_session_search(row: &Row) -> session::SessionResult<SessionSearchResult> {
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

fn row_to_message(row: &Row) -> session::SessionResult<SessionMessage> {
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
            .map_err(|_| session::SessionError::Store("message time overflow".to_string()))?,
    })
}

fn row_to_message_metadata(row: &Row) -> session::SessionResult<SessionMessageMetadata> {
    Ok(SessionMessageMetadata {
        stable_message_id: row.try_get(0).map_err(postgres_error)?,
        session_id: row.try_get(1).map_err(postgres_error)?,
        sequence: from_i64(row.try_get(2).map_err(postgres_error)?, "message sequence")?,
        role: row.try_get(3).map_err(postgres_error)?,
        blocks_count: from_i64(row.try_get(4).map_err(postgres_error)?, "message blocks")?,
        tool_use_id: row.try_get(5).map_err(postgres_error)?,
        tool_name: row.try_get(6).map_err(postgres_error)?,
        created_at_ms: i64_to_u64(
            row.try_get(7).map_err(postgres_error)?,
            "message created time",
        )?,
        content_bytes: from_i64(
            row.try_get(8).map_err(postgres_error)?,
            "message content bytes",
        )?,
    })
}

fn row_to_context_index_card(row: &Row) -> session::SessionResult<ContextIndexCard> {
    Ok(ContextIndexCard {
        schema_version: session::CONTEXT_INDEX_CARD_SCHEMA_VERSION,
        card_id: row.try_get(0).map_err(postgres_error)?,
        parent_card_id: row.try_get(1).map_err(postgres_error)?,
        session_id: row.try_get(2).map_err(postgres_error)?,
        source_start_sequence: from_i64(
            row.try_get(3).map_err(postgres_error)?,
            "card source start",
        )?,
        source_end_sequence: from_i64(row.try_get(4).map_err(postgres_error)?, "card source end")?,
        source_message_count: from_i64(
            row.try_get(5).map_err(postgres_error)?,
            "card source count",
        )?,
        source_digest: row.try_get(6).map_err(postgres_error)?,
        summary: row.try_get(7).map_err(postgres_error)?,
        scope: row.try_get(8).map_err(postgres_error)?,
        authority: row.try_get(9).map_err(postgres_error)?,
        generation: i64_to_u64(row.try_get(10).map_err(postgres_error)?, "card generation")?,
        created_at_ms: i64_to_u64(
            row.try_get(11).map_err(postgres_error)?,
            "card created time",
        )?,
        updated_at_ms: i64_to_u64(
            row.try_get(12).map_err(postgres_error)?,
            "card updated time",
        )?,
    })
}

fn row_to_event(row: &Row) -> session::SessionResult<SessionEvent> {
    Ok(SessionEvent {
        session_id: row.try_get(0).map_err(postgres_error)?,
        event_type: row.try_get(1).map_err(postgres_error)?,
        event_json: row.try_get(2).map_err(postgres_error)?,
        sequence: from_i64(row.try_get(3).map_err(postgres_error)?, "event sequence")?,
        created_at_ms: i64_to_u64(row.try_get(4).map_err(postgres_error)?, "event time")?,
    })
}

fn row_to_snapshot(row: &Row) -> session::SessionResult<SessionSnapshot> {
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
) -> session::SessionResult<String> {
    let mut value: serde_json::Value =
        serde_json::from_str(&event.event_json).map_err(|error| {
            session::SessionError::Store(format!("decode allocated session event JSON: {error}"))
        })?;
    if let Some(object) = value.as_object_mut() {
        object.insert("sequence".to_string(), serde_json::Value::from(sequence));
    }
    serde_json::to_string(&value).map_err(|error| {
        session::SessionError::Store(format!("encode allocated session event JSON: {error}"))
    })
}

fn context_envelope_id(event_json: &str) -> session::SessionResult<String> {
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
            session::SessionError::Store(
                "ContextEnvelope append requires envelope.id or envelope_id".to_string(),
            )
        })
}

fn checkpoint_from_event(event: &SessionEvent) -> Option<String> {
    if event.event_type != session::SESSION_DOMAIN_EVENT_TYPE {
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

fn storage_error(error: storage::StorageError) -> session::SessionError {
    match error {
        storage::StorageError::Postgres(error) => postgres_error(error),
        other => session::SessionError::Store(other.to_string()),
    }
}

fn postgres_error(error: postgres::Error) -> session::SessionError {
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
    session::SessionError::Store(detail)
}

fn migration_export_error(table: &str, error: session::SessionError) -> session::SessionError {
    session::SessionError::Store(format!("export PostgreSQL `{table}` snapshot: {error}"))
}

fn to_i64(value: usize, label: &str) -> session::SessionResult<i64> {
    i64::try_from(value).map_err(|_| session::SessionError::Store(format!("{label} overflow")))
}

fn from_i64(value: i64, label: &str) -> session::SessionResult<usize> {
    usize::try_from(value).map_err(|_| session::SessionError::Store(format!("{label} overflow")))
}

fn to_u64_i64(value: u64, label: &str) -> session::SessionResult<i64> {
    i64::try_from(value).map_err(|_| session::SessionError::Store(format!("{label} overflow")))
}

fn i64_to_u64(value: i64, label: &str) -> session::SessionResult<u64> {
    u64::try_from(value).map_err(|_| session::SessionError::Store(format!("{label} overflow")))
}

fn i64_to_u32(value: i64, label: &str) -> session::SessionResult<u32> {
    u32::try_from(value).map_err(|_| session::SessionError::Store(format!("{label} overflow")))
}

// Keep this explicit rather than using partial/default methods: adding a new
// Session operation fails compilation until PostgreSQL has a real owner.
#[allow(clippy::too_many_arguments)]
impl session::SessionStoreBackend for PostgresSessionStore {
    fn create_session(&self, v: &SessionRecord) -> session::SessionResult<()> {
        self.create_session(v)
    }
    fn get_session(&self, v: &str) -> session::SessionResult<Option<SessionRecord>> {
        self.get_session(v)
    }
    fn get_sessions_by_ids(
        &self,
        session_ids: &[String],
    ) -> session::SessionResult<Vec<SessionRecord>> {
        self.get_sessions_by_ids(session_ids)
    }
    fn get_session_recovery_manifest(
        &self,
        v: &str,
    ) -> session::SessionResult<Option<SessionRecoveryManifest>> {
        self.get_session_recovery_manifest(v)
    }
    fn get_session_presence_projection(
        &self,
        session_id: &str,
    ) -> session::SessionResult<Option<SessionPresenceProjection>> {
        self.get_session_presence_projection(session_id)
    }
    fn upsert_session_presence_projection(
        &self,
        projection: &SessionPresenceProjection,
    ) -> session::SessionResult<()> {
        self.upsert_session_presence_projection(projection)
    }
    fn compare_and_upsert_session_presence_projection(
        &self,
        projection: &SessionPresenceProjection,
        expected_revision: Option<u64>,
    ) -> session::SessionResult<bool> {
        self.compare_and_upsert_session_presence_projection(projection, expected_revision)
    }
    fn delete_session_presence_projection(&self, session_id: &str) -> session::SessionResult<()> {
        self.delete_session_presence_projection(session_id)
    }
    fn get_session_recovery_manifests_by_ids(
        &self,
        session_ids: &[String],
    ) -> session::SessionResult<Vec<SessionRecoveryManifest>> {
        self.get_session_recovery_manifests_by_ids(session_ids)
    }
    fn rebuild_session_recovery_manifest(
        &self,
        session_id: &str,
        now_ms: u64,
    ) -> session::SessionResult<Option<SessionRecoveryManifest>> {
        self.rebuild_session_recovery_manifest(session_id, now_ms)
    }
    fn list_active_session_recovery_manifests(
        &self,
        offset: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionRecoveryManifest>> {
        self.list_active_session_recovery_manifests(offset, limit)
    }
    fn list_required_session_recovery_manifests(
        &self,
        offset: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionRecoveryManifest>> {
        self.list_required_session_recovery_manifests(offset, limit)
    }
    fn set_session_recovery_signal(
        &self,
        session_id: &str,
        signal: SessionRecoverySignal,
        active: bool,
        observed_at_ms: u64,
    ) -> session::SessionResult<SessionRecoveryManifest> {
        self.set_session_recovery_signal(session_id, signal, active, observed_at_ms)
    }
    fn update_session(&self, v: &SessionRecord) -> session::SessionResult<()> {
        self.update_session(v)
    }
    fn upsert_session(&self, v: &SessionRecord) -> session::SessionResult<()> {
        self.upsert_session(v)
    }
    fn upsert_session_with_mission_outbox(
        &self,
        v: &SessionRecord,
        r: &SessionMissionOutboxRequest,
    ) -> session::SessionResult<SessionMissionOutboxRecord> {
        self.upsert_session_with_mission_outbox(v, r)
    }
    fn plan_session_lifecycle(
        &self,
        plan: &SessionLifecyclePlan,
    ) -> session::SessionResult<SessionLifecycleIntent> {
        self.plan_session_lifecycle(plan)
    }
    fn get_session_lifecycle_intent(
        &self,
        operation_id: &str,
    ) -> session::SessionResult<Option<SessionLifecycleIntent>> {
        self.get_session_lifecycle_intent(operation_id)
    }
    fn list_recoverable_session_lifecycle_intents(
        &self,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionLifecycleIntent>> {
        self.list_recoverable_session_lifecycle_intents(limit)
    }
    fn fence_session_lifecycle(
        &self,
        request: &SessionLifecycleFenceRequest,
    ) -> session::SessionResult<SessionLifecycleIntent> {
        self.fence_session_lifecycle(request)
    }
    fn transition_session_lifecycle(
        &self,
        transition: &SessionLifecycleTransition,
    ) -> session::SessionResult<SessionLifecycleIntent> {
        self.transition_session_lifecycle(transition)
    }
    fn commit_session_lifecycle_tombstone(
        &self,
        request: &SessionLifecycleTombstoneRequest,
    ) -> session::SessionResult<SessionLifecycleIntent> {
        self.commit_session_lifecycle_tombstone(request)
    }
    fn delete_session(&self, v: &str) -> session::SessionResult<()> {
        self.delete_session(v)
    }
    fn delete_session_with_mission_outbox(
        &self,
        r: &SessionMissionOutboxRequest,
    ) -> session::SessionResult<bool> {
        self.delete_session_with_mission_outbox(r)
    }
    fn mark_session_closed(&self, v: &str) -> session::SessionResult<()> {
        self.mark_session_closed(v)
    }
    fn list_sessions(&self) -> session::SessionResult<Vec<SessionRecord>> {
        self.list_sessions()
    }
    fn list_sessions_page(
        &self,
        v: &SessionListOptions<'_>,
    ) -> session::SessionResult<SessionListPage> {
        self.list_sessions_page(v)
    }
    fn session_usage_summary(
        &self,
        recent_limit: usize,
    ) -> session::SessionResult<SessionUsageSummary> {
        self.session_usage_summary(recent_limit)
    }
    fn discover_browsable_sessions(
        &self,
        current_session_id: &str,
        query: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> session::SessionResult<SessionListPage> {
        self.discover_browsable_sessions(current_session_id, query, limit, offset)
    }
    fn list_sessions_by_platform(&self, v: &str) -> session::SessionResult<Vec<SessionRecord>> {
        self.list_sessions_by_platform(v)
    }
    fn list_sessions_by_workspace_root(
        &self,
        v: &str,
    ) -> session::SessionResult<Vec<SessionRecord>> {
        self.list_sessions_by_workspace_root(v)
    }
    fn search_sessions(
        &self,
        q: &str,
        l: usize,
    ) -> session::SessionResult<Vec<SessionSearchResult>> {
        self.search_sessions(q, None, l)
    }
    fn search_sessions_by_platform(
        &self,
        q: &str,
        p: &str,
        l: usize,
    ) -> session::SessionResult<Vec<SessionSearchResult>> {
        self.search_sessions(q, Some(p), l)
    }
    fn associate_memory(&self, a: &str, b: &str) -> session::SessionResult<()> {
        self.associate_memory(a, b)
    }
    fn get_session_memories(&self, a: &str) -> session::SessionResult<Vec<String>> {
        self.get_session_memories(a)
    }
    fn disassociate_memory(&self, a: &str, b: &str) -> session::SessionResult<()> {
        self.disassociate_memory(a, b)
    }
    fn append_event(&self, v: &SessionEvent) -> session::SessionResult<()> {
        self.append_event(v)
    }
    fn append_event_allocating_sequence(
        &self,
        v: &SessionEvent,
    ) -> session::SessionResult<SessionEvent> {
        self.append_event_allocating_sequence(v)
    }
    fn append_session_domain_event_if_absent_allocating_sequence(
        &self,
        event: &SessionEvent,
        event_id: &str,
    ) -> session::SessionResult<(SessionEvent, bool)> {
        self.append_session_domain_event_if_absent_allocating_sequence(event, event_id)
    }
    fn get_session_domain_event_by_id(
        &self,
        session_id: &str,
        event_id: &str,
    ) -> session::SessionResult<Option<SessionEvent>> {
        self.get_session_domain_event_by_id(session_id, event_id)
    }
    fn append_events_allocating_sequence(
        &self,
        v: &[SessionEvent],
    ) -> session::SessionResult<Vec<SessionEvent>> {
        self.append_events_allocating_sequence(v)
    }
    fn append_events_allocating_sequence_if_checkpoint_absent(
        &self,
        v: &[SessionEvent],
        c: &str,
    ) -> session::SessionResult<Option<Vec<SessionEvent>>> {
        self.append_events_allocating_sequence_if_checkpoint_absent(v, c)
    }
    fn append_context_envelope_event_if_absent(
        &self,
        v: &SessionEvent,
    ) -> session::SessionResult<bool> {
        self.append_context_envelope_event_if_absent(v)
    }
    fn append_context_envelope_event_if_absent_allocating_sequence(
        &self,
        v: &SessionEvent,
    ) -> session::SessionResult<Option<SessionEvent>> {
        self.append_context_envelope_event_if_absent_allocating_sequence(v)
    }
    fn get_events(&self, a: &str, b: usize) -> session::SessionResult<Vec<SessionEvent>> {
        self.get_events(a, b)
    }
    fn get_events_limited(
        &self,
        a: &str,
        b: usize,
        c: usize,
    ) -> session::SessionResult<Vec<SessionEvent>> {
        self.get_events_limited(a, b, c)
    }
    fn get_session_domain_timeline_limited(
        &self,
        a: &str,
        b: usize,
        c: usize,
    ) -> session::SessionResult<Vec<SessionEvent>> {
        self.get_session_domain_timeline_limited(a, b, c)
    }
    fn count_session_domain_timeline_from(
        &self,
        a: &str,
        b: usize,
    ) -> session::SessionResult<usize> {
        self.count_session_domain_timeline_from(a, b)
    }
    fn get_session_domain_events_by_kind_limited(
        &self,
        session_id: &str,
        kind: &str,
        from_seq: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionEvent>> {
        self.get_session_domain_events_by_kind_limited(session_id, kind, from_seq, limit)
    }
    fn get_latest_session_domain_event_by_kind(
        &self,
        session_id: &str,
        kind: &str,
    ) -> session::SessionResult<Option<SessionEvent>> {
        self.get_latest_session_domain_event_by_kind(session_id, kind)
    }
    fn count_session_domain_events_by_kind_from(
        &self,
        session_id: &str,
        kind: &str,
        from_seq: usize,
    ) -> session::SessionResult<usize> {
        self.count_session_domain_events_by_kind_from(session_id, kind, from_seq)
    }
    fn has_session_domain_event_kind(&self, kind: &str) -> session::SessionResult<bool> {
        self.has_session_domain_event_kind(kind)
    }
    fn has_session_with_domain_event_kinds(
        &self,
        kinds: &[String],
    ) -> session::SessionResult<bool> {
        self.has_session_with_domain_event_kinds(kinds)
    }
    fn get_events_by_type_limited(
        &self,
        a: &str,
        b: &str,
        c: usize,
        d: usize,
    ) -> session::SessionResult<Vec<SessionEvent>> {
        self.get_events_by_type_limited(a, b, c, d)
    }
    fn count_events_from(&self, a: &str, b: usize) -> session::SessionResult<usize> {
        self.count_events_from(a, b)
    }
    fn count_events_by_type_from(
        &self,
        a: &str,
        b: &str,
        c: usize,
    ) -> session::SessionResult<usize> {
        self.count_events_by_type_from(a, b, c)
    }
    fn get_context_event_by_envelope_id(
        &self,
        a: &str,
    ) -> session::SessionResult<Option<SessionEvent>> {
        self.get_context_event_by_envelope_id(a)
    }
    fn next_event_sequence(&self, a: &str) -> session::SessionResult<usize> {
        self.next_event_sequence(a)
    }
    fn delete_events_from(&self, a: &str, b: usize) -> session::SessionResult<usize> {
        self.delete_events_from(a, b)
    }
    fn delete_events_by_type_from(
        &self,
        a: &str,
        b: &str,
        c: usize,
    ) -> session::SessionResult<usize> {
        self.delete_events_by_type_from(a, b, c)
    }
    fn save_snapshot(&self, v: &SessionSnapshot) -> session::SessionResult<()> {
        self.save_snapshot(v)
    }
    fn get_latest_snapshot(&self, a: &str) -> session::SessionResult<Option<SessionSnapshot>> {
        self.get_latest_snapshot(a)
    }
    fn prune_before(&self, a: &str) -> session::SessionResult<usize> {
        self.prune_before(a)
    }
    fn insert_message(&self, v: &SessionMessage) -> session::SessionResult<()> {
        self.insert_message(v)
    }
    fn commit_terminal_transcript_if_fenced(
        &self,
        request: &SessionTerminalTranscriptCommit,
    ) -> session::SessionResult<SessionTerminalTranscriptReceipt> {
        self.commit_terminal_transcript_if_fenced(request)
    }
    fn insert_messages_batch(&self, a: &[SessionMessage]) -> session::SessionResult<()> {
        self.insert_messages_batch(a)
    }
    fn copy_session_messages_at_cutoff(
        &self,
        a: &str,
        b: &str,
        c: usize,
    ) -> session::SessionResult<usize> {
        self.copy_session_messages_at_cutoff(a, b, c)
    }
    fn branch_session_at_cutoff(
        &self,
        request: &SessionBranchRequest,
    ) -> session::SessionResult<SessionBranchResult> {
        self.branch_session_at_cutoff(request)
    }
    fn get_session_branch_activation(
        &self,
        operation_id: &str,
    ) -> session::SessionResult<Option<SessionBranchActivation>> {
        self.get_session_branch_activation(operation_id)
    }
    fn list_recoverable_session_branch_activations(
        &self,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionBranchActivation>> {
        self.list_recoverable_session_branch_activations(limit)
    }
    fn transition_session_branch_activation(
        &self,
        transition: &SessionBranchActivationTransition,
    ) -> session::SessionResult<SessionBranchActivation> {
        self.transition_session_branch_activation(transition)
    }
    fn append_message_with_runtime_outbox(
        &self,
        a: &SessionMessage,
        b: &SessionRuntimeOutboxRequest,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        self.append_message_with_runtime_outbox(a, b)
    }
    fn append_ingress_with_runtime_outbox(
        &self,
        a: &str,
        b: &str,
        c: Option<&str>,
        d: u64,
        e: &SessionRuntimeOutboxRequest,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        self.append_ingress_with_runtime_outbox(a, b, c, d, e)
    }
    fn claim_session_runtime_outbox(
        &self,
        a: &str,
        b: u64,
        c: u64,
        d: usize,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        self.claim_session_runtime_outbox(a, b, c, d)
    }
    fn ack_session_runtime_outbox(
        &self,
        a: &str,
        b: &str,
        c: u64,
        d: &str,
        e: u64,
        f: SessionRuntimeInputStatus,
        g: u64,
        h: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        self.ack_session_runtime_outbox(a, b, c, d, e, f, g, h)
    }
    fn mark_session_runtime_outbox_running(
        &self,
        a: &str,
        b: &str,
        c: u64,
        d: &str,
        e: u64,
        f: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        self.mark_session_runtime_outbox_running(a, b, c, d, e, f)
    }
    fn renew_session_runtime_outbox_lease(
        &self,
        a: &str,
        b: &str,
        c: u64,
        d: &str,
        e: u64,
        f: u64,
        g: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        self.renew_session_runtime_outbox_lease(a, b, c, d, e, f, g)
    }
    fn fail_session_runtime_outbox(
        &self,
        a: &str,
        b: &str,
        c: u64,
        d: &str,
        e: u64,
        f: OutboxFailureClass,
        g: &str,
        h: u64,
        i: u32,
        j: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        self.fail_session_runtime_outbox(a, b, c, d, e, f, g, h, i, j)
    }
    fn requeue_claimed_session_runtime_outbox(
        &self,
        a: &str,
        b: &str,
        c: u64,
        d: &str,
        e: u64,
        f: InputRoutingDecision,
        g: Option<&str>,
        h: Option<&str>,
        i: &str,
        j: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        self.requeue_claimed_session_runtime_outbox(a, b, c, d, e, f, g, h, i, j)
    }
    fn retry_blocked_session_runtime_outbox(
        &self,
        a: &str,
        b: u64,
        c: u64,
        d: &str,
        e: &str,
        f: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        self.retry_blocked_session_runtime_outbox(a, b, c, d, e, f)
    }
    fn cancel_session_runtime_outbox(
        &self,
        a: &str,
        b: u64,
        c: u64,
        d: &str,
        e: &str,
        f: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        self.cancel_session_runtime_outbox(a, b, c, d, e, f)
    }
    fn reclassify_session_runtime_outbox(
        &self,
        a: &str,
        b: u64,
        c: u64,
        d: InputRoutingDecision,
        e: Option<&str>,
        f: Option<&str>,
        g: &str,
        h: &str,
        i: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        self.reclassify_session_runtime_outbox(a, b, c, d, e, f, g, h, i)
    }
    fn get_session_input_admission(
        &self,
        a: &str,
    ) -> session::SessionResult<Option<SessionInputAdmission>> {
        self.get_session_input_admission(a)
    }
    fn close_session_input_admission(
        &self,
        a: &str,
        b: u64,
        c: &str,
        d: &str,
        e: u64,
    ) -> session::SessionResult<SessionInputAdmission> {
        self.close_session_input_admission(a, b, c, d, e)
    }
    fn advance_session_input_generation(
        &self,
        a: &str,
        b: u64,
        c: bool,
        d: &str,
        e: &str,
        f: u64,
    ) -> session::SessionResult<SessionInputAdmission> {
        self.advance_session_input_generation(a, b, c, d, e, f)
    }
    fn get_session_runtime_outbox(
        &self,
        a: &str,
    ) -> session::SessionResult<Option<SessionRuntimeOutboxRecord>> {
        self.get_session_runtime_outbox(a)
    }
    fn get_session_runtime_outbox_by_input_id(
        &self,
        a: &str,
    ) -> session::SessionResult<Option<SessionRuntimeOutboxRecord>> {
        self.get_session_runtime_outbox_by_input_id(a)
    }
    fn session_runtime_outbox_for_session(
        &self,
        a: &str,
        b: usize,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        self.session_runtime_outbox_for_session(a, b)
    }
    fn session_runtime_outbox_for_sessions(
        &self,
        a: &[String],
        b: usize,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        self.session_runtime_outbox_for_sessions(a, b)
    }
    fn active_session_runtime_outbox(
        &self,
        a: usize,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        self.active_session_runtime_outbox(a)
    }
    fn session_runtime_outbox_health(&self) -> session::SessionResult<SessionRuntimeOutboxHealth> {
        self.session_runtime_outbox_health()
    }
    fn blocked_session_runtime_outbox(
        &self,
        a: usize,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        self.blocked_session_runtime_outbox(a)
    }
    fn claim_session_mission_outbox(
        &self,
        a: &str,
        b: &str,
        c: u64,
        d: u64,
        e: usize,
    ) -> session::SessionResult<Vec<SessionMissionOutboxRecord>> {
        self.claim_session_mission_outbox(a, b, c, d, e)
    }
    fn ack_session_mission_outbox(
        &self,
        a: &str,
        b: &str,
        c: u64,
        d: u64,
    ) -> session::SessionResult<SessionMissionOutboxRecord> {
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
    ) -> session::SessionResult<SessionMissionOutboxRecord> {
        self.fail_session_mission_outbox(a, b, c, d, e, f, g, h)
    }
    fn get_session_mission_outbox(
        &self,
        a: &str,
    ) -> session::SessionResult<Option<SessionMissionOutboxRecord>> {
        self.get_session_mission_outbox(a)
    }
    fn get_messages(
        &self,
        a: &str,
        b: usize,
        c: usize,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        self.get_messages(a, b, c)
    }
    fn get_messages_from_sequence(
        &self,
        a: &str,
        b: usize,
        c: usize,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        self.get_messages_from_sequence(a, b, c)
    }
    fn get_messages_in_ranges(
        &self,
        session_id: &str,
        ranges: &[(usize, usize)],
        limit: usize,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        self.get_messages_in_ranges(session_id, ranges, limit)
    }
    fn get_message_by_stable_id(
        &self,
        session_id: &str,
        stable_message_id: &str,
    ) -> session::SessionResult<Option<SessionMessage>> {
        self.get_message_by_stable_id(session_id, stable_message_id)
    }
    fn get_message_by_sequence(
        &self,
        session_id: &str,
        sequence: usize,
    ) -> session::SessionResult<Option<SessionMessage>> {
        self.get_message_by_sequence(session_id, sequence)
    }
    fn get_message_metadata_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionMessageMetadata>> {
        self.get_message_metadata_page(session_id, from_sequence, limit)
    }
    fn get_context_index_cards(
        &self,
        session_id: &str,
        limit: usize,
    ) -> session::SessionResult<Vec<ContextIndexCard>> {
        self.get_context_index_cards(session_id, limit)
    }
    fn reconcile_session_context_index(
        &self,
        session_id: &str,
        card_span: usize,
        parent_span: usize,
        now_ms: u64,
    ) -> session::SessionResult<ContextIndexCoverage> {
        self.reconcile_session_context_index(session_id, card_span, parent_span, now_ms)
    }
    fn get_all_messages(&self, a: &str) -> session::SessionResult<Vec<SessionMessage>> {
        self.get_all_messages(a)
    }
    fn get_message_count(&self, a: &str) -> session::SessionResult<usize> {
        self.get_message_count(a)
    }
    fn delete_messages_from(&self, a: &str, b: usize) -> session::SessionResult<usize> {
        self.delete_messages_from(a, b)
    }
    fn search_messages(
        &self,
        a: &str,
        b: Option<&str>,
        c: usize,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        self.search_messages(a, b, c)
    }
    fn search_messages_in_sessions(
        &self,
        a: &str,
        b: &[String],
        c: usize,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        self.search_messages_in_sessions(a, b, c)
    }
    fn search_messages_visible(
        &self,
        a: &str,
        b: Option<&str>,
        c: &[String],
        d: bool,
        e: usize,
    ) -> session::SessionResult<Vec<SessionMessage>> {
        self.search_messages_visible(a, b, c, d, e)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier, Mutex, MutexGuard, OnceLock};

    use session::{SessionStoreBackend, UnifiedSessionStore};
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

    fn clear_isolated_store(store: &PostgresSessionStore) {
        let mut connection = store
            .executor
            .checkout_background()
            .expect("isolated PostgreSQL test connection");
        connection
            .batch_execute(
                "TRUNCATE TABLE
                    session_branch_activations,
                    session_lifecycle_intents,
                    session_presence_projection,
                    session_runtime_outbox_history,
                    session_mission_outbox_history,
                    session_runtime_outbox,
                    session_mission_outbox,
                    session_event_checkpoints,
                    session_snapshots,
                    session_events,
                    session_messages,
                    session_memory_associations,
                    session_recovery_manifest,
                    session_records
                 CASCADE",
            )
            .expect("clear isolated PostgreSQL Session store");
    }

    fn unique_id(prefix: &str) -> String {
        format!(
            "{prefix}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        )
    }

    fn runtime_request(
        id: &str,
        generation: u64,
        decision: InputRoutingDecision,
        target_turn_id: Option<&str>,
        created_at_ms: u64,
    ) -> SessionRuntimeOutboxRequest {
        SessionRuntimeOutboxRequest {
            input_id: format!("input-{id}"),
            request_id: format!("request-{id}"),
            turn_id: format!("turn-{id}"),
            message_id: format!("message-{id}"),
            session_generation: generation,
            decision,
            target_turn_id: target_turn_id.map(str::to_string),
            classification_json: Some(
                serde_json::json!({"classifier":"test.v1","reason":"contract"}).to_string(),
            ),
            created_at_ms,
            runtime_options_json: Some(r#"{"profile":"test"}"#.to_string()),
        }
    }

    fn append_runtime_input(
        store: &PostgresSessionStore,
        session_id: &str,
        request: &SessionRuntimeOutboxRequest,
    ) -> SessionRuntimeOutboxRecord {
        store
            .append_ingress_with_runtime_outbox(
                session_id,
                "user",
                Some(r#"[{"type":"text","text":"test input"}]"#),
                request.created_at_ms,
                request,
            )
            .expect("append durable runtime input")
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn postgres_reads_selected_context_ranges_with_one_query() {
        let _guard = postgres_test_guard();
        let store = real_store();
        clear_isolated_store(&store);
        let session_id = unique_id("context-ranges");
        store
            .create_session(&session(&session_id))
            .expect("create Session");
        let messages = (0..24)
            .map(|sequence| SessionMessage {
                stable_message_id: format!("{session_id}:message:{sequence}"),
                session_id: session_id.clone(),
                sequence,
                role: "user".to_string(),
                content_json: serde_json::json!([
                    {"type":"text","text":format!("message {sequence}")}
                ])
                .to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: sequence as u64,
            })
            .collect::<Vec<_>>();
        store
            .insert_messages_batch(&messages)
            .expect("insert messages");

        let selected = store
            .get_messages_in_ranges(&session_id, &[(2, 5), (12, 15)], 32)
            .expect("read exact selected ranges");
        assert_eq!(
            selected
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3, 4, 12, 13, 14]
        );
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn postgres_activation_index_and_manifest_repair_match_sqlite_semantics() {
        let _guard = postgres_test_guard();
        let store = real_store();
        clear_isolated_store(&store);
        let session_id = unique_id("activation-index");
        store
            .create_session(&session(&session_id))
            .expect("create Session");
        let messages = (0..300)
            .map(|sequence| SessionMessage {
                stable_message_id: format!("{session_id}:message:{sequence}"),
                session_id: session_id.clone(),
                sequence,
                role: if sequence % 2 == 0 {
                    "user"
                } else {
                    "assistant"
                }
                .to_string(),
                content_json: serde_json::json!([
                    {"type":"text","text":format!("message {sequence}")}
                ])
                .to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: sequence as u64,
            })
            .collect::<Vec<_>>();
        store
            .insert_messages_batch(&messages)
            .expect("insert messages");
        store
            .append_event(&SessionEvent {
                session_id: session_id.clone(),
                event_type: session::SESSION_DOMAIN_EVENT_TYPE.to_string(),
                event_json: serde_json::json!({
                    "event_id": "pg-checkpoint",
                    "session_id": session_id,
                    "sequence": 0,
                    "scope": "runtime",
                    "kind": "memory.semantic_checkpoint.created",
                    "payload": {},
                    "created_at_ms": 500
                })
                .to_string(),
                sequence: 0,
                created_at_ms: 500,
            })
            .expect("append checkpoint");

        assert_eq!(
            store
                .get_message_by_stable_id(&session_id, &format!("{session_id}:message:299"))
                .expect("exact message")
                .expect("message exists")
                .sequence,
            299
        );
        assert_eq!(
            store
                .get_message_metadata_page(&session_id, 298, 8)
                .expect("metadata")
                .len(),
            2
        );
        assert_eq!(
            store
                .get_latest_session_domain_event_by_kind(
                    &session_id,
                    "memory.semantic_checkpoint.created",
                )
                .expect("latest checkpoint")
                .expect("checkpoint exists")
                .sequence,
            0
        );
        let coverage = store
            .reconcile_session_context_index(&session_id, 128, 4, 600)
            .expect("reconcile context index");
        assert!(coverage.complete);
        assert_eq!(coverage.covered_messages, 300);

        let mut connection = store.executor.checkout_critical().expect("connection");
        connection
            .execute(
                "DELETE FROM session_recovery_manifest WHERE session_id=$1",
                &[&session_id],
            )
            .expect("remove manifest");
        drop(connection);
        let rebuilt = store
            .rebuild_session_recovery_manifest(&session_id, 700)
            .expect("rebuild manifest")
            .expect("manifest exists");
        assert_eq!(rebuilt.transcript_messages, 300);
        assert_eq!(
            rebuilt.latest_checkpoint_event_id.as_deref(),
            Some("pg-checkpoint")
        );
        assert!(rebuilt.index_pending);
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn existing_postgres_outbox_schema_migrates_claim_fence_epoch_in_place() {
        let _guard = postgres_test_guard();
        let store = real_store();
        clear_isolated_store(&store);
        store
            .create_session(&session("claim-fence-migration"))
            .expect("create migration Session");
        let request = runtime_request(
            "claim-fence-migration",
            1,
            InputRoutingDecision::StartNewTurn,
            None,
            100,
        );
        append_runtime_input(&store, "claim-fence-migration", &request);
        let claimed = store
            .claim_session_runtime_outbox("migration-worker", 101, 1_000, 1)
            .expect("claim pre-migration input")
            .remove(0);
        let token = claimed.claim_token.clone().expect("pre-migration token");
        let running = store
            .mark_session_runtime_outbox_running(
                &request.request_id,
                "migration-worker",
                1,
                &token,
                claimed.revision,
                102,
            )
            .expect("mark pre-migration input running");
        let expected_epoch = running.revision;

        let mut connection = store
            .executor()
            .checkout_critical()
            .expect("checkout migration database");
        connection
            .batch_execute(
                "ALTER TABLE session_runtime_outbox
                     DROP CONSTRAINT IF EXISTS session_runtime_claim_fence_epoch_positive;
                 ALTER TABLE session_runtime_outbox DROP COLUMN claim_fence_epoch;
                 DELETE FROM cowd_schema_migrations
                  WHERE id='session.0010.terminal-claim-fence-epoch';",
            )
            .expect("restore the version-9 outbox schema");
        drop(connection);
        drop(store);

        let migrated = real_store()
            .get_session_runtime_outbox(&request.request_id)
            .expect("read migrated input")
            .expect("migrated input remains");
        assert_eq!(migrated.status, SessionRuntimeInputStatus::Running);
        assert_eq!(migrated.claim_fence_epoch, Some(expected_epoch));
    }

    #[test]
    fn sqlite_snapshot_contains_full_session_truth_and_is_stable() {
        let source = SqliteSessionStore::open_in_memory().expect("SQLite source opens");
        source
            .create_session(&session("migration-session"))
            .expect("session");
        source
            .upsert_session_with_mission_outbox(
                &session("migration-session"),
                &SessionMissionOutboxRequest {
                    request_id: "mission-copy".to_string(),
                    session_id: "migration-session".to_string(),
                    title: "Migrate a non-empty mission outbox".to_string(),
                    workspace_key: "workspace-copy".to_string(),
                    operation: SessionMissionOutboxOperation::Start,
                    created_at_ms: 7,
                },
            )
            .expect("mission outbox");
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
                event_type: session::SESSION_DOMAIN_EVENT_TYPE.to_string(),
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
        source
            .plan_session_lifecycle(&SessionLifecyclePlan {
                operation_id: "lifecycle-copy".to_string(),
                session_id: "migration-session".to_string(),
                disposition: SessionCloseDisposition::Archive,
                expected_generation: 1,
                created_at_ms: 5,
            })
            .expect("lifecycle intent");
        source
            .branch_session_at_cutoff(&SessionBranchRequest {
                operation_id: "branch-copy".to_string(),
                source_session_id: "migration-session".to_string(),
                source_message_count: 1,
                target: session("migration-branch"),
                mission_outbox: SessionMissionOutboxRequest {
                    request_id: "mission-branch-copy".to_string(),
                    session_id: "migration-branch".to_string(),
                    title: "Migrate branch activation".to_string(),
                    workspace_key: "workspace-copy".to_string(),
                    operation: SessionMissionOutboxOperation::Register,
                    created_at_ms: 6,
                },
                source_event_json: r#"{"kind":"session.branch.source"}"#.to_string(),
                target_event_json: r#"{"kind":"session.branch.target"}"#.to_string(),
                created_at_ms: 6,
            })
            .expect("branch activation");
        let first = export_sqlite_session_snapshot(&source).expect("first snapshot");
        let second = export_sqlite_session_snapshot(&source).expect("second snapshot");
        assert_eq!(
            first.canonical_digest().unwrap(),
            second.canonical_digest().unwrap()
        );
        assert_eq!(first.schema_version, 5);
        assert_eq!(first.sessions.len(), 2);
        assert_eq!(first.messages.len(), 2);
        assert_eq!(first.events.len(), 4);
        assert_eq!(first.checkpoints.len(), 1);
        assert_eq!(first.snapshots.len(), 1);
        assert_eq!(first.mission_outbox.len(), 2);
        assert_eq!(first.lifecycle_intents.len(), 1);
        assert_eq!(first.branch_activations.len(), 1);
    }

    #[tokio::test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    async fn postgres_adapter_real_copy_fences_and_injected_facade() {
        let _guard = postgres_test_guard();
        let target = real_store();
        clear_isolated_store(&target);
        let source = SqliteSessionStore::open_in_memory().expect("SQLite source opens");
        source
            .create_session(&session("migration-session"))
            .expect("session");
        source
            .upsert_session_with_mission_outbox(
                &session("migration-session"),
                &SessionMissionOutboxRequest {
                    request_id: "mission-copy".to_string(),
                    session_id: "migration-session".to_string(),
                    title: "Migrate a non-empty mission outbox".to_string(),
                    workspace_key: "workspace-copy".to_string(),
                    operation: SessionMissionOutboxOperation::Start,
                    created_at_ms: 7,
                },
            )
            .expect("mission outbox");
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
        source
            .plan_session_lifecycle(&SessionLifecyclePlan {
                operation_id: "lifecycle-copy".to_string(),
                session_id: "migration-session".to_string(),
                disposition: SessionCloseDisposition::Archive,
                expected_generation: 1,
                created_at_ms: 2,
            })
            .expect("lifecycle intent");
        source
            .branch_session_at_cutoff(&SessionBranchRequest {
                operation_id: "branch-copy".to_string(),
                source_session_id: "migration-session".to_string(),
                source_message_count: 1,
                target: session("migration-branch"),
                mission_outbox: SessionMissionOutboxRequest {
                    request_id: "mission-branch-copy".to_string(),
                    session_id: "migration-branch".to_string(),
                    title: "Migrate branch activation".to_string(),
                    workspace_key: "workspace-copy".to_string(),
                    operation: SessionMissionOutboxOperation::Register,
                    created_at_ms: 3,
                },
                source_event_json: r#"{"kind":"session.branch.source"}"#.to_string(),
                target_event_json: r#"{"kind":"session.branch.target"}"#.to_string(),
                created_at_ms: 3,
            })
            .expect("branch activation");
        let root = tempfile::tempdir().expect("manifest root");
        let manifest =
            copy_quiesced_session_store(&source, &target, root.path().join("session.json"))
                .expect("copy");
        assert_eq!(manifest.source_digest, manifest.target_digest);
        let copied = target
            .export_migration_snapshot()
            .expect("export copied PostgreSQL snapshot");
        assert_eq!(copied.mission_outbox.len(), 2);
        assert!(copied
            .mission_outbox
            .iter()
            .any(|item| item.request_id == "mission-copy" && item.revision == 0));
        assert_eq!(copied.lifecycle_intents.len(), 1);
        assert_eq!(copied.branch_activations.len(), 1);
        let initial_source_events = copied
            .events
            .iter()
            .filter(|event| event.session_id == "migration-session")
            .count();
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
        assert_eq!(
            sequences,
            vec![initial_source_events, initial_source_events + 1]
        );
        target
            .delete_session("migration-session")
            .expect("delete isolated migration session");
        target
            .delete_session("migration-branch")
            .expect("delete isolated migration branch");
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn postgres_fenced_terminal_commit_matches_sqlite_atomic_identity_contract() {
        let _guard = postgres_test_guard();
        let store = real_store();
        let session_id = unique_id("terminal-fence");
        let id = unique_id("terminal-input");
        store
            .create_session(&session(&session_id))
            .expect("create fenced terminal session");
        let request = runtime_request(&id, 1, InputRoutingDecision::StartNewTurn, None, 100);
        append_runtime_input(&store, &session_id, &request);
        let claimed = store
            .claim_session_runtime_outbox("runtime-worker", 101, 1_000, 1)
            .expect("claim input")
            .remove(0);
        let token = claimed.claim_token.clone().expect("claim token");
        let running = store
            .mark_session_runtime_outbox_running(
                &request.request_id,
                "runtime-worker",
                1,
                &token,
                claimed.revision,
                102,
            )
            .expect("mark running");
        let renewed = store
            .renew_session_runtime_outbox_lease(
                &request.request_id,
                "runtime-worker",
                1,
                &token,
                running.revision,
                103,
                1_000,
            )
            .expect("renew running lease");
        assert!(renewed.revision > running.revision);
        let messages = vec![
            SessionMessage {
                stable_message_id: format!("tool-{id}"),
                session_id: session_id.clone(),
                sequence: 0,
                role: "tool".to_string(),
                content_json: r#"[{"type":"text","text":"evidence"}]"#.to_string(),
                blocks_count: 1,
                tool_use_id: Some(format!("tool-use-{id}")),
                tool_name: Some("read".to_string()),
                token_usage_json: None,
                created_at_ms: 0,
            },
            SessionMessage {
                stable_message_id: format!("tool-secondary-{id}"),
                session_id: session_id.clone(),
                sequence: 0,
                role: "tool".to_string(),
                content_json: r#"[{"type":"text","text":"secondary evidence"}]"#.to_string(),
                blocks_count: 1,
                tool_use_id: Some(format!("tool-use-secondary-{id}")),
                tool_name: Some("read".to_string()),
                token_usage_json: None,
                created_at_ms: 0,
            },
            SessionMessage {
                stable_message_id: format!("assistant-{id}"),
                session_id: session_id.clone(),
                sequence: 0,
                role: "assistant".to_string(),
                content_json: r#"[{"type":"text","text":"done"}]"#.to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: Some(r#"{"output_tokens":1}"#.to_string()),
                created_at_ms: 0,
            },
        ];
        let commit = SessionTerminalTranscriptCommit {
            terminal_message_id: format!("assistant-{id}"),
            ingress_message_id: request.message_id.clone(),
            session_id: session_id.clone(),
            turn_id: request.turn_id.clone(),
            messages,
            runtime_commit_cursor: 42,
            consumed_input_sequence: running.sequence,
            created_at_ms: 104,
            fence: session::SessionTerminalExecutionFence {
                request_id: request.request_id.clone(),
                input_sequence: running.sequence,
                session_generation: 1,
                claim_owner: "runtime-worker".to_string(),
                claim_token: token,
                claim_fence_epoch: running
                    .claim_fence_epoch
                    .expect("running input owns an immutable claim fence"),
            },
        };
        let mut wrong_sequence = commit.clone();
        wrong_sequence.fence.input_sequence = wrong_sequence.fence.input_sequence.saturating_add(1);
        wrong_sequence.consumed_input_sequence = wrong_sequence.fence.input_sequence;
        assert!(matches!(
            store.commit_terminal_transcript_if_fenced(&wrong_sequence),
            Err(session::SessionError::StaleExecutionFence(_))
        ));
        let receipt = store
            .commit_terminal_transcript_if_fenced(&commit)
            .expect("commit with renewed live fence");
        assert!(receipt.inserted);
        assert_eq!(receipt.input.status, SessionRuntimeInputStatus::Completed);
        assert_eq!(receipt.input.revision, renewed.revision + 1);
        assert_eq!(store.get_message_count(&session_id).unwrap(), 4);
        let replay = store
            .commit_terminal_transcript_if_fenced(&commit)
            .expect("exact replay");
        assert!(!replay.inserted);
        assert_eq!(replay.messages, receipt.messages);

        let mut reordered_intermediate = commit.clone();
        reordered_intermediate.messages.swap(0, 1);
        assert!(matches!(
            store.commit_terminal_transcript_if_fenced(&reordered_intermediate),
            Err(session::SessionError::StaleExecutionFence(_))
        ));
        assert_eq!(store.get_message_count(&session_id).unwrap(), 4);

        let mut conflicting = commit.clone();
        conflicting.terminal_message_id = format!("assistant-conflict-{id}");
        conflicting
            .messages
            .last_mut()
            .expect("terminal row")
            .stable_message_id = conflicting.terminal_message_id.clone();
        assert!(matches!(
            store.commit_terminal_transcript_if_fenced(&conflicting),
            Err(session::SessionError::StaleExecutionFence(_))
        ));
        assert_eq!(store.get_message_count(&session_id).unwrap(), 4);
        store
            .delete_session(&session_id)
            .expect("delete fenced terminal session");

        let stale_session = unique_id("terminal-stale");
        let stale_id = unique_id("terminal-stale-input");
        store
            .create_session(&session(&stale_session))
            .expect("create stale terminal session");
        let stale_request =
            runtime_request(&stale_id, 1, InputRoutingDecision::StartNewTurn, None, 200);
        append_runtime_input(&store, &stale_session, &stale_request);
        let stale_claim = store
            .claim_session_runtime_outbox("runtime-worker-old", 201, 50, 1)
            .expect("claim stale input")
            .remove(0);
        let stale_token = stale_claim.claim_token.clone().expect("stale token");
        let stale_running = store
            .mark_session_runtime_outbox_running(
                &stale_request.request_id,
                "runtime-worker-old",
                1,
                &stale_token,
                stale_claim.revision,
                202,
            )
            .expect("mark stale input running");
        let stale_commit = SessionTerminalTranscriptCommit {
            terminal_message_id: format!("assistant-{stale_id}"),
            ingress_message_id: stale_request.message_id.clone(),
            session_id: stale_session.clone(),
            turn_id: stale_request.turn_id.clone(),
            messages: vec![SessionMessage {
                stable_message_id: format!("assistant-{stale_id}"),
                session_id: stale_session.clone(),
                sequence: 0,
                role: "assistant".to_string(),
                content_json: r#"[{"type":"text","text":"must not commit"}]"#.to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: 0,
            }],
            runtime_commit_cursor: 43,
            consumed_input_sequence: stale_running.sequence,
            created_at_ms: 250,
            fence: session::SessionTerminalExecutionFence {
                request_id: stale_request.request_id.clone(),
                input_sequence: stale_running.sequence,
                session_generation: 1,
                claim_owner: "runtime-worker-old".to_string(),
                claim_token: stale_token.clone(),
                claim_fence_epoch: stale_running
                    .claim_fence_epoch
                    .expect("running input owns an immutable claim fence"),
            },
        };
        let reclaimed = store
            .claim_session_runtime_outbox("runtime-worker-new", 251, 1_000, 1)
            .expect("reclaim expired input")
            .remove(0);
        assert_ne!(reclaimed.claim_token.as_deref(), Some(stale_token.as_str()));
        assert!(matches!(
            store.commit_terminal_transcript_if_fenced(&stale_commit),
            Err(session::SessionError::StaleExecutionFence(_))
        ));
        assert_eq!(store.get_message_count(&stale_session).unwrap(), 1);
        store
            .advance_session_input_generation(
                &stale_session,
                1,
                true,
                "test",
                "replace stale generation",
                252,
            )
            .expect("advance stale generation");
        assert!(matches!(
            store.commit_terminal_transcript_if_fenced(&stale_commit),
            Err(session::SessionError::StaleExecutionFence(_))
        ));
        assert_eq!(store.get_message_count(&stale_session).unwrap(), 1);
        store
            .delete_session(&stale_session)
            .expect("delete stale terminal session");
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn postgres_terminal_commit_and_generation_advance_share_one_lock_order() {
        let _guard = postgres_test_guard();
        let store = real_store();
        let session_id = unique_id("terminal-lock-order");
        let id = unique_id("terminal-lock-input");
        store
            .create_session(&session(&session_id))
            .expect("create lock-order session");
        let request = runtime_request(&id, 1, InputRoutingDecision::StartNewTurn, None, 100);
        append_runtime_input(&store, &session_id, &request);
        let claimed = store
            .claim_session_runtime_outbox("runtime-worker", 101, 1_000, 1)
            .expect("claim lock-order input")
            .remove(0);
        let token = claimed.claim_token.clone().expect("claim token");
        let running = store
            .mark_session_runtime_outbox_running(
                &request.request_id,
                "runtime-worker",
                1,
                &token,
                claimed.revision,
                102,
            )
            .expect("mark lock-order input running");
        let commit = SessionTerminalTranscriptCommit {
            terminal_message_id: format!("assistant-{id}"),
            ingress_message_id: request.message_id.clone(),
            session_id: session_id.clone(),
            turn_id: request.turn_id.clone(),
            messages: vec![SessionMessage {
                stable_message_id: format!("assistant-{id}"),
                session_id: session_id.clone(),
                sequence: 0,
                role: "assistant".to_string(),
                content_json: r#"[{"type":"text","text":"lock order"}]"#.to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: 0,
            }],
            runtime_commit_cursor: 44,
            consumed_input_sequence: running.sequence,
            created_at_ms: 103,
            fence: session::SessionTerminalExecutionFence {
                request_id: request.request_id,
                input_sequence: running.sequence,
                session_generation: 1,
                claim_owner: "runtime-worker".to_string(),
                claim_token: token,
                claim_fence_epoch: running
                    .claim_fence_epoch
                    .expect("running input owns an immutable claim fence"),
            },
        };
        let barrier = Arc::new(Barrier::new(2));
        let commit_worker = {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                store.commit_terminal_transcript_if_fenced(&commit)
            })
        };
        let generation_worker = {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            let session_id = session_id.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store.advance_session_input_generation(
                    &session_id,
                    1,
                    true,
                    "lock-order-test",
                    "race terminal commit",
                    104,
                )
            })
        };
        let commit_result = commit_worker.join().expect("commit worker");
        let generation = generation_worker
            .join()
            .expect("generation worker")
            .expect("generation advance cannot deadlock");
        assert_eq!(generation.generation, 2);
        let messages = store
            .get_message_count(&session_id)
            .expect("count lock-order transcript");
        match commit_result {
            Ok(receipt) => {
                assert!(receipt.inserted);
                assert_eq!(messages, 2);
            }
            Err(session::SessionError::StaleExecutionFence(_)) => {
                assert_eq!(messages, 1);
            }
            Err(error) => panic!("unexpected terminal commit result: {error}"),
        }
        store
            .delete_session(&session_id)
            .expect("delete lock-order session");
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn postgres_branch_command_commits_every_artifact_or_nothing() {
        let _guard = postgres_test_guard();
        let store = real_store();
        let source = unique_id("branch-command-source");
        let target = unique_id("branch-command-target");
        let rollback_target = unique_id("branch-command-rollback");
        store
            .create_session(&session(&source))
            .expect("create branch source");
        for sequence in 0..2 {
            store
                .insert_message(&SessionMessage {
                    stable_message_id: format!("{source}-message-{sequence}"),
                    session_id: source.clone(),
                    sequence,
                    role: "user".to_string(),
                    content_json: format!(
                        r#"[{{"type":"text","text":"source message {sequence}"}}]"#
                    ),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: Some(r#"{"input_tokens":3,"output_tokens":2}"#.to_string()),
                    created_at_ms: 100 + sequence as u64,
                })
                .expect("append source message");
        }
        let source_generation = store
            .get_session_input_admission(&source)
            .expect("read source admission")
            .expect("source admission exists")
            .generation;
        let request = SessionBranchRequest {
            operation_id: format!("branch-{source}-{target}-1"),
            source_session_id: source.clone(),
            source_message_count: 1,
            target: session(&target),
            mission_outbox: SessionMissionOutboxRequest {
                request_id: format!("mission-{target}"),
                session_id: target.clone(),
                title: "Create durable branch".to_string(),
                workspace_key: "branch-workspace".to_string(),
                operation: SessionMissionOutboxOperation::Register,
                created_at_ms: 200,
            },
            source_event_json: serde_json::json!({
                "kind": "session.branched",
                "target_session_id": target.clone(),
                "source_message_count": 1
            })
            .to_string(),
            target_event_json: serde_json::json!({
                "kind": "session.branch_created",
                "source_session_id": source.clone(),
                "source_message_count": 1
            })
            .to_string(),
            created_at_ms: 200,
        };
        let result = store
            .branch_session_at_cutoff(&request)
            .expect("branch command commits");
        assert_eq!(result.copied_message_count, 1);
        assert_eq!(result.source_message_count, 1);
        assert_eq!(result.target.message_count, 1);
        assert_eq!(
            result.activation.phase,
            SessionBranchActivationPhase::BranchCommitted
        );
        let replay = store
            .branch_session_at_cutoff(&request)
            .expect("committed branch retry resumes activation receipt");
        assert_eq!(replay.target.session_id, target);
        assert_eq!(replay.copied_message_count, 1);
        let persisted_target = store
            .get_session(&target)
            .expect("read target")
            .expect("target exists");
        assert_eq!(persisted_target.message_count, 1);
        assert_eq!(persisted_target.input_tokens, 3);
        assert_eq!(persisted_target.output_tokens, 2);
        let copied = store
            .get_all_messages(&target)
            .expect("read branch messages");
        assert_eq!(copied.len(), 1);
        assert_eq!(copied[0].sequence, 0);
        assert!(copied[0]
            .stable_message_id
            .starts_with(&format!("branch:{target}:")));
        assert!(store
            .get_session_mission_outbox(&request.mission_outbox.request_id)
            .expect("read branch mission")
            .is_some());
        let source_events = store
            .get_events_limited(&source, 0, 10)
            .expect("read source branch event");
        assert_eq!(
            source_events
                .iter()
                .filter(|event| event.event_type == "SessionBranched")
                .count(),
            1
        );
        let target_events = store
            .get_events_limited(&target, 0, 10)
            .expect("read target branch event");
        assert_eq!(
            target_events
                .iter()
                .filter(|event| event.event_type == "BranchCreated")
                .count(),
            1
        );
        assert_eq!(
            store
                .get_session_input_admission(&source)
                .expect("read source admission after branch")
                .expect("source admission remains")
                .generation,
            source_generation,
            "branch command must not advance source generation"
        );
        let activation_pending = store
            .transition_session_branch_activation(&SessionBranchActivationTransition {
                operation_id: request.operation_id.clone(),
                expected_revision: result.activation.revision,
                expected_phase: SessionBranchActivationPhase::BranchCommitted,
                next_phase: SessionBranchActivationPhase::ActivationPending,
                updated_at_ms: 204,
                error: None,
            })
            .expect("fence Gateway activation after branch commit");
        let failed = store
            .transition_session_branch_activation(&SessionBranchActivationTransition {
                operation_id: request.operation_id.clone(),
                expected_revision: activation_pending.revision,
                expected_phase: SessionBranchActivationPhase::ActivationPending,
                next_phase: SessionBranchActivationPhase::Failed,
                updated_at_ms: 205,
                error: Some("simulated activation failure".to_string()),
            })
            .expect("persist branch activation failure");
        assert!(store
            .list_recoverable_session_branch_activations(10)
            .expect("list recoverable branch activations")
            .iter()
            .any(|activation| activation.operation_id == request.operation_id));
        let pending = store
            .transition_session_branch_activation(&SessionBranchActivationTransition {
                operation_id: request.operation_id.clone(),
                expected_revision: failed.revision,
                expected_phase: SessionBranchActivationPhase::Failed,
                next_phase: SessionBranchActivationPhase::ActivationPending,
                updated_at_ms: 206,
                error: None,
            })
            .expect("resume branch activation");
        let activated = store
            .transition_session_branch_activation(&SessionBranchActivationTransition {
                operation_id: request.operation_id.clone(),
                expected_revision: pending.revision,
                expected_phase: SessionBranchActivationPhase::ActivationPending,
                next_phase: SessionBranchActivationPhase::Activated,
                updated_at_ms: 207,
                error: None,
            })
            .expect("complete branch activation");
        assert_eq!(activated.phase, SessionBranchActivationPhase::Activated);

        let source_event_count = source_events.len();
        let rollback_request = SessionBranchRequest {
            operation_id: format!("branch-{source}-{rollback_target}-2"),
            source_session_id: source.clone(),
            source_message_count: 2,
            target: session(&rollback_target),
            mission_outbox: SessionMissionOutboxRequest {
                request_id: format!("mission-{rollback_target}"),
                session_id: rollback_target.clone(),
                title: "Rollback invalid branch".to_string(),
                workspace_key: "branch-workspace".to_string(),
                operation: SessionMissionOutboxOperation::Register,
                created_at_ms: 201,
            },
            source_event_json: r#"{"kind":"session.branched"}"#.to_string(),
            target_event_json: "{invalid-json".to_string(),
            created_at_ms: 201,
        };
        assert!(store.branch_session_at_cutoff(&rollback_request).is_err());
        assert!(store
            .get_session(&rollback_target)
            .expect("read rolled back target")
            .is_none());
        assert!(store
            .get_session_mission_outbox(&rollback_request.mission_outbox.request_id)
            .expect("read rolled back mission")
            .is_none());
        assert_eq!(
            store
                .get_events_limited(&source, 0, 10)
                .expect("read source events after rollback")
                .len(),
            source_event_count
        );

        store.delete_session(&target).expect("delete branch target");
        store.delete_session(&source).expect("delete branch source");
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn postgres_presence_projection_is_mutable_and_does_not_append_history() {
        let _guard = postgres_test_guard();
        let session_id = unique_id("presence-projection");
        let store = real_store();
        store
            .create_session(&session(&session_id))
            .expect("create presence Session");
        let attachment = session::SessionAttachment {
            session_id: session_id.clone(),
            actor: session::SessionActor {
                id: "web-1".to_string(),
                surface: "webui".to_string(),
                role: Some("reader".to_string()),
            },
            attached_at_ms: 100,
            last_seen_ms: 100,
        };
        store
            .upsert_session_presence_projection(&SessionPresenceProjection {
                session_id: session_id.clone(),
                state: "attached".to_string(),
                attachments_json: serde_json::to_string(&vec![attachment]).unwrap(),
                next_sequence: 1,
                revision: 1,
                updated_at_ms: 100,
            })
            .expect("insert presence projection");
        assert!(
            store
                .get_session_recovery_manifest(&session_id)
                .expect("presence recovery manifest")
                .expect("presence recovery row")
                .active_writer_or_attachment
        );

        store
            .upsert_session_presence_projection(&SessionPresenceProjection {
                session_id: session_id.clone(),
                state: "detached".to_string(),
                attachments_json: "[]".to_string(),
                next_sequence: 1,
                revision: 2,
                updated_at_ms: 200,
            })
            .expect("expire presence projection");
        assert!(
            !store
                .get_session_recovery_manifest(&session_id)
                .expect("expired recovery manifest")
                .expect("expired recovery row")
                .active_writer_or_attachment
        );
        assert!(
            store
                .get_events_limited(&session_id, 0, 10)
                .expect("presence Session history")
                .is_empty(),
            "mutable presence must not append immutable Session events"
        );
        store
            .delete_session(&session_id)
            .expect("delete presence Session");
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn postgres_lifecycle_intent_recovers_each_phase_and_commits_one_tombstone() {
        let _guard = postgres_test_guard();
        let session_id = unique_id("lifecycle-recovery");
        let operation_id = format!("session-lifecycle:archive:{session_id}");
        let mission_id = format!("mission:lifecycle-close:{session_id}");
        let store = real_store();
        store
            .create_session(&session(&session_id))
            .expect("create lifecycle session");
        let input = runtime_request(
            &format!("{session_id}-input"),
            1,
            InputRoutingDecision::StartNewTurn,
            None,
            100,
        );
        append_runtime_input(&store, &session_id, &input);
        let planned = store
            .plan_session_lifecycle(&SessionLifecyclePlan {
                operation_id: operation_id.clone(),
                session_id: session_id.clone(),
                disposition: session::SessionCloseDisposition::Archive,
                expected_generation: 1,
                created_at_ms: 110,
            })
            .expect("plan lifecycle");
        assert_eq!(planned.phase, SessionLifecyclePhase::Planned);
        drop(store);

        let store = real_store();
        let fenced = store
            .fence_session_lifecycle(&SessionLifecycleFenceRequest {
                transition: SessionLifecycleTransition {
                    operation_id: operation_id.clone(),
                    expected_revision: planned.revision,
                    expected_phase: SessionLifecyclePhase::Planned,
                    next_phase: SessionLifecyclePhase::AdmissionFenced,
                    updated_at_ms: 120,
                    error: None,
                },
                actor: "postgres-contract".to_string(),
                reason: "archive".to_string(),
                transitional_status: "archiving".to_string(),
                event: SessionEvent {
                    session_id: session_id.clone(),
                    event_type: "session.archive_started".to_string(),
                    event_json: r#"{"kind":"session.archive_started"}"#.to_string(),
                    sequence: 0,
                    created_at_ms: 120,
                },
            })
            .expect("fence lifecycle");
        assert_eq!(fenced.phase, SessionLifecyclePhase::AdmissionFenced);
        assert!(store
            .session_runtime_outbox_for_session(&session_id, 10)
            .unwrap()
            .iter()
            .all(|input| input.status == SessionRuntimeInputStatus::Expired));
        let failed = store
            .transition_session_lifecycle(&SessionLifecycleTransition {
                operation_id: operation_id.clone(),
                expected_revision: fenced.revision,
                expected_phase: SessionLifecyclePhase::AdmissionFenced,
                next_phase: SessionLifecyclePhase::Failed,
                updated_at_ms: 121,
                error: Some("simulated worker crash".to_string()),
            })
            .expect("persist lifecycle failure");
        drop(store);

        let store = real_store();
        assert!(store
            .list_recoverable_session_lifecycle_intents(10)
            .unwrap()
            .iter()
            .any(|intent| intent.operation_id == operation_id));
        let resumed = store
            .transition_session_lifecycle(&SessionLifecycleTransition {
                operation_id: operation_id.clone(),
                expected_revision: failed.revision,
                expected_phase: SessionLifecyclePhase::Failed,
                next_phase: SessionLifecyclePhase::AdmissionFenced,
                updated_at_ms: 130,
                error: None,
            })
            .expect("resume lifecycle");
        let drained = store
            .transition_session_lifecycle(&SessionLifecycleTransition {
                operation_id: operation_id.clone(),
                expected_revision: resumed.revision,
                expected_phase: SessionLifecyclePhase::AdmissionFenced,
                next_phase: SessionLifecyclePhase::RuntimeDrained,
                updated_at_ms: 140,
                error: None,
            })
            .expect("mark Runtime drained");
        drop(store);

        let store = real_store();
        let mut record = store
            .get_session(&session_id)
            .unwrap()
            .expect("lifecycle Session");
        record.status = "archived".to_string();
        record.last_activity = "2026-07-26T00:00:01Z".to_string();
        record.metadata_json = Some(r#"{"tombstone":{"kind":"archived"}}"#.to_string());
        let tombstone = SessionLifecycleTombstoneRequest {
            transition: SessionLifecycleTransition {
                operation_id: operation_id.clone(),
                expected_revision: drained.revision,
                expected_phase: SessionLifecyclePhase::RuntimeDrained,
                next_phase: SessionLifecyclePhase::TombstoneCommitted,
                updated_at_ms: 150,
                error: None,
            },
            record,
            mission_outbox: SessionMissionOutboxRequest {
                request_id: mission_id.clone(),
                session_id: session_id.clone(),
                title: "Lifecycle recovery".to_string(),
                workspace_key: "postgres-contract".to_string(),
                operation: SessionMissionOutboxOperation::Close,
                created_at_ms: 150,
            },
            event: SessionEvent {
                session_id: session_id.clone(),
                event_type: "session.archived".to_string(),
                event_json: r#"{"kind":"session.archived"}"#.to_string(),
                sequence: 0,
                created_at_ms: 150,
            },
        };
        let committed = store
            .commit_session_lifecycle_tombstone(&tombstone)
            .expect("commit lifecycle tombstone");
        assert_eq!(committed.phase, SessionLifecyclePhase::TombstoneCommitted);
        assert!(store
            .get_session_mission_outbox(&mission_id)
            .unwrap()
            .is_some());
        drop(store);

        let store = real_store();
        assert_eq!(
            store
                .get_events_limited(&session_id, 0, 100)
                .unwrap()
                .iter()
                .filter(|event| event.event_type == "session.archived")
                .count(),
            1
        );
        let unloaded = store
            .transition_session_lifecycle(&SessionLifecycleTransition {
                operation_id: operation_id.clone(),
                expected_revision: committed.revision,
                expected_phase: SessionLifecyclePhase::TombstoneCommitted,
                next_phase: SessionLifecyclePhase::Unloaded,
                updated_at_ms: 160,
                error: None,
            })
            .expect("mark lifecycle unloaded");
        assert_eq!(unloaded.phase, SessionLifecyclePhase::Unloaded);
        assert!(store
            .commit_session_lifecycle_tombstone(&tombstone)
            .is_err());
        assert_eq!(
            store
                .get_events_limited(&session_id, 0, 100)
                .unwrap()
                .iter()
                .filter(|event| event.event_type == "session.archived")
                .count(),
            1
        );
        store
            .delete_session(&session_id)
            .expect("cleanup lifecycle Session");
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn postgres_delete_lifecycle_recovers_stable_phases_and_commits_one_tombstone() {
        let _guard = postgres_test_guard();
        let session_id = unique_id("delete-lifecycle-recovery");
        let operation_id = format!("session-lifecycle:delete:{session_id}");
        let mission_id = format!("mission:delete-lifecycle:{session_id}");
        let store = real_store();
        store
            .create_session(&session(&session_id))
            .expect("create delete lifecycle Session");
        let planned = store
            .plan_session_lifecycle(&SessionLifecyclePlan {
                operation_id: operation_id.clone(),
                session_id: session_id.clone(),
                disposition: session::SessionCloseDisposition::Delete,
                expected_generation: 1,
                created_at_ms: 100,
            })
            .expect("plan delete lifecycle");
        drop(store);

        let store = real_store();
        let fenced = store
            .fence_session_lifecycle(&SessionLifecycleFenceRequest {
                transition: SessionLifecycleTransition {
                    operation_id: operation_id.clone(),
                    expected_revision: planned.revision,
                    expected_phase: SessionLifecyclePhase::Planned,
                    next_phase: SessionLifecyclePhase::AdmissionFenced,
                    updated_at_ms: 110,
                    error: None,
                },
                actor: "postgres-contract".to_string(),
                reason: "delete".to_string(),
                transitional_status: "deleting".to_string(),
                event: SessionEvent {
                    session_id: session_id.clone(),
                    event_type: "session.delete_started".to_string(),
                    event_json: r#"{"kind":"session.delete_started"}"#.to_string(),
                    sequence: 0,
                    created_at_ms: 110,
                },
            })
            .expect("fence delete lifecycle");
        drop(store);

        let store = real_store();
        let drained = store
            .transition_session_lifecycle(&SessionLifecycleTransition {
                operation_id: operation_id.clone(),
                expected_revision: fenced.revision,
                expected_phase: SessionLifecyclePhase::AdmissionFenced,
                next_phase: SessionLifecyclePhase::RuntimeDrained,
                updated_at_ms: 120,
                error: None,
            })
            .expect("mark delete Runtime drained");
        drop(store);

        let store = real_store();
        let mut record = store
            .get_session(&session_id)
            .unwrap()
            .expect("delete lifecycle Session");
        record.status = "deleted".to_string();
        record.last_activity = "2026-07-26T00:00:01Z".to_string();
        record.metadata_json = Some(r#"{"tombstone":{"kind":"deleted"}}"#.to_string());
        let request = SessionLifecycleTombstoneRequest {
            transition: SessionLifecycleTransition {
                operation_id: operation_id.clone(),
                expected_revision: drained.revision,
                expected_phase: SessionLifecyclePhase::RuntimeDrained,
                next_phase: SessionLifecyclePhase::TombstoneCommitted,
                updated_at_ms: 130,
                error: None,
            },
            record,
            mission_outbox: SessionMissionOutboxRequest {
                request_id: mission_id,
                session_id: session_id.clone(),
                title: "Delete lifecycle recovery".to_string(),
                workspace_key: "postgres-contract".to_string(),
                operation: SessionMissionOutboxOperation::Close,
                created_at_ms: 130,
            },
            event: SessionEvent {
                session_id: session_id.clone(),
                event_type: "session.deleted".to_string(),
                event_json: r#"{"kind":"session.deleted"}"#.to_string(),
                sequence: 0,
                created_at_ms: 130,
            },
        };
        let committed = store
            .commit_session_lifecycle_tombstone(&request)
            .expect("commit delete lifecycle tombstone");
        drop(store);

        let store = real_store();
        assert_eq!(
            store
                .get_events_limited(&session_id, 0, 100)
                .unwrap()
                .iter()
                .filter(|event| event.event_type == "session.deleted")
                .count(),
            1
        );
        let unloaded = store
            .transition_session_lifecycle(&SessionLifecycleTransition {
                operation_id: operation_id,
                expected_revision: committed.revision,
                expected_phase: SessionLifecyclePhase::TombstoneCommitted,
                next_phase: SessionLifecyclePhase::Unloaded,
                updated_at_ms: 140,
                error: None,
            })
            .expect("unload deleted Session");
        assert_eq!(unloaded.phase, SessionLifecyclePhase::Unloaded);
        assert!(store.commit_session_lifecycle_tombstone(&request).is_err());
        assert_eq!(
            store
                .get_events_limited(&session_id, 0, 100)
                .unwrap()
                .iter()
                .filter(|event| event.event_type == "session.deleted")
                .count(),
            1
        );
        store
            .delete_session(&session_id)
            .expect("cleanup deleted Session");
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn postgres_durable_input_contract_is_fenced_ordered_and_auditable() {
        let _guard = postgres_test_guard();
        let store = real_store();
        let source = unique_id("durable-source");
        let peer = unique_id("durable-peer");
        let branch = unique_id("durable-branch");
        let rejected = unique_id("durable-rejected");
        for id in [&source, &peer, &branch, &rejected] {
            store
                .create_session(&session(id))
                .expect("create isolated session");
        }

        let source_first = runtime_request(
            &format!("{source}-1"),
            1,
            InputRoutingDecision::StartNewTurn,
            None,
            100,
        );
        let source_second = runtime_request(
            &format!("{source}-2"),
            1,
            InputRoutingDecision::EnqueueNextStep,
            None,
            101,
        );
        let peer_first = runtime_request(
            &format!("{peer}-1"),
            1,
            InputRoutingDecision::StartNewTurn,
            None,
            102,
        );
        let wrong_generation = runtime_request(
            &format!("{source}-wrong-generation"),
            2,
            InputRoutingDecision::StartNewTurn,
            None,
            99,
        );
        assert!(store
            .append_ingress_with_runtime_outbox(
                &source,
                "user",
                Some(r#"[{"type":"text","text":"must roll back"}]"#),
                99,
                &wrong_generation,
            )
            .is_err());
        assert_eq!(
            store.get_message_count(&source).expect("message count"),
            0,
            "rejected generation must not leave a transcript row"
        );
        let first = append_runtime_input(&store, &source, &source_first);
        let second = append_runtime_input(&store, &source, &source_second);
        let peer_record = append_runtime_input(&store, &peer, &peer_first);
        let rejected_duplicate = append_runtime_input(
            &store,
            &rejected,
            &runtime_request(
                &format!("{rejected}-duplicate"),
                1,
                InputRoutingDecision::RejectDuplicate,
                None,
                103,
            ),
        );
        let rejected_policy = append_runtime_input(
            &store,
            &rejected,
            &runtime_request(
                &format!("{rejected}-policy"),
                1,
                InputRoutingDecision::RejectPolicy,
                None,
                104,
            ),
        );
        assert_eq!(
            rejected_duplicate.status,
            SessionRuntimeInputStatus::RejectedDuplicate
        );
        assert_eq!(
            rejected_policy.status,
            SessionRuntimeInputStatus::RejectedPolicy
        );
        assert_eq!(rejected_duplicate.terminal_at_ms, Some(103));
        assert_eq!(rejected_policy.terminal_at_ms, Some(104));
        assert_eq!(first.status, SessionRuntimeInputStatus::Queued);
        assert_eq!(first.revision, 2);
        assert_eq!(second.sequence, 1);
        assert_eq!(
            store
                .get_session_runtime_outbox_by_input_id(&source_first.input_id)
                .expect("lookup input")
                .expect("input exists"),
            first
        );
        assert_eq!(
            store
                .get_session_domain_timeline_limited(&source, 0, 20)
                .expect("input timeline")
                .iter()
                .filter(|event| event.event_json.contains("session.input."))
                .count(),
            6,
            "accepted, classified and queued must be atomic timeline evidence"
        );

        let claimed = store
            .claim_session_runtime_outbox("worker-a", 200, 1_000, 10)
            .expect("claim session heads");
        assert_eq!(claimed.len(), 2, "one head per Session may be claimed");
        assert!(
            claimed.iter().all(|item| item.session_id != rejected),
            "terminal policy decisions must never enter the runnable claim set"
        );
        assert!(claimed.iter().any(|item| item.input_id == first.input_id));
        assert!(claimed
            .iter()
            .any(|item| item.input_id == peer_record.input_id));
        assert!(
            !claimed.iter().any(|item| item.input_id == second.input_id),
            "same-Session second input must remain behind the active head"
        );

        let first_claim = claimed
            .iter()
            .find(|item| item.input_id == first.input_id)
            .expect("source head claimed");
        let token = first_claim
            .claim_token
            .as_deref()
            .expect("claim token issued");
        let running = store
            .mark_session_runtime_outbox_running(
                &first_claim.request_id,
                "worker-a",
                1,
                token,
                first_claim.revision,
                201,
            )
            .expect("mark running");
        assert!(store
            .ack_session_runtime_outbox(
                &running.request_id,
                "worker-a",
                1,
                "stale-token",
                running.revision,
                SessionRuntimeInputStatus::Completed,
                1,
                202,
            )
            .is_err());
        let renewed = store
            .renew_session_runtime_outbox_lease(
                &running.request_id,
                "worker-a",
                1,
                token,
                running.revision,
                202,
                1_000,
            )
            .expect("renew running lease");
        let completed = store
            .ack_session_runtime_outbox(
                &renewed.request_id,
                "worker-a",
                1,
                token,
                renewed.revision,
                SessionRuntimeInputStatus::Completed,
                7,
                203,
            )
            .expect("ack completed input");
        assert_eq!(completed.status, SessionRuntimeInputStatus::Completed);
        assert_eq!(completed.runtime_commit_cursor, Some(7));
        assert_eq!(completed.terminal_at_ms, Some(203));

        let next = store
            .claim_session_runtime_outbox("worker-b", 204, 1_000, 10)
            .expect("claim released Session head");
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].input_id, second.input_id);
        let requeued = store
            .requeue_claimed_session_runtime_outbox(
                &next[0].request_id,
                "worker-b",
                1,
                next[0].claim_token.as_deref().expect("claim token"),
                next[0].revision,
                InputRoutingDecision::StartNewTurn,
                None,
                Some(r#"{"classifier":"target-lost.v1"}"#),
                "target turn is no longer active",
                205,
            )
            .expect("owner-fenced requeue");
        assert_eq!(requeued.status, SessionRuntimeInputStatus::Reclassified);
        assert_eq!(requeued.decision, InputRoutingDecision::StartNewTurn);
        let reclaimed = store
            .claim_session_runtime_outbox("worker-c", 206, 1_000, 10)
            .expect("reclaim reclassified input");
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].input_id, second.input_id);

        let peer_claim = claimed
            .iter()
            .find(|item| item.input_id == peer_record.input_id)
            .expect("peer head claimed");
        let cancelled = store
            .cancel_session_runtime_outbox(
                &peer_claim.input_id,
                1,
                peer_claim.revision,
                "operator",
                "cancel peer test input",
                207,
            )
            .expect("cancel by input id");
        assert_eq!(cancelled.status, SessionRuntimeInputStatus::Cancelled);
        let peer_second = runtime_request(
            &format!("{peer}-2"),
            1,
            InputRoutingDecision::EnqueueNextStep,
            None,
            208,
        );
        let peer_second = append_runtime_input(&store, &peer, &peer_second);
        let peer_second = store
            .reclassify_session_runtime_outbox(
                &peer_second.input_id,
                1,
                peer_second.revision,
                InputRoutingDecision::StartNewTurn,
                None,
                Some(r#"{"classifier":"operator.v1"}"#),
                "operator",
                "explicit reroute",
                209,
            )
            .expect("reclassify queued input");
        assert_eq!(peer_second.status, SessionRuntimeInputStatus::Reclassified);

        let generation_before_branch = store
            .get_session_input_admission(&source)
            .expect("source admission")
            .expect("source exists")
            .generation;
        assert_eq!(
            store
                .copy_session_messages_at_cutoff(&source, &branch, 1)
                .expect("copy immutable branch cutoff"),
            1
        );
        let branch_messages = store.get_all_messages(&branch).expect("branch messages");
        assert_eq!(branch_messages.len(), 1);
        assert_eq!(branch_messages[0].sequence, 0);
        assert!(branch_messages[0]
            .stable_message_id
            .starts_with(&format!("branch:{branch}:")));
        assert!(store
            .copy_session_messages_at_cutoff(&source, &branch, 1)
            .is_err());
        assert_eq!(
            store
                .get_session_input_admission(&source)
                .expect("source admission")
                .expect("source exists")
                .generation,
            generation_before_branch,
            "branch copy must never advance source generation"
        );

        let closed = store
            .close_session_input_admission(
                &source,
                generation_before_branch,
                "operator",
                "close test source",
                210,
            )
            .expect("close admission and expire owned work");
        assert!(!closed.open);
        assert_eq!(closed.generation, generation_before_branch + 1);
        let expired = store
            .get_session_runtime_outbox(&reclaimed[0].request_id)
            .expect("expired lookup")
            .expect("expired input exists");
        assert_eq!(expired.status, SessionRuntimeInputStatus::Expired);
        assert!(store
            .mark_session_runtime_outbox_running(
                &reclaimed[0].request_id,
                "worker-c",
                generation_before_branch,
                reclaimed[0].claim_token.as_deref().expect("claim token"),
                reclaimed[0].revision,
                211,
            )
            .is_err());
        let health = store
            .session_runtime_outbox_health()
            .expect("runtime input health");
        assert!(health.completed >= 1);
        assert!(health.rejected_duplicate >= 1);
        assert!(health.rejected_policy >= 1);
        assert!(health.cancelled >= 1);
        assert!(health.expired >= 1);
        assert!(health.reclassified >= 1);

        for id in [&source, &peer, &branch, &rejected] {
            store.delete_session(id).expect("delete isolated session");
        }
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn postgres_runtime_failure_retry_and_terminal_statuses_are_real() {
        let _guard = postgres_test_guard();
        let store = real_store();
        let session_id = unique_id("durable-failure");
        store
            .create_session(&session(&session_id))
            .expect("create isolated session");
        let request = runtime_request(&session_id, 1, InputRoutingDecision::StartNewTurn, None, 10);
        let explicit_message = SessionMessage {
            stable_message_id: request.message_id.clone(),
            session_id: session_id.clone(),
            sequence: 0,
            role: "user".to_string(),
            content_json: r#"[{"type":"text","text":"failure path"}]"#.to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: 10,
        };
        let queued = store
            .append_message_with_runtime_outbox(&explicit_message, &request)
            .expect("append explicit message and durable input atomically");
        assert_eq!(queued.status, SessionRuntimeInputStatus::Queued);
        let claimed = store
            .claim_session_runtime_outbox("failure-worker", 20, 100, 1)
            .expect("claim failure input")
            .pop()
            .expect("failure input claimed");
        let running = store
            .mark_session_runtime_outbox_running(
                &claimed.request_id,
                "failure-worker",
                1,
                claimed.claim_token.as_deref().expect("claim token"),
                claimed.revision,
                21,
            )
            .expect("mark failure input running");
        let queued = store
            .fail_session_runtime_outbox(
                &running.request_id,
                "failure-worker",
                1,
                running.claim_token.as_deref().expect("claim token"),
                running.revision,
                OutboxFailureClass::Retryable,
                "temporary dependency failure",
                30,
                3,
                22,
            )
            .expect("schedule retry");
        assert_eq!(queued.status, SessionRuntimeInputStatus::Queued);
        assert!(queued.claim_token.is_none());
        assert!(store
            .claim_session_runtime_outbox("early-worker", 29, 100, 1)
            .expect("early claim")
            .is_empty());
        let claimed = store
            .claim_session_runtime_outbox("blocked-worker", 30, 100, 1)
            .expect("claim retry")
            .pop()
            .expect("retry claimed");
        let blocked = store
            .fail_session_runtime_outbox(
                &claimed.request_id,
                "blocked-worker",
                1,
                claimed.claim_token.as_deref().expect("claim token"),
                claimed.revision,
                OutboxFailureClass::AuthorizationBlocked,
                "approval required",
                31,
                3,
                31,
            )
            .expect("block authorization failure");
        assert_eq!(blocked.status, SessionRuntimeInputStatus::Blocked);
        assert!(blocked.terminal_at_ms.is_none());
        let queued = store
            .retry_blocked_session_runtime_outbox(
                &blocked.request_id,
                1,
                blocked.revision,
                "operator",
                "approval granted",
                32,
            )
            .expect("release blocked input");
        let claimed = store
            .claim_session_runtime_outbox("permanent-worker", 33, 100, 1)
            .expect("claim released input")
            .pop()
            .expect("released input claimed");
        assert_eq!(claimed.request_id, queued.request_id);
        let failed = store
            .fail_session_runtime_outbox(
                &claimed.request_id,
                "permanent-worker",
                1,
                claimed.claim_token.as_deref().expect("claim token"),
                claimed.revision,
                OutboxFailureClass::Permanent,
                "permanent runtime failure",
                34,
                3,
                34,
            )
            .expect("record permanent failure");
        assert_eq!(failed.status, SessionRuntimeInputStatus::Failed);
        assert_eq!(failed.terminal_at_ms, Some(34));
        assert!(store
            .retry_blocked_session_runtime_outbox(
                &failed.request_id,
                1,
                failed.revision,
                "operator",
                "must not retry terminal failure",
                35,
            )
            .is_err());
        store
            .delete_session(&session_id)
            .expect("delete isolated session");
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn postgres_v8_migrates_legacy_runtime_rows_in_place() {
        let _guard = postgres_test_guard();
        let url =
            std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
        let mut client =
            postgres::Client::connect(&url, postgres::NoTls).expect("connect isolated PostgreSQL");
        let schema = unique_id("legacy_v8").replace('-', "_");
        client
            .batch_execute(&format!(
                "CREATE SCHEMA {schema}; SET search_path TO {schema};"
            ))
            .expect("create isolated migration schema");
        client
            .batch_execute(
                "CREATE TABLE session_records(
                     session_id TEXT PRIMARY KEY,
                     updated_at_ms BIGINT NOT NULL DEFAULT 0
                 );
                 CREATE TABLE session_runtime_outbox(
                     request_id TEXT PRIMARY KEY,
                     session_id TEXT NOT NULL REFERENCES session_records(session_id),
                     sequence BIGINT NOT NULL,
                     status TEXT NOT NULL,
                     next_attempt_at_ms BIGINT NOT NULL,
                     claim_expires_at_ms BIGINT,
                     updated_at_ms BIGINT NOT NULL DEFAULT 0
                 );
                 CREATE TABLE session_recovery_manifest(
                     session_id TEXT PRIMARY KEY,
                     in_flight_turn BOOLEAN NOT NULL DEFAULT FALSE,
                     manifest_revision BIGINT NOT NULL DEFAULT 0
                 );
                 CREATE OR REPLACE FUNCTION cowd_refresh_session_recovery_manifest(
                     target_session_id TEXT,bump_history BOOLEAN
                 ) RETURNS VOID LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$;
                 INSERT INTO session_records(session_id) VALUES('legacy');
                 INSERT INTO session_recovery_manifest(session_id) VALUES('legacy');
                 INSERT INTO session_runtime_outbox(
                     request_id,session_id,sequence,status,next_attempt_at_ms
                 ) VALUES
                     ('pending','legacy',0,'pending',0),
                     ('retry','legacy',1,'retry_scheduled',0),
                     ('done','legacy',2,'materialized',0),
                     ('blocked','legacy',3,'blocked_materialization',0);",
            )
            .expect("seed legacy schema");
        let migration = SESSION_MIGRATIONS
            .iter()
            .find(|migration| migration.version == 8)
            .expect("v8 migration exists");
        assert_eq!(migration.version, 8);
        for statement in migration.statements {
            client
                .batch_execute(statement)
                .unwrap_or_else(|error| panic!("v8 statement failed: {statement}: {error}"));
        }
        let admission = client
            .query_one(
                "SELECT input_generation,input_admission_open
                   FROM session_records WHERE session_id='legacy'",
                &[],
            )
            .expect("load migrated admission");
        assert_eq!(admission.get::<_, i64>(0), 1);
        assert!(admission.get::<_, bool>(1));
        let rows = client
            .query(
                "SELECT request_id,input_id,status,session_generation,decision
                   FROM session_runtime_outbox ORDER BY sequence",
                &[],
            )
            .expect("load migrated rows");
        let expected = [
            ("pending", "queued"),
            ("retry", "queued"),
            ("done", "completed"),
            ("blocked", "blocked"),
        ];
        for (row, (request_id, status)) in rows.iter().zip(expected) {
            assert_eq!(row.get::<_, String>(0), request_id);
            assert_eq!(row.get::<_, String>(1), request_id);
            assert_eq!(row.get::<_, String>(2), status);
            assert_eq!(row.get::<_, i64>(3), 1);
            assert_eq!(row.get::<_, String>(4), "start_new_turn");
        }
        client
            .batch_execute(&format!(
                "SET search_path TO public; DROP SCHEMA {schema} CASCADE;"
            ))
            .expect("drop isolated migration schema");
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
