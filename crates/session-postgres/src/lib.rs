//! PostgreSQL durable session adapter.
//!
//! The adapter is constructed only from the host-owned, bounded
//! [`storage::PostgresExecutor`]. It never accepts a path or a database URL.

use std::{collections::BTreeMap, fs, path::Path};

use harness_contract::turn::InputRoutingDecision;
use postgres::{types::ToSql, Row};
use serde::{Deserialize, Serialize};
use session::persistence::domain::{
    ingress::{
        applied_input_projection, decision_requires_target_turn, input_decision_as_str,
        parse_input_decision as parse_input_decision_value,
    },
    lifecycle::{validate_fence_metadata, validate_plan_identity},
    query::bounded_limit,
    terminal::{validate_terminal_commit, validate_terminal_transcript},
};
use session::{
    build_context_index_cards, context_index_card_digest, context_index_source_digest,
    ContextIndexCard, ContextIndexCoverage, OutboxFailureClass, SessionBranchActivation,
    SessionBranchActivationPhase, SessionBranchActivationTransition, SessionBranchRequest,
    SessionBranchResult, SessionCloseDisposition, SessionEvent, SessionInputAdmission,
    SessionLifecycleFenceRequest, SessionLifecycleIntent, SessionLifecyclePhase,
    SessionLifecyclePlan, SessionLifecycleTombstoneRequest, SessionLifecycleTransition,
    SessionListOptions, SessionListPage, SessionMessage, SessionMessageMetadata,
    SessionPresenceProjection, SessionRecord, SessionRecoveryManifest, SessionRecoverySignal,
    SessionRuntimeInputStatus, SessionRuntimeOutboxHealth, SessionRuntimeOutboxRecord,
    SessionRuntimeOutboxRequest, SessionSearchResult, SessionSnapshot,
    SessionTerminalTranscriptCommit, SessionTerminalTranscriptReceipt, SessionUsageBucket,
    SessionUsageSummary, SqliteSessionStore,
};
use session::{SessionDomainEvent, SessionDomainRef, SessionDomainScope};
use sha2::{Digest, Sha256};
use storage::{
    PostgresConnection, PostgresConnectionConfig, PostgresExecutor, PostgresMigrationSpec,
    PostgresTransaction, SecretRefResolver,
};

mod ingress;
mod lifecycle;
mod query;
mod terminal;

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
    pub runtime_history: Vec<SessionOutboxHistory>,
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
    let sessions = sqlite_rows(&connection, "SELECT session_id,platform,chat_id,user_id,model,created_at,last_activity,message_count,reset_policy,metadata_json,input_tokens,output_tokens,status FROM sessions ORDER BY session_id", sqlite_row_to_session)?;
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
    let runtime_outbox = sqlite_rows(&connection, "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,decision,target_turn_id,classification_json,task_route_hint_json,status,runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json FROM session_runtime_outbox ORDER BY request_id", sqlite_row_to_runtime_outbox)?;
    let runtime_history = sqlite_rows(&connection, "SELECT request_id,action,actor,reason,from_status,to_status,attempts,created_at_ms FROM session_runtime_outbox_history ORDER BY id", sqlite_row_to_history)?;
    Ok(SessionMigrationSnapshot {
        schema_version: 6,
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
        runtime_history,
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
        status: row.get(12)?,
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
        task_route_hint: row
            .get::<_, Option<String>>(10)?
            .map(|value| serde_json::from_str(&value).map_err(sqlite_conversion_error))
            .transpose()?,
        status: SessionRuntimeInputStatus::parse(&row.get::<_, String>(11)?)?,
        runtime_commit_cursor: row
            .get::<_, Option<i64>>(12)?
            .map(|value| u64::try_from(value).map_err(sqlite_conversion_error))
            .transpose()?,
        attempts: u32::try_from(row.get::<_, i64>(13)?).map_err(sqlite_conversion_error)?,
        next_attempt_at_ms: u64::try_from(row.get::<_, i64>(14)?)
            .map_err(sqlite_conversion_error)?,
        claim_owner: row.get(15)?,
        claim_token: row.get(16)?,
        claim_expires_at_ms: row
            .get::<_, Option<i64>>(17)?
            .map(|value| u64::try_from(value).map_err(sqlite_conversion_error))
            .transpose()?,
        failure_class: row
            .get::<_, Option<String>>(18)?
            .as_deref()
            .map(OutboxFailureClass::parse)
            .transpose()?,
        last_error: row.get(19)?,
        revision: u64::try_from(row.get::<_, i64>(20)?).map_err(sqlite_conversion_error)?,
        created_at_ms: u64::try_from(row.get::<_, i64>(21)?).map_err(sqlite_conversion_error)?,
        updated_at_ms: u64::try_from(row.get::<_, i64>(22)?).map_err(sqlite_conversion_error)?,
        terminal_at_ms: row
            .get::<_, Option<i64>>(23)?
            .map(|value| u64::try_from(value).map_err(sqlite_conversion_error))
            .transpose()?,
        runtime_options_json: row.get(24)?,
        claim_fence_epoch: row
            .get::<_, Option<i64>>(25)?
            .map(|value| u64::try_from(value).map_err(sqlite_conversion_error))
            .transpose()?,
        application_receipt: row
            .get::<_, Option<String>>(26)?
            .map(|value| serde_json::from_str(&value).map_err(sqlite_conversion_error))
            .transpose()?,
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
}, PostgresMigrationSpec {
    id: "session.0016.task-route-hint",
    domain: SESSION_DOMAIN,
    version: 16,
    description: "persist the Runtime-owned task routing hint with durable Session ingress",
    statements: &[
        "ALTER TABLE session_runtime_outbox
             ADD COLUMN IF NOT EXISTS task_route_hint_json TEXT",
    ],
}, PostgresMigrationSpec {
    id: "session.0017.remove-mission-outbox",
    domain: SESSION_DOMAIN,
    version: 17,
    description: "remove the obsolete Session-owned Mission delivery path",
    statements: &[
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
                 FALSE,
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
                 mission_agent_team_continuation=FALSE,
                 last_activity_ms=GREATEST(
                     session_recovery_manifest.last_activity_ms,
                     EXCLUDED.last_activity_ms
                 ),
                 manifest_revision=
                     session_recovery_manifest.manifest_revision+1;
         END
         $$",
        "DROP TRIGGER IF EXISTS session_recovery_mission_outbox_change
             ON session_mission_outbox",
        "DROP TABLE IF EXISTS session_mission_outbox_history",
        "DROP TABLE IF EXISTS session_mission_outbox",
        "UPDATE session_recovery_manifest
            SET mission_agent_team_continuation=FALSE,
                manifest_revision=manifest_revision+1
          WHERE mission_agent_team_continuation=TRUE",
    ],
}, PostgresMigrationSpec {
    id: "session.0018.input-application-receipt",
    domain: SESSION_DOMAIN,
    version: 18,
    description: "persist Runtime input disposition materialization receipts",
    statements: &[
        "ALTER TABLE session_runtime_outbox ADD COLUMN IF NOT EXISTS application_receipt_json TEXT",
    ],
}, PostgresMigrationSpec {
    id: "session.0019.runtime-outbox-target-turn-index",
    domain: SESSION_DOMAIN,
    version: 19,
    description: "index Runtime input continuations by target turn without mutating prior migrations",
    statements: &[
        "CREATE INDEX IF NOT EXISTS idx_session_runtime_outbox_target_turn
             ON session_runtime_outbox(
                 target_turn_id, session_id, session_generation, sequence
             )
             WHERE target_turn_id IS NOT NULL",
    ],
}];

#[derive(Clone, Debug)]
pub struct PostgresSessionStore {
    executor: PostgresExecutor,
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
    status FROM session_records WHERE session_id=$1";

const RUNTIME_OUTBOX_SELECT: &str =
    "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,
            decision,target_turn_id,classification_json,task_route_hint_json,status,runtime_commit_cursor,attempts,
            next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,last_error,
            revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json
       FROM session_runtime_outbox WHERE request_id=$1";

fn session_params(session: &SessionRecord) -> [&(dyn ToSql + Sync); 13] {
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
            reset_policy,metadata_json,input_tokens,output_tokens,status,
            created_at_ms,updated_at_ms
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
            cowd_safe_session_epoch_ms($6),cowd_safe_session_epoch_ms($7))
         ON CONFLICT(session_id) DO UPDATE SET
            platform=EXCLUDED.platform,chat_id=EXCLUDED.chat_id,user_id=EXCLUDED.user_id,
            model=EXCLUDED.model,created_at=EXCLUDED.created_at,last_activity=EXCLUDED.last_activity,
            message_count=EXCLUDED.message_count,reset_policy=EXCLUDED.reset_policy,
            metadata_json=EXCLUDED.metadata_json,input_tokens=EXCLUDED.input_tokens,
            output_tokens=EXCLUDED.output_tokens,
            status=EXCLUDED.status,created_at_ms=EXCLUDED.created_at_ms,
            updated_at_ms=EXCLUDED.updated_at_ms",
        &session_params(session),
    ).map_err(postgres_error)?;
    Ok(())
}

fn parse_input_decision(value: &str) -> session::SessionResult<InputRoutingDecision> {
    parse_input_decision_value(value).ok_or_else(|| {
        session::SessionError::Store(format!("unknown session input decision `{value}`"))
    })
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
        "attached" => Ok(SessionRuntimeInputStatus::Attached),
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
        task_route_hint: record.task_route_hint.clone(),
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
    let task_route_hint_json = request
        .task_route_hint
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| session::SessionError::Store(error.to_string()))?;
    let row = transaction.query_one(
        "INSERT INTO session_runtime_outbox(
             input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,
             decision,target_turn_id,classification_json,task_route_hint_json,status,attempts,next_attempt_at_ms,
             revision,created_at_ms,updated_at_ms,runtime_options_json
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'accepted',0,$12,0,$12,$12,$13)
         RETURNING input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,
                   decision,target_turn_id,classification_json,task_route_hint_json,status,runtime_commit_cursor,attempts,
                   next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,
                   last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json",
        &[&request.input_id,&request.request_id,&request.turn_id,&request.message_id,
          &message.session_id,&to_i64(message.sequence, "message sequence")?,
          &to_u64_i64(request.session_generation, "session generation")?,
          &input_decision_as_str(request.decision),&request.target_turn_id,
          &request.classification_json,&task_route_hint_json,&now,&request.runtime_options_json],
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
                  decision,target_turn_id,classification_json,task_route_hint_json,status,runtime_commit_cursor,attempts,
                  next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,
                  last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json",
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
                  decision,target_turn_id,classification_json,task_route_hint_json,status,runtime_commit_cursor,attempts,
                  next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,
                  last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json",
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
    let task_route_hint_json: Option<String> = row.try_get(10).map_err(postgres_error)?;
    let status: String = row.try_get(11).map_err(postgres_error)?;
    let failure: Option<String> = row.try_get(18).map_err(postgres_error)?;
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
        task_route_hint: task_route_hint_json
            .map(|value| {
                serde_json::from_str(&value)
                    .map_err(|error| session::SessionError::Store(error.to_string()))
            })
            .transpose()?,
        status: parse_runtime_status(&status)?,
        runtime_commit_cursor: row
            .try_get::<_, Option<i64>>(12)
            .map_err(postgres_error)?
            .map(|value| i64_to_u64(value, "runtime cursor"))
            .transpose()?,
        attempts: i64_to_u32(row.try_get(13).map_err(postgres_error)?, "runtime attempts")?,
        next_attempt_at_ms: i64_to_u64(
            row.try_get(14).map_err(postgres_error)?,
            "runtime next attempt",
        )?,
        claim_owner: row.try_get(15).map_err(postgres_error)?,
        claim_token: row.try_get(16).map_err(postgres_error)?,
        claim_expires_at_ms: row
            .try_get::<_, Option<i64>>(17)
            .map_err(postgres_error)?
            .map(|value| i64_to_u64(value, "runtime lease"))
            .transpose()?,
        failure_class: failure
            .map(|value| {
                OutboxFailureClass::parse(&value)
                    .map_err(|error| session::SessionError::Store(error.to_string()))
            })
            .transpose()?,
        last_error: row.try_get(19).map_err(postgres_error)?,
        revision: i64_to_u64(row.try_get(20).map_err(postgres_error)?, "runtime revision")?,
        created_at_ms: i64_to_u64(
            row.try_get(21).map_err(postgres_error)?,
            "runtime created time",
        )?,
        updated_at_ms: i64_to_u64(
            row.try_get(22).map_err(postgres_error)?,
            "runtime updated time",
        )?,
        terminal_at_ms: row
            .try_get::<_, Option<i64>>(23)
            .map_err(postgres_error)?
            .map(|value| i64_to_u64(value, "runtime terminal time"))
            .transpose()?,
        runtime_options_json: row.try_get(24).map_err(postgres_error)?,
        claim_fence_epoch: row
            .try_get::<_, Option<i64>>(25)
            .map_err(postgres_error)?
            .map(|value| i64_to_u64(value, "runtime claim fence epoch"))
            .transpose()?,
        application_receipt: row
            .try_get::<_, Option<String>>(26)
            .map_err(postgres_error)?
            .map(|value| {
                serde_json::from_str(&value)
                    .map_err(|error| session::SessionError::Store(error.to_string()))
            })
            .transpose()?,
    })
}

fn pg_history_rows(
    connection: &mut PostgresConnection,
    table: &str,
) -> session::SessionResult<Vec<SessionOutboxHistory>> {
    debug_assert_eq!(table, "session_runtime_outbox_history");
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
        && snapshot.runtime_history.is_empty()
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
    let task_route_hint_json = item
        .task_route_hint
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| session::SessionError::Store(error.to_string()))?;
    let application_receipt_json = item
        .application_receipt
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| session::SessionError::Store(error.to_string()))?;
    transaction.execute(
        "INSERT INTO session_runtime_outbox(
             input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,
             decision,target_turn_id,classification_json,task_route_hint_json,status,runtime_commit_cursor,attempts,
             next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,last_error,
             revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)",
        &[&item.input_id,&item.request_id,&item.turn_id,&item.message_id,&item.session_id,
          &to_i64(item.sequence,"runtime sequence")?,
          &to_u64_i64(item.session_generation,"session generation")?,
          &input_decision_as_str(item.decision),&item.target_turn_id,&item.classification_json,
          &task_route_hint_json,&item.status.as_str(),
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
          &item.claim_fence_epoch.map(|value|to_u64_i64(value,"runtime claim fence epoch")).transpose()?,
          &application_receipt_json],
    ).map_err(postgres_error)?;
    Ok(())
}

fn import_history_tx(
    transaction: &mut PostgresTransaction<'_>,
    table: &str,
    item: &SessionOutboxHistory,
) -> session::SessionResult<()> {
    debug_assert_eq!(table, "session_runtime_outbox_history");
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
                decision,target_turn_id,classification_json,task_route_hint_json,status,runtime_commit_cursor,attempts,
                next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,
                last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json
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
                decision,target_turn_id,classification_json,task_route_hint_json,status,runtime_commit_cursor,attempts,
                next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,
                last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json
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

fn runtime_turn_is_terminal_tx(
    transaction: &mut PostgresTransaction<'_>,
    session_id: &str,
    session_generation: u64,
    turn_id: &str,
) -> session::SessionResult<bool> {
    transaction
        .query_one(
            "SELECT EXISTS(
               SELECT 1
                 FROM session_runtime_outbox
                WHERE session_id=$1 AND session_generation=$2 AND turn_id=$3
                  AND status IN (
                    'rejected_duplicate','rejected_policy','completed','supplemented',
                    'failed','cancelled','expired'
                  )
             )",
            &[
                &session_id,
                &to_u64_i64(session_generation, "session generation")?,
                &turn_id,
            ],
        )
        .map_err(postgres_error)?
        .try_get(0)
        .map_err(postgres_error)
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
        status: row.try_get(12).map_err(postgres_error)?,
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
    fn attach_session_runtime_outbox(
        &self,
        a: &str,
        b: u64,
        c: u64,
        d: &str,
        e: &str,
        f: &str,
        g: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        self.attach_session_runtime_outbox(a, b, c, d, e, f, g)
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
    fn set_session_input_application_receipt(
        &self,
        input_ids: &[String],
        expected_revisions: &[u64],
        receipt: &harness_contract::input_disposition::SessionInputApplicationReceipt,
        now_ms: u64,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        self.set_session_input_application_receipt(input_ids, expected_revisions, receipt, now_ms)
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
    fn session_runtime_outbox_for_turn_relation(
        &self,
        a: &str,
        b: u64,
        c: &str,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        self.session_runtime_outbox_for_turn_relation(a, b, c)
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
#[path = "tests.rs"]
mod tests;
