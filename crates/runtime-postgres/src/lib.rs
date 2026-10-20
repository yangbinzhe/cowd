#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]
//! PostgreSQL adapters for Runtime durable domains.
//!
//! This crate owns PostgreSQL SQL and depends on the Runtime backend contract.
//! The `runtime` crate itself remains free of PostgreSQL drivers.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

#[cfg(test)]
use harness_contract::{
    mission::MissionOrganizationAction,
    policy::{
        AutonomyProfileId, ExecutionPolicyBinding, PermissionMode, SessionExecutionPolicy,
        SessionExecutionPolicyOrigin,
    },
    reality::EvidenceRef,
    task::{TaskCreateCommand, TaskOrigin, TaskPhaseSpec, TaskSpec},
};
use postgres::Row;
use runtime::task::{
    validate_backend_mutation, validate_task_aggregate_for_backend, TaskAggregate,
    TaskAggregateService, TaskEvidenceOutboxRecord, TaskMissionAssignmentOutboxRecord,
    TaskMutation, TaskMutationResult, TaskStoreBackend, TaskTurnBinding,
};
use runtime::{
    runtime_event_request_hash_with_terminal as request_hash_with_terminal,
    validate_runtime_decision_lease_claims as validate_decision_lease_claims,
    validate_runtime_event as validate_event,
    validate_runtime_event_transaction as validate_transaction,
    validate_runtime_fenced_terminal as validate_fenced_terminal, AppendTransactionReceipt,
    AppendTransactionRequest, CommittedEventBatch, CommittedStreamRevision, DurableRuntimeEvent,
    ExpectedStreamRevision, MissionOrganizationDecision, MissionOrganizationStatus,
    RuntimeEventInput, RuntimeEventRecord, RuntimeEventScope, RuntimeEventStore,
    RuntimeEventStoreBackend, RuntimeEventStoreError, RuntimeEventStoreResult,
    RuntimeProjectionCheckpoint, RuntimeProjectionInterest, RuntimeProjectionScanPage, RuntimeSourceMutationFence,
    RuntimeProjectionWorkClass, RuntimeSessionOutboxFailureClass, RuntimeSessionOutboxHealth,
    RuntimeSessionOutboxRecord, RuntimeSessionTerminalFenceAdoption, RuntimeTransactionEventInput,
    SessionTerminalInput, TaskKind, TaskMissionAssignment, TaskMissionAssignmentCommand,
    TaskMissionAssignmentReceipt, VerifiedDecisionLease,
};
use serde_json::Value;
use storage::{
    PostgresClient, PostgresConnection, PostgresConnectionConfig, PostgresExecutor,
    PostgresMigrationSpec, PostgresTransaction, SecretRefResolver, StorageError,
};

const RUNTIME_EVENT_DOMAIN: &str = "runtime_event";
const TASK_DOMAIN: &str = "runtime_task";
const ARTIFACT_DOMAIN: &str = "runtime_artifact";
fn validate_clean_domain_catalog(
    executor: &PostgresExecutor,
    domain: &str,
    specs: &[PostgresMigrationSpec],
) -> Result<(), StorageError> {
    let mut connection = executor.checkout_online_read()?;
    let mut connection = connection.transaction()?;
    connection.batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")?;
    let tables = specs
        .iter()
        .flat_map(|spec| spec.statements.iter())
        .filter_map(|statement| {
            statement
                .trim_start()
                .strip_prefix("CREATE TABLE ")
                .and_then(|tail| tail.split_whitespace().next())
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    let row = connection.query_one("SELECT to_regclass('cowd_schema_migrations') IS NOT NULL, (SELECT COUNT(*) FROM unnest($1::TEXT[]) AS names(name) WHERE to_regclass(name) IS NOT NULL)", &[&tables])?;
    let has_ledger: bool = row.get(0);
    let table_count: i64 = row.get(1);
    let rows = if has_ledger {
        connection.query(
            "SELECT id,checksum FROM cowd_schema_migrations WHERE domain=$1",
            &[&domain],
        )?
    } else {
        Vec::new()
    };
    if rows.is_empty() && table_count == 0 {
        connection.commit()?;
        return Ok(());
    }
    if usize::try_from(table_count).ok() != Some(tables.len())
        || rows.len() != specs.len()
        || specs.iter().any(|spec| {
            !rows.iter().any(|row| {
                row.get::<_, String>(0) == spec.id && row.get::<_, String>(1) == spec.checksum()
            })
        })
    {
        return Err(StorageError::Other(format!(
            "unsupported {domain} catalog; select a clean namespace"
        )));
    }
    connection.commit()?;
    Ok(())
}
const MAX_TRANSACTION_BYTES: usize = 32 * 1024 * 1024;
const EVENT_COLUMNS: &str =
    "event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms, \
    commit_cursor, transaction_id, transaction_index, schema_version, idempotency_key";

const RUNTIME_EVENT_MIGRATIONS: &[PostgresMigrationSpec] = &[PostgresMigrationSpec {
    id: "runtime_event.current-baseline",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 1,
    description: "initialize current clean runtime_event schema",
    statements: &[
        r#"DO $clean$ BEGIN IF EXISTS(SELECT 1 FROM cowd_schema_migrations WHERE domain='runtime_event' AND id<>'runtime_event.current-baseline') OR to_regclass('runtime_commits') IS NOT NULL THEN RAISE EXCEPTION 'unsupported runtime_event catalog; initialize an explicitly selected clean namespace'; END IF; END $clean$"#,
        r#"CREATE TABLE runtime_commits (
            commit_cursor BIGSERIAL PRIMARY KEY,
            transaction_id TEXT NOT NULL UNIQUE,
            request_hash TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL
        )"#,
        r#"CREATE TABLE runtime_events (
            event_id TEXT PRIMARY KEY,
            stream_id TEXT NOT NULL,
            sequence BIGINT NOT NULL,
            scope TEXT NOT NULL,
            kind TEXT NOT NULL,
            status TEXT,
            actor TEXT,
            payload JSONB NOT NULL,
            refs JSONB NOT NULL,
            created_at_ms BIGINT NOT NULL,
            commit_cursor BIGINT NOT NULL,
            transaction_id TEXT NOT NULL REFERENCES runtime_commits(transaction_id),
            transaction_index BIGINT NOT NULL,
            schema_version BIGINT NOT NULL DEFAULT 1,
            idempotency_key TEXT
            ,encoded_bytes BIGINT NOT NULL CHECK(encoded_bytes > 0)
            ,session_id TEXT,
            root_execution_id TEXT,
            activity_id TEXT
        )"#,
        r#"CREATE TABLE runtime_event_source_fence (
            singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
            incarnation TEXT NOT NULL,
            mutation_epoch BIGINT NOT NULL DEFAULT 0 CHECK(mutation_epoch >= 0)
        )"#,
        r#"INSERT INTO runtime_event_source_fence(singleton,incarnation)
            VALUES(TRUE,md5(random()::TEXT || clock_timestamp()::TEXT || pg_backend_pid()::TEXT))"#,
        r#"CREATE FUNCTION runtime_event_source_changed() RETURNS trigger LANGUAGE plpgsql AS $fence$
        DECLARE changed_rows BIGINT;
        BEGIN
            PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(TG_TABLE_SCHEMA || ':cowd-runtime-source-mutation', 0));
            EXECUTE pg_catalog.format('UPDATE %I.%I SET mutation_epoch=mutation_epoch+1 WHERE singleton=TRUE', TG_TABLE_SCHEMA, 'runtime_event_source_fence');
            GET DIAGNOSTICS changed_rows = ROW_COUNT;
            IF changed_rows <> 1 THEN RAISE EXCEPTION 'source mutation fence missing'; END IF;
            RETURN NULL;
        END $fence$"#,
        r#"CREATE TRIGGER runtime_events_source_mutation BEFORE UPDATE OR DELETE OR TRUNCATE
            ON runtime_events FOR EACH STATEMENT EXECUTE FUNCTION runtime_event_source_changed()"#,
        r#"CREATE TABLE runtime_transaction_streams (
            transaction_id TEXT NOT NULL REFERENCES runtime_commits(transaction_id),
            stream_id TEXT NOT NULL,
            expected_revision BIGINT NOT NULL,
            committed_revision BIGINT NOT NULL,
            PRIMARY KEY(transaction_id, stream_id)
        )"#,
        r#"CREATE TABLE runtime_stream_heads (
            stream_id TEXT PRIMARY KEY,
            revision BIGINT NOT NULL
        )"#,
        r#"CREATE TABLE runtime_session_outbox (
            terminal_id TEXT PRIMARY KEY,
            message_id TEXT NOT NULL UNIQUE,
            session_id TEXT NOT NULL,
            commit_cursor BIGINT NOT NULL REFERENCES runtime_commits(commit_cursor),
            payload_ref TEXT NOT NULL,
            execution_id TEXT,
            turn_id TEXT,
            request_id TEXT,
            session_generation BIGINT,
            input_sequence BIGINT,
            input_claim_owner TEXT,
            input_claim_token TEXT,
            input_claim_revision BIGINT,
            status TEXT NOT NULL,
            attempts BIGINT NOT NULL DEFAULT 0,
            next_attempt_at BIGINT,
            claim_owner TEXT,
            claim_expires_at BIGINT,
            failure_class TEXT,
            last_error TEXT,
            materialized_at BIGINT,
            revision BIGINT NOT NULL DEFAULT 0
        )"#,
        r#"CREATE TABLE runtime_consumed_decision_leases (
            lease_id TEXT PRIMARY KEY,
            principal_id TEXT NOT NULL,
            review_id TEXT NOT NULL,
            action TEXT NOT NULL,
            scope TEXT NOT NULL,
            evidence_digest TEXT NOT NULL,
            credential_epoch BIGINT NOT NULL,
            consumed_at_ms BIGINT NOT NULL
        )"#,
        r#"CREATE TABLE runtime_projection_checkpoints (
            projection_id TEXT PRIMARY KEY,
            source_cursor BIGINT NOT NULL,
            revision BIGINT NOT NULL,
            payload JSONB NOT NULL,
            updated_at_ms BIGINT NOT NULL
        )"#,
        r#"CREATE UNIQUE INDEX idx_runtime_events_stream_sequence
            ON runtime_events(stream_id, sequence)"#,
        r#"CREATE INDEX idx_runtime_events_scope_created
            ON runtime_events(scope, created_at_ms)"#,
        r#"CREATE UNIQUE INDEX idx_runtime_events_commit_index
            ON runtime_events(commit_cursor, transaction_index)"#,
        r#"CREATE UNIQUE INDEX idx_runtime_events_transaction_index
            ON runtime_events(transaction_id, transaction_index)"#,
        r#"CREATE UNIQUE INDEX idx_runtime_events_stream_idempotency
            ON runtime_events(stream_id, idempotency_key) WHERE idempotency_key IS NOT NULL"#,
        r#"CREATE INDEX idx_runtime_session_outbox_claim
            ON runtime_session_outbox(status, next_attempt_at, claim_expires_at, commit_cursor)"#,
        r#"CREATE INDEX idx_runtime_session_outbox_execution
            ON runtime_session_outbox(session_id, execution_id, turn_id)"#,
        r#"CREATE INDEX idx_runtime_consumed_decision_leases_review
            ON runtime_consumed_decision_leases(review_id, action)"#,
        r#"CREATE INDEX idx_runtime_events_refs_gin
            ON runtime_events USING GIN (refs jsonb_path_ops)"#,
        r#"CREATE INDEX idx_runtime_events_scope_commit
            ON runtime_events(scope, commit_cursor, transaction_index)"#,
        r#"CREATE INDEX idx_runtime_events_scope_kind_commit
            ON runtime_events(scope, kind, commit_cursor, transaction_index)"#,
        r#"CREATE INDEX idx_runtime_events_stream_kind_sequence
            ON runtime_events(stream_id, kind, sequence DESC)"#,
        r#"CREATE INDEX idx_runtime_events_scope_stream_commit
            ON runtime_events(scope, stream_id, commit_cursor, transaction_index)"#,
        r#"CREATE INDEX idx_runtime_events_scope_stream_sequence
            ON runtime_events(scope, stream_id, sequence DESC) INCLUDE(status)"#,
        r#"CREATE INDEX idx_runtime_events_root_execution_commit
            ON runtime_events(root_execution_id, commit_cursor, transaction_index)
            WHERE root_execution_id IS NOT NULL"#,
        r#"CREATE INDEX idx_runtime_events_root_kind_commit
            ON runtime_events(root_execution_id, kind, commit_cursor, transaction_index)
            WHERE root_execution_id IS NOT NULL"#,
        r#"CREATE INDEX idx_runtime_events_activity_commit
            ON runtime_events(activity_id, commit_cursor, transaction_index)
            WHERE activity_id IS NOT NULL"#,
        r#"CREATE INDEX idx_runtime_events_session_position ON runtime_events(session_id,commit_cursor,transaction_index) WHERE session_id IS NOT NULL"#,
    ],
}];

const TASK_MIGRATIONS: &[PostgresMigrationSpec] = &[PostgresMigrationSpec {
    id: "runtime_task.current-baseline",
    domain: TASK_DOMAIN,
    version: 1,
    description: "initialize current clean runtime_task schema",
    statements: &[
        r#"DO $clean$ BEGIN IF EXISTS(SELECT 1 FROM cowd_schema_migrations WHERE domain='runtime_task' AND id<>'runtime_task.current-baseline') OR to_regclass('runtime_tasks') IS NOT NULL THEN RAISE EXCEPTION 'unsupported runtime_task catalog; initialize an explicitly selected clean namespace'; END IF; END $clean$"#,
        r#"CREATE TABLE runtime_tasks (
            task_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL,
            record_json JSONB NOT NULL
        )"#,
        r#"CREATE TABLE runtime_task_evidence_outbox (
            outbox_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL REFERENCES runtime_tasks(task_id) ON DELETE CASCADE,
                revision BIGINT NOT NULL,
                event_kind TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                projected_at_ms BIGINT,
                record_json JSONB NOT NULL,
                UNIQUE(task_id, revision)
        )"#,
        r#"CREATE TABLE runtime_task_graph_refs (
            task_id TEXT NOT NULL REFERENCES runtime_tasks(task_id) ON DELETE CASCADE,
                graph_id TEXT NOT NULL,
                graph_revision BIGINT NOT NULL,
                PRIMARY KEY(task_id, graph_id)
        )"#,
        r#"CREATE TABLE runtime_task_turn_bindings (
            binding_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL REFERENCES runtime_tasks(task_id) ON DELETE CASCADE,
                session_id TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                role TEXT NOT NULL,
                input_id TEXT,
                bound_at_ms BIGINT NOT NULL,
                record_json JSONB NOT NULL,
                UNIQUE(task_id, session_id, turn_id)
        )"#,
        r#"CREATE TABLE runtime_task_mission_assignment_outbox (
            operation_id TEXT PRIMARY KEY,
                created_at_ms BIGINT NOT NULL,
                projected_at_ms BIGINT,
                record_json JSONB NOT NULL
        )"#,
        r#"CREATE TABLE runtime_mission_organization_decisions (
            decision_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                next_attempt_at_ms BIGINT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                record_json JSONB NOT NULL
        )"#,
        r#"CREATE INDEX idx_runtime_tasks_status_created
            ON runtime_tasks(status, created_at_ms DESC, task_id DESC)"#,
        r#"CREATE INDEX idx_runtime_task_evidence_outbox_pending
                ON runtime_task_evidence_outbox(projected_at_ms, created_at_ms, outbox_id)"#,
        r#"CREATE INDEX idx_runtime_task_graph_refs_graph
                ON runtime_task_graph_refs(graph_id, task_id)"#,
        r#"CREATE UNIQUE INDEX idx_runtime_task_turn_primary
                ON runtime_task_turn_bindings(session_id, turn_id)
                WHERE role='primary'"#,
        r#"CREATE INDEX idx_runtime_task_turn_session
                ON runtime_task_turn_bindings(session_id, bound_at_ms DESC, binding_id)"#,
        r#"CREATE INDEX idx_runtime_task_turn_task
                ON runtime_task_turn_bindings(task_id, bound_at_ms ASC, binding_id)"#,
        r#"CREATE INDEX idx_runtime_task_mission_assignment_pending
                ON runtime_task_mission_assignment_outbox(projected_at_ms, created_at_ms, operation_id)"#,
        r#"CREATE INDEX idx_runtime_mission_organization_claim
                ON runtime_mission_organization_decisions(status, next_attempt_at_ms, created_at_ms, decision_id)"#,
    ],
}];

const ARTIFACT_MIGRATIONS: &[PostgresMigrationSpec] = &[PostgresMigrationSpec {
    id: "runtime_artifact.current-baseline",
    domain: ARTIFACT_DOMAIN,
    version: 1,
    description: "initialize current clean runtime_artifact schema",
    statements: &[
        r#"DO $clean$ BEGIN IF EXISTS(SELECT 1 FROM cowd_schema_migrations WHERE domain='runtime_artifact' AND id<>'runtime_artifact.current-baseline') OR to_regclass('artifact_objects') IS NOT NULL THEN RAISE EXCEPTION 'unsupported runtime_artifact catalog; initialize an explicitly selected clean namespace'; END IF; END $clean$"#,
        r#"CREATE TABLE artifact_objects (
            sha256 TEXT PRIMARY KEY,
            generation TEXT NOT NULL,
            ready BOOLEAN NOT NULL DEFAULT FALSE,
            deleting BOOLEAN NOT NULL DEFAULT FALSE,
            inflight BIGINT NOT NULL DEFAULT 0 CHECK(inflight >= 0),
            bytes BIGINT NOT NULL,
            tier TEXT NOT NULL,
            compact_body BYTEA,
            created_at_ms BIGINT NOT NULL
        )"#,
        r#"CREATE TABLE artifact_records (
            artifact_id TEXT PRIMARY KEY,
            record_class TEXT NOT NULL CHECK(record_class IN ('live','private_prepared')),
            prepared_origin JSONB CHECK(prepared_origin IS NULL OR octet_length(prepared_origin::TEXT) <= 8192),
            sha256 TEXT NOT NULL REFERENCES artifact_objects(sha256),
            bytes BIGINT NOT NULL,
            media_type TEXT NOT NULL,
            visibility_scope TEXT NOT NULL,
            tier TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            last_access_at_ms BIGINT NOT NULL,
            catalog_creation_xid xid8 NOT NULL DEFAULT pg_current_xact_id()
            ,CHECK(record_class <> 'private_prepared' OR (length(artifact_id)<=128 AND length(sha256)<=71 AND length(media_type)<=256 AND length(visibility_scope)<=1024))
        )"#,
        r#"CREATE TABLE artifact_pins (
            artifact_id TEXT NOT NULL REFERENCES artifact_records(artifact_id) ON DELETE CASCADE,
            owner TEXT NOT NULL,
            until_ms BIGINT NOT NULL,
            PRIMARY KEY(artifact_id, owner)
        )"#,
        r#"CREATE TABLE artifact_publication_attempts (
            token TEXT PRIMARY KEY CHECK(length(token) BETWEEN 1 AND 128),
            sha256 TEXT NOT NULL,
            generation TEXT NOT NULL,
            publication_id TEXT NOT NULL CHECK(length(publication_id) BETWEEN 1 AND 128),
            state TEXT NOT NULL CHECK(state IN ('active','consumed','released'))
        )"#,
        r#"CREATE INDEX idx_artifact_active_attempts ON artifact_publication_attempts(token) WHERE state='active'"#,
        r#"CREATE INDEX idx_artifact_prepared_inventory ON artifact_records(artifact_id) WHERE record_class='private_prepared'"#,
        r#"CREATE TABLE artifact_catalog_invalidations (
            scope_key TEXT PRIMARY KEY, revision BIGINT NOT NULL
        )"#,
        r#"CREATE INDEX idx_artifact_records_hash
            ON artifact_records(sha256)"#,
        r#"CREATE INDEX idx_artifact_pins_expiry
            ON artifact_pins(until_ms)"#,
        r#"CREATE INDEX idx_artifact_catalog_scope_id ON artifact_records(visibility_scope,artifact_id) WHERE record_class='live'"#,
        r#"CREATE FUNCTION artifact_catalog_record_changed() RETURNS trigger LANGUAGE plpgsql AS $$
         DECLARE old_scope TEXT; new_scope TEXT; affected_scope TEXT;
         BEGIN
           IF TG_OP='UPDATE' AND (to_jsonb(OLD)-'last_access_at_ms')=(to_jsonb(NEW)-'last_access_at_ms') THEN RETURN NULL; END IF;
           old_scope := OLD.visibility_scope;
           IF TG_OP='UPDATE' THEN new_scope := NEW.visibility_scope; END IF;
           FOR affected_scope IN SELECT DISTINCT scope FROM unnest(ARRAY[old_scope,new_scope]) AS scope WHERE scope IS NOT NULL ORDER BY scope LOOP
             INSERT INTO artifact_catalog_invalidations(scope_key,revision) VALUES(affected_scope,1)
             ON CONFLICT(scope_key) DO UPDATE SET revision=artifact_catalog_invalidations.revision+1;
           END LOOP;
           RETURN NULL;
         END $$"#,
        r#"CREATE TRIGGER artifact_catalog_revision AFTER UPDATE OR DELETE ON artifact_records FOR EACH ROW EXECUTE FUNCTION artifact_catalog_record_changed()"#,
    ],
}];

mod event_store;
pub use event_store::PostgresRuntimeEventStore;
mod task_store;
pub use task_store::PostgresTaskStore;
mod artifact_store;
pub use artifact_store::PostgresArtifactRepository;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
