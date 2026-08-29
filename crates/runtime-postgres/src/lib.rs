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
use std::fs;
use std::path::{Path, PathBuf};
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
    TaskMutation, TaskMutationResult, TaskStoreBackend, TaskStoreSnapshot, TaskTurnBinding,
};
use runtime::{
    runtime_event_request_hash_with_terminal as request_hash_with_terminal,
    validate_runtime_decision_lease_claims as validate_decision_lease_claims,
    validate_runtime_event as validate_event,
    validate_runtime_event_transaction as validate_transaction,
    validate_runtime_fenced_terminal as validate_fenced_terminal, AppendTransactionReceipt,
    AppendTransactionRequest, CommittedEventBatch, CommittedStreamRevision, DurableRuntimeEvent,
    ExpectedStreamRevision, MissionOrganizationDecision, MissionOrganizationStatus,
    RuntimeDecisionLeaseSnapshot, RuntimeEventCommitSnapshot, RuntimeEventInput,
    RuntimeEventRecord, RuntimeEventScope, RuntimeEventStore, RuntimeEventStoreBackend,
    RuntimeEventStoreError, RuntimeEventStoreResult, RuntimeEventStoreSnapshot,
    RuntimeEventStreamHeadSnapshot, RuntimeEventTransactionStreamSnapshot,
    RuntimeProjectionCheckpoint, RuntimeProjectionInterest, RuntimeProjectionScanPage,
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
const MAX_TRANSACTION_BYTES: usize = 32 * 1024 * 1024;
const EVENT_COLUMNS: &str =
    "event_id, stream_id, sequence, scope, kind, status, actor, payload, refs, created_at_ms, \
    commit_cursor, transaction_id, transaction_index, schema_version, idempotency_key";

const RUNTIME_EVENT_MIGRATIONS: &[PostgresMigrationSpec] = &[PostgresMigrationSpec {
    id: "runtime_event.0001.initial",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 1,
    description: "create durable runtime event ledger",
    statements: &[
        "CREATE TABLE IF NOT EXISTS runtime_commits (
            commit_cursor BIGSERIAL PRIMARY KEY,
            transaction_id TEXT NOT NULL UNIQUE,
            request_hash TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS runtime_events (
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
        )",
        "CREATE TABLE IF NOT EXISTS runtime_transaction_streams (
            transaction_id TEXT NOT NULL REFERENCES runtime_commits(transaction_id),
            stream_id TEXT NOT NULL,
            expected_revision BIGINT NOT NULL,
            committed_revision BIGINT NOT NULL,
            PRIMARY KEY(transaction_id, stream_id)
        )",
        "CREATE TABLE IF NOT EXISTS runtime_stream_heads (
            stream_id TEXT PRIMARY KEY,
            revision BIGINT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS runtime_session_outbox (
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
        )",
        "CREATE TABLE IF NOT EXISTS runtime_consumed_decision_leases (
            lease_id TEXT PRIMARY KEY,
            principal_id TEXT NOT NULL,
            review_id TEXT NOT NULL,
            action TEXT NOT NULL,
            scope TEXT NOT NULL,
            evidence_digest TEXT NOT NULL,
            credential_epoch BIGINT NOT NULL,
            consumed_at_ms BIGINT NOT NULL
        )",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_stream_sequence
            ON runtime_events(stream_id, sequence)",
        "CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_created
            ON runtime_events(scope, created_at_ms)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_commit_index
            ON runtime_events(commit_cursor, transaction_index)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_transaction_index
            ON runtime_events(transaction_id, transaction_index)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_stream_idempotency
            ON runtime_events(stream_id, idempotency_key) WHERE idempotency_key IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_runtime_commits_cursor ON runtime_commits(commit_cursor)",
        "CREATE INDEX IF NOT EXISTS idx_runtime_session_outbox_claim
            ON runtime_session_outbox(status, next_attempt_at, claim_expires_at, commit_cursor)",
        "CREATE INDEX IF NOT EXISTS idx_runtime_consumed_decision_leases_review
            ON runtime_consumed_decision_leases(review_id, action)",
    ],
}, PostgresMigrationSpec {
    id: "runtime_event.0002.terminal-execution-relation",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 2,
    description: "persist terminal execution and turn correlation",
    statements: &[
        "ALTER TABLE runtime_session_outbox ADD COLUMN IF NOT EXISTS execution_id TEXT",
        "ALTER TABLE runtime_session_outbox ADD COLUMN IF NOT EXISTS turn_id TEXT",
    ],
}, PostgresMigrationSpec {
    id: "runtime_event.0003.terminal-execution-fence",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 3,
    description: "persist terminal Session generation and input claim fence",
    statements: &[
        "ALTER TABLE runtime_session_outbox ADD COLUMN IF NOT EXISTS request_id TEXT",
        "ALTER TABLE runtime_session_outbox ADD COLUMN IF NOT EXISTS session_generation BIGINT",
        "ALTER TABLE runtime_session_outbox ADD COLUMN IF NOT EXISTS input_claim_owner TEXT",
        "ALTER TABLE runtime_session_outbox ADD COLUMN IF NOT EXISTS input_claim_token TEXT",
        "ALTER TABLE runtime_session_outbox ADD COLUMN IF NOT EXISTS input_claim_revision BIGINT",
    ],
}, PostgresMigrationSpec {
    id: "runtime_event.0004.terminal-input-sequence-fence",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 4,
    description: "persist the exact durable Session input sequence in terminal fences",
    statements: &[
        "ALTER TABLE runtime_session_outbox ADD COLUMN IF NOT EXISTS input_sequence BIGINT",
    ],
}, PostgresMigrationSpec {
    id: "runtime_event.0005.event-reference-index",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 5,
    description: "index durable event references for reverse lineage queries",
    statements: &[
        "CREATE INDEX IF NOT EXISTS idx_runtime_events_refs_gin
            ON runtime_events USING GIN (refs jsonb_path_ops)",
    ],
}, PostgresMigrationSpec {
    id: "runtime_event.0006.session-reference-and-scope-replay",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 6,
    description: "backfill terminal Session references and index complete scope replay",
    statements: &[
        "UPDATE runtime_events
         SET refs = refs || jsonb_build_array(
             jsonb_build_object('kind', 'session', 'id', payload->>'session_id')
         )
         WHERE kind = 'runtime.session.terminal_requested'
           AND NULLIF(BTRIM(payload->>'session_id'), '') IS NOT NULL
           AND NOT refs @> jsonb_build_array(
               jsonb_build_object('kind', 'session', 'id', payload->>'session_id')
           )",
        "CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_commit
            ON runtime_events(scope, commit_cursor, transaction_index)",
        "CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_kind_commit
            ON runtime_events(scope, kind, commit_cursor, transaction_index)",
    ],
}, PostgresMigrationSpec {
    id: "runtime_event.0007.stream-kind-cursor",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 7,
    description: "index exact projector checkpoint lookup by stream and event kind",
    statements: &[
        "CREATE INDEX IF NOT EXISTS idx_runtime_events_stream_kind_sequence
            ON runtime_events(stream_id, kind, sequence DESC)",
    ],
}, PostgresMigrationSpec {
    id: "runtime_event.0008.scope-stream-family-replay",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 8,
    description: "index aggregate-family replay without scanning an entire runtime scope",
    statements: &[
        "CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_stream_commit
            ON runtime_events(scope, stream_id, commit_cursor, transaction_index)",
    ],
}, PostgresMigrationSpec {
    id: "runtime_event.0009.latest-stream-status",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 9,
    description: "index canonical stream discovery by latest durable status",
    statements: &[
        "CREATE INDEX IF NOT EXISTS idx_runtime_events_scope_stream_sequence
            ON runtime_events(scope, stream_id, sequence DESC) INCLUDE(status)",
    ],
}, PostgresMigrationSpec {
    id: "runtime_event.0010.activity-identity-index",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 10,
    description: "index Runtime-owned root execution and activity identities",
    statements: &[
        "ALTER TABLE runtime_events ADD COLUMN IF NOT EXISTS root_execution_id TEXT",
        "ALTER TABLE runtime_events ADD COLUMN IF NOT EXISTS activity_id TEXT",
        "UPDATE runtime_events
            SET root_execution_id = payload #>> '{_runtime_activity_binding,root_execution_id}',
                activity_id = payload #>> '{_runtime_activity_binding,activity_id}'
          WHERE root_execution_id IS NULL OR activity_id IS NULL",
        "CREATE INDEX IF NOT EXISTS idx_runtime_events_root_execution_commit
            ON runtime_events(root_execution_id, commit_cursor, transaction_index)
            WHERE root_execution_id IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_runtime_events_root_kind_commit
            ON runtime_events(root_execution_id, kind, commit_cursor, transaction_index)
            WHERE root_execution_id IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_runtime_events_activity_commit
            ON runtime_events(activity_id, commit_cursor, transaction_index)
            WHERE activity_id IS NOT NULL",
    ],
}, PostgresMigrationSpec {
    id: "runtime_event.0011.mutable-projection-checkpoints",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 11,
    description: "move rebuildable projector cursors out of the immutable Runtime journal",
    statements: &[
        "CREATE TABLE IF NOT EXISTS runtime_projection_checkpoints (
            projection_id TEXT PRIMARY KEY,
            source_cursor BIGINT NOT NULL,
            revision BIGINT NOT NULL,
            payload JSONB NOT NULL,
            updated_at_ms BIGINT NOT NULL
        )",
        "INSERT INTO runtime_projection_checkpoints
            (projection_id, source_cursor, revision, payload, updated_at_ms)
         SELECT 'projector:knowledge-candidate',
                (payload->>'source_cursor')::BIGINT, 1, payload, created_at_ms
           FROM runtime_events
          WHERE kind='knowledge.candidate.projector.checkpoint.v1'
          ORDER BY commit_cursor DESC, transaction_index DESC
          LIMIT 1
         ON CONFLICT(projection_id) DO NOTHING",
        "INSERT INTO runtime_projection_checkpoints
            (projection_id, source_cursor, revision, payload, updated_at_ms)
         SELECT 'projector:evolution-signal',
                (payload->>'source_cursor')::BIGINT, 1, payload, created_at_ms
           FROM runtime_events
          WHERE kind='evolution.signal.projector.checkpoint.v1'
          ORDER BY commit_cursor DESC, transaction_index DESC
          LIMIT 1
         ON CONFLICT(projection_id) DO NOTHING",
        "INSERT INTO runtime_projection_checkpoints
            (projection_id, source_cursor, revision, payload, updated_at_ms)
         SELECT 'projector:outcome',
                (payload#>>'{checkpoint,source_cursor}')::BIGINT, 1, payload, created_at_ms
           FROM runtime_events
          WHERE kind='runtime.outcome.projector.checkpoint.v1'
          ORDER BY commit_cursor DESC, transaction_index DESC
          LIMIT 1
         ON CONFLICT(projection_id) DO NOTHING",
        "DELETE FROM runtime_events WHERE kind LIKE '%.projector.checkpoint.v1'",
        "DELETE FROM runtime_transaction_streams AS stream
          WHERE NOT EXISTS (
              SELECT 1 FROM runtime_events AS event
               WHERE event.transaction_id=stream.transaction_id
          )",
        "DELETE FROM runtime_commits AS committed
          WHERE NOT EXISTS (
              SELECT 1 FROM runtime_events AS event
               WHERE event.transaction_id=committed.transaction_id
          )",
    ],
}, PostgresMigrationSpec {
    id: "runtime_event.0012.mutable-mission-evidence-checkpoint",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 12,
    description: "move the Mission evidence projector cursor out of the immutable Runtime journal",
    statements: &[
        "INSERT INTO runtime_projection_checkpoints
            (projection_id, source_cursor, revision, payload, updated_at_ms)
         SELECT 'projector:mission-evidence',
                (payload#>>'{projection,source_cursor}')::BIGINT,
                1,
                payload->'projection',
                created_at_ms
           FROM runtime_events
          WHERE kind='mission_evidence.projector.checkpoint.v1'
          ORDER BY commit_cursor DESC, transaction_index DESC
          LIMIT 1
         ON CONFLICT(projection_id) DO NOTHING",
        "DELETE FROM runtime_events
          WHERE kind='mission_evidence.projector.checkpoint.v1'",
        "DELETE FROM runtime_transaction_streams AS stream
          WHERE NOT EXISTS (
              SELECT 1 FROM runtime_events AS event
               WHERE event.transaction_id=stream.transaction_id
          )",
        "DELETE FROM runtime_commits AS committed
          WHERE NOT EXISTS (
              SELECT 1 FROM runtime_events AS event
               WHERE event.transaction_id=committed.transaction_id
          )",
    ],
}, PostgresMigrationSpec {
    id: "runtime_event.0013.remove-live-snapshot-history",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 13,
    description: "retain active live execution state as mutable checkpoints and remove derived snapshot history",
    statements: &[
        "INSERT INTO runtime_projection_checkpoints
            (projection_id, source_cursor, revision, payload, updated_at_ms)
         SELECT DISTINCT ON (payload->>'execution_id')
                'execution-live:' || (payload->>'execution_id'),
                commit_cursor,
                1,
                payload,
                created_at_ms
           FROM runtime_events
          WHERE kind='execution.live.snapshot.v1'
            AND NULLIF(payload->>'execution_id', '') IS NOT NULL
            AND payload#>>'{live,status}' NOT IN ('complete', 'error', 'cancelled')
          ORDER BY payload->>'execution_id', commit_cursor DESC, transaction_index DESC
         ON CONFLICT(projection_id) DO NOTHING",
        "DELETE FROM runtime_projection_checkpoints
          WHERE projection_id LIKE 'execution-live:%'
            AND payload#>>'{live,status}' IN ('complete', 'error', 'cancelled')",
        "DELETE FROM runtime_events
          WHERE kind='execution.live.snapshot.v1'",
        "DELETE FROM runtime_transaction_streams AS stream
          WHERE NOT EXISTS (
              SELECT 1 FROM runtime_events AS event
               WHERE event.transaction_id=stream.transaction_id
          )",
        "DELETE FROM runtime_commits AS committed
          WHERE NOT EXISTS (
              SELECT 1 FROM runtime_events AS event
               WHERE event.transaction_id=committed.transaction_id
          )",
        "DELETE FROM runtime_stream_heads AS head
          WHERE NOT EXISTS (
              SELECT 1 FROM runtime_events AS event
               WHERE event.stream_id=head.stream_id
          )",
    ],
}, PostgresMigrationSpec {
    id: "runtime_event.0014.remove-redundant-commit-index",
    domain: RUNTIME_EVENT_DOMAIN,
    version: 14,
    description: "remove the commit cursor index duplicated by the primary key",
    statements: &["DROP INDEX IF EXISTS idx_runtime_commits_cursor"],
}];

const TASK_MIGRATIONS: &[PostgresMigrationSpec] = &[
    PostgresMigrationSpec {
        id: "runtime_task.0001.initial",
        domain: TASK_DOMAIN,
        version: 1,
        description: "create durable task control-plane records",
        statements: &[
            "CREATE TABLE IF NOT EXISTS runtime_tasks (
            task_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL,
            record_json JSONB NOT NULL
        )",
            "CREATE INDEX IF NOT EXISTS idx_runtime_tasks_status_created
            ON runtime_tasks(status, created_at_ms DESC, task_id DESC)",
        ],
    },
    PostgresMigrationSpec {
        id: "runtime_task.0002.evidence-outbox",
        domain: TASK_DOMAIN,
        version: 2,
        description: "commit canonical task revisions with a durable evidence outbox",
        statements: &[
            "CREATE TABLE IF NOT EXISTS runtime_task_evidence_outbox (
                outbox_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL REFERENCES runtime_tasks(task_id) ON DELETE CASCADE,
                revision BIGINT NOT NULL,
                event_kind TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                projected_at_ms BIGINT,
                record_json JSONB NOT NULL,
                UNIQUE(task_id, revision)
            )",
            "CREATE INDEX IF NOT EXISTS idx_runtime_task_evidence_outbox_pending
                ON runtime_task_evidence_outbox(projected_at_ms, created_at_ms, outbox_id)",
        ],
    },
    PostgresMigrationSpec {
        id: "runtime_task.0003.graph-reference-index",
        domain: TASK_DOMAIN,
        version: 3,
        description: "index canonical Task ownership by execution graph",
        statements: &[
            "CREATE TABLE IF NOT EXISTS runtime_task_graph_refs (
                task_id TEXT NOT NULL REFERENCES runtime_tasks(task_id) ON DELETE CASCADE,
                graph_id TEXT NOT NULL,
                graph_revision BIGINT NOT NULL,
                PRIMARY KEY(task_id, graph_id)
            )",
            "INSERT INTO runtime_task_graph_refs(task_id, graph_id, graph_revision)
             SELECT task.task_id,
                    reference ->> 'graph_id',
                    COALESCE((reference ->> 'graph_revision')::BIGINT, 0)
               FROM runtime_tasks AS task
               CROSS JOIN LATERAL jsonb_array_elements(
                   COALESCE(task.record_json -> 'graph_refs', '[]'::jsonb)
               ) AS reference
              WHERE NULLIF(reference ->> 'graph_id', '') IS NOT NULL
             ON CONFLICT(task_id, graph_id) DO UPDATE
                 SET graph_revision=EXCLUDED.graph_revision",
            "CREATE INDEX IF NOT EXISTS idx_runtime_task_graph_refs_graph
                ON runtime_task_graph_refs(graph_id, task_id)",
        ],
    },
    PostgresMigrationSpec {
        id: "runtime_task.0004.turn-bindings",
        domain: TASK_DOMAIN,
        version: 4,
        description: "bind canonical Tasks to Session Turns with one primary per Turn",
        statements: &[
            "CREATE TABLE IF NOT EXISTS runtime_task_turn_bindings (
                binding_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL REFERENCES runtime_tasks(task_id) ON DELETE CASCADE,
                session_id TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                role TEXT NOT NULL,
                input_id TEXT,
                bound_at_ms BIGINT NOT NULL,
                record_json JSONB NOT NULL,
                UNIQUE(task_id, session_id, turn_id)
            )",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_task_turn_primary
                ON runtime_task_turn_bindings(session_id, turn_id)
                WHERE role='primary'",
            "CREATE INDEX IF NOT EXISTS idx_runtime_task_turn_session
                ON runtime_task_turn_bindings(session_id, bound_at_ms DESC, binding_id)",
            "CREATE INDEX IF NOT EXISTS idx_runtime_task_turn_task
                ON runtime_task_turn_bindings(task_id, bound_at_ms ASC, binding_id)",
        ],
    },
    PostgresMigrationSpec {
        id: "runtime_task.0005.mission-assignment-and-organization",
        domain: TASK_DOMAIN,
        version: 5,
        description: "add atomic Task Mission assignment receipts and organizer decisions",
        statements: &[
            "CREATE TABLE IF NOT EXISTS runtime_task_mission_assignment_outbox (
                operation_id TEXT PRIMARY KEY,
                created_at_ms BIGINT NOT NULL,
                projected_at_ms BIGINT,
                record_json JSONB NOT NULL
            )",
            "CREATE INDEX IF NOT EXISTS idx_runtime_task_mission_assignment_pending
                ON runtime_task_mission_assignment_outbox(projected_at_ms, created_at_ms, operation_id)",
            "CREATE TABLE IF NOT EXISTS runtime_mission_organization_decisions (
                decision_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                next_attempt_at_ms BIGINT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                record_json JSONB NOT NULL
            )",
            "CREATE INDEX IF NOT EXISTS idx_runtime_mission_organization_claim
                ON runtime_mission_organization_decisions(status, next_attempt_at_ms, created_at_ms, decision_id)",
        ],
    },
    PostgresMigrationSpec {
        id: "runtime_task.0006.root-lineage-and-turn-binding-backfill",
        domain: TASK_DOMAIN,
        version: 6,
        description: "upgrade pre-lineage task aggregates into locked root tasks with primary turn bindings",
        statements: &[
            "WITH legacy AS (
                SELECT task.*,
                       COALESCE(NULLIF(task.record_json ->> 'source_session_id',''), 'legacy-session:' || task.task_id) AS legacy_session_id,
                       COALESCE(NULLIF(task.record_json ->> 'source_turn_id',''), 'legacy-turn:' || task.task_id) AS legacy_turn_id,
                       CASE WHEN row_number() OVER (
                           PARTITION BY task.record_json ->> 'source_session_id', task.record_json ->> 'source_turn_id'
                           ORDER BY task.created_at_ms, task.task_id
                       )=1 THEN 'primary' ELSE 'additional' END AS legacy_role
                  FROM runtime_tasks AS task
                 WHERE NOT (task.record_json ? 'kind')
             )
             INSERT INTO runtime_task_turn_bindings(
                binding_id,task_id,session_id,turn_id,role,input_id,bound_at_ms,record_json
             )
             SELECT 'task-turn:legacy:' || md5(legacy.task_id || ':' || legacy.legacy_turn_id),
                    legacy.task_id,
                    legacy.legacy_session_id,
                    legacy.legacy_turn_id,
                    legacy.legacy_role,
                    NULL,
                    legacy.created_at_ms,
                    jsonb_build_object(
                        'binding_id','task-turn:legacy:' || md5(legacy.task_id || ':' || legacy.legacy_turn_id),
                        'task_id',legacy.task_id,
                        'session_id',legacy.legacy_session_id,
                        'turn_id',legacy.legacy_turn_id,
                        'role',legacy.legacy_role,
                        'evidence_refs','[]'::jsonb,
                        'bound_at_ms',legacy.created_at_ms
                    )
               FROM legacy
             ON CONFLICT DO NOTHING",
            "UPDATE runtime_tasks AS task
                SET record_json=(task.record_json - 'source_session_id' - 'source_turn_id') ||
                    jsonb_build_object(
                        'kind','root',
                        'origin','system',
                        'origin_session_id',COALESCE(NULLIF(task.record_json ->> 'source_session_id',''), 'legacy-session:' || task.task_id),
                        'origin_turn_id',COALESCE(NULLIF(task.record_json ->> 'source_turn_id',''), 'legacy-turn:' || task.task_id),
                        'root_task_id',task.task_id,
                        'parent_task_id',NULL,
                        'predecessor_task_id',NULL,
                        'mission_assignment','explicit_locked',
                        'mission_assignment_revision',1,
                        'mission_assigned_by','migration/runtime-task-v6',
                        'mission_assignment_evidence_refs','[]'::jsonb
                    )
              WHERE NOT (task.record_json ? 'kind')",
        ],
    },
];

const ARTIFACT_MIGRATIONS: &[PostgresMigrationSpec] = &[PostgresMigrationSpec {
    id: "runtime_artifact.0001.initial",
    domain: ARTIFACT_DOMAIN,
    version: 1,
    description: "create unified artifact compact tier and catalogue",
    statements: &[
        "CREATE TABLE IF NOT EXISTS artifact_objects (
            sha256 TEXT PRIMARY KEY,
            bytes BIGINT NOT NULL,
            tier TEXT NOT NULL,
            compact_body BYTEA,
            created_at_ms BIGINT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS artifact_records (
            artifact_id TEXT PRIMARY KEY,
            sha256 TEXT NOT NULL REFERENCES artifact_objects(sha256),
            bytes BIGINT NOT NULL,
            media_type TEXT NOT NULL,
            visibility_scope TEXT NOT NULL,
            tier TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            last_access_at_ms BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_artifact_records_hash
            ON artifact_records(sha256)",
        "CREATE TABLE IF NOT EXISTS artifact_pins (
            artifact_id TEXT NOT NULL REFERENCES artifact_records(artifact_id) ON DELETE CASCADE,
            owner TEXT NOT NULL,
            until_ms BIGINT NOT NULL,
            PRIMARY KEY(artifact_id, owner)
        )",
        "CREATE INDEX IF NOT EXISTS idx_artifact_pins_expiry
            ON artifact_pins(until_ms)",
    ],
}];

mod event_store;
pub use event_store::{
    copy_quiesced_runtime_event_store, PostgresRuntimeEventStore, RuntimeEventMigrationManifest,
};
mod task_store;
pub use task_store::{copy_quiesced_task_service, PostgresTaskStore, TaskMigrationManifest};
mod artifact_store;
pub use artifact_store::PostgresArtifactRepository;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
