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
    AppendTransactionReceipt, AppendTransactionRequest, CommittedEventBatch,
    CommittedStreamRevision, DurableRuntimeEvent, ExpectedStreamRevision,
    MissionOrganizationDecision, MissionOrganizationStatus, RuntimeDecisionLeaseSnapshot,
    RuntimeEventCommitSnapshot, RuntimeEventInput, RuntimeEventRecord, RuntimeEventScope,
    RuntimeEventStore, RuntimeEventStoreBackend, RuntimeEventStoreError, RuntimeEventStoreResult,
    RuntimeEventStoreSnapshot, RuntimeEventStreamHeadSnapshot,
    RuntimeEventTransactionStreamSnapshot, RuntimeProjectionCheckpoint, RuntimeProjectionWorkClass,
    RuntimeSessionOutboxFailureClass, RuntimeSessionOutboxHealth, RuntimeSessionOutboxRecord,
    RuntimeSessionTerminalFenceAdoption, RuntimeTransactionEventInput, SessionTerminalInput,
    TaskKind, TaskMissionAssignment, TaskMissionAssignmentCommand, TaskMissionAssignmentReceipt,
    VerifiedDecisionLease,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use storage::{
    PostgresClient, PostgresConnection, PostgresConnectionConfig, PostgresExecutor,
    PostgresMigrationSpec, PostgresTransaction, SecretRefResolver, StorageError,
};

const RUNTIME_EVENT_DOMAIN: &str = "runtime_event";
const TASK_DOMAIN: &str = "runtime_task";
const ARTIFACT_DOMAIN: &str = "runtime_artifact";
const MAX_TRANSACTION_EVENTS: usize = 10_000;
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

/// Complete PostgreSQL implementation of the Runtime event backend contract.
#[derive(Clone, Debug)]
pub struct PostgresRuntimeEventStore {
    executor: PostgresExecutor,
}

/// Immutable proof written only after a domain-owned RuntimeEvent copy has
/// reached digest equality. It intentionally contains no backend URL or path.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeEventMigrationManifest {
    pub domain: String,
    pub source_digest: String,
    pub target_digest: String,
    pub commit_count: usize,
    pub event_count: usize,
    pub terminal_count: usize,
    pub decision_lease_count: usize,
}

impl PostgresRuntimeEventStore {
    pub fn new(executor: PostgresExecutor) -> RuntimeEventStoreResult<Self> {
        executor.apply_migrations(RUNTIME_EVENT_DOMAIN, RUNTIME_EVENT_MIGRATIONS)?;
        Ok(Self { executor })
    }

    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> RuntimeEventStoreResult<Self> {
        Self::new(PostgresExecutor::connect(config, resolver)?)
    }

    #[must_use]
    pub fn executor(&self) -> &PostgresExecutor {
        &self.executor
    }

    fn checkout_event_read(&self) -> Result<PostgresConnection, StorageError> {
        match RuntimeEventStore::current_projection_work_class() {
            Some(RuntimeProjectionWorkClass::Background) => self.executor.checkout_background(),
            Some(RuntimeProjectionWorkClass::Recovery) | None => {
                self.executor.checkout_online_read()
            }
        }
    }

    fn checkout_event_write(&self) -> Result<PostgresConnection, StorageError> {
        match RuntimeEventStore::current_projection_work_class() {
            Some(RuntimeProjectionWorkClass::Background) => self.executor.checkout_background(),
            Some(RuntimeProjectionWorkClass::Recovery) | None => self.executor.checkout_critical(),
        }
    }

    #[must_use]
    pub fn into_runtime_event_store(self) -> RuntimeEventStore {
        RuntimeEventStore::from_backend(Arc::new(self))
    }
}

impl RuntimeEventStoreBackend for PostgresRuntimeEventStore {
    fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String> {
        validate_event(&input).map_err(|error| error.to_string())?;
        let mut connection = self
            .checkout_event_write()
            .map_err(|error| error.to_string())?;
        let mut tx = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let stream_lock = format!("cowd-runtime-event-stream:{}", input.stream_id);
        pg(tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&stream_lock],
        ))
        .map_err(|error| error.to_string())?;
        let expected_revision =
            stream_head(&mut tx, &input.stream_id).map_err(|error| error.to_string())?;
        let request = AppendTransactionRequest {
            transaction_id: format!("runtime-tx-{}", uuid::Uuid::new_v4()),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: input.stream_id.clone(),
                expected_revision,
            }],
            events: vec![input.into()],
        };
        let receipt =
            append_transaction_in_tx(&mut tx, &request, None).map_err(|error| error.to_string())?;
        let event_id = receipt
            .event_ids
            .first()
            .ok_or_else(|| "committed runtime transaction has no event".to_string())?;
        let event = pg(tx.query_one(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE event_id=$1"),
            &[&event_id],
        ))
        .and_then(|row| row_to_event(&row))
        .map_err(|error| error.to_string())?;
        tx.commit().map_err(|error| error.to_string())?;
        Ok(event)
    }

    fn append_transaction(
        &self,
        request: AppendTransactionRequest,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let receipt = append_transaction_in_tx(&mut tx, &request, None)?;
        pg(tx.commit())?;
        Ok(receipt)
    }

    fn append_transaction_with_terminal(
        &self,
        request: AppendTransactionRequest,
        terminal: SessionTerminalInput,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let receipt = append_transaction_in_tx(&mut tx, &request, Some(&terminal))?;
        pg(tx.commit())?;
        Ok(receipt)
    }

    fn consume_verified_decision_lease(
        &self,
        lease_id: &str,
        principal_id: &str,
        review_id: &str,
        action: &str,
        scope: &str,
        evidence_digest: &str,
        credential_epoch: u64,
        consumed_at_ms: u64,
    ) -> RuntimeEventStoreResult<()> {
        validate_decision_lease_claims(
            lease_id,
            principal_id,
            review_id,
            action,
            scope,
            evidence_digest,
        )?;
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let inserted = pg(tx.execute(
            "INSERT INTO runtime_consumed_decision_leases
             (lease_id, principal_id, review_id, action, scope, evidence_digest, credential_epoch, consumed_at_ms)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(lease_id) DO NOTHING",
            &[
                &lease_id,
                &principal_id,
                &review_id,
                &action,
                &scope,
                &evidence_digest,
                &to_i64(credential_epoch, "credential_epoch")?,
                &to_i64(consumed_at_ms, "consumed_at_ms")?,
            ],
        ))?;
        if inserted == 0 {
            return Err(RuntimeEventStoreError::DecisionLeaseAlreadyConsumed {
                lease_id: lease_id.to_string(),
            });
        }
        pg(tx.commit())?;
        Ok(())
    }

    fn append_transaction_with_verified_decision_lease(
        &self,
        request: AppendTransactionRequest,
        lease: &VerifiedDecisionLease,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        validate_decision_lease_claims(
            lease.lease_id(),
            lease.principal_id(),
            lease.review_id(),
            lease.action(),
            lease.scope(),
            lease.evidence_digest(),
        )?;
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let receipt = append_transaction_in_tx(&mut tx, &request, None)?;
        let inserted = pg(tx.execute(
            "INSERT INTO runtime_consumed_decision_leases
             (lease_id, principal_id, review_id, action, scope, evidence_digest, credential_epoch, consumed_at_ms)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(lease_id) DO NOTHING",
            &[
                &lease.lease_id(),
                &lease.principal_id(),
                &lease.review_id(),
                &lease.action(),
                &lease.scope(),
                &lease.evidence_digest(),
                &to_i64(lease.credential_epoch(), "credential_epoch")?,
                &to_i64(now_ms(), "consumed_at_ms")?,
            ],
        ))?;
        if inserted == 0 {
            let existing = pg(tx.query_one(
                "SELECT principal_id, review_id, action, scope, evidence_digest, credential_epoch
                   FROM runtime_consumed_decision_leases WHERE lease_id=$1",
                &[&lease.lease_id()],
            ))?;
            let existing_epoch: i64 = pg(existing.try_get(5))?;
            let matches = pg(existing.try_get::<_, String>(0))? == lease.principal_id()
                && pg(existing.try_get::<_, String>(1))? == lease.review_id()
                && pg(existing.try_get::<_, String>(2))? == lease.action()
                && pg(existing.try_get::<_, String>(3))? == lease.scope()
                && pg(existing.try_get::<_, String>(4))? == lease.evidence_digest()
                && existing_epoch == to_i64(lease.credential_epoch(), "credential_epoch")?;
            if !receipt.duplicate || !matches {
                return Err(RuntimeEventStoreError::DecisionLeaseAlreadyConsumed {
                    lease_id: lease.lease_id().to_string(),
                });
            }
        }
        pg(tx.commit())?;
        Ok(receipt)
    }

    fn append_batch_if_revision(
        &self,
        stream_id: String,
        expected_revision: u64,
        transaction_id: String,
        events: Vec<RuntimeTransactionEventInput>,
    ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
        if events
            .iter()
            .any(|event| event.event.stream_id != stream_id)
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "single-stream batch contains an event for another stream".to_string(),
            ));
        }
        self.append_transaction(AppendTransactionRequest {
            transaction_id,
            expected_streams: vec![ExpectedStreamRevision {
                stream_id,
                expected_revision,
            }],
            events,
        })
    }

    fn events_after_cursor(
        &self,
        cursor: u64,
        max_commits: usize,
    ) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>> {
        if max_commits == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self.checkout_event_read()?;
        let rows = pg(connection.query(
            &format!(
                "WITH selected_commits AS (
                    SELECT commit_cursor FROM runtime_commits
                     WHERE commit_cursor>$1
                     ORDER BY commit_cursor ASC LIMIT $2
                 )
                 SELECT event_id, stream_id, sequence, scope, kind, status, actor, payload, refs,
                        created_at_ms, event.commit_cursor, transaction_id, transaction_index,
                        schema_version, idempotency_key
                   FROM runtime_events AS event
                   JOIN selected_commits AS selected
                     ON selected.commit_cursor=event.commit_cursor
                  ORDER BY event.commit_cursor ASC, transaction_index ASC"
            ),
            &[
                &to_i64(cursor, "cursor")?,
                &to_i64(max_commits as u64, "max_commits")?,
            ],
        ))?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            events.push(row_to_event(&row)?);
        }
        Ok(group_committed_events(events))
    }

    fn projection_checkpoint(
        &self,
        projection_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeProjectionCheckpoint>> {
        validate_projection_id(projection_id)?;
        let mut connection = self.checkout_event_read()?;
        pg(connection.query_opt(
            "SELECT projection_id, source_cursor, revision, payload, updated_at_ms
               FROM runtime_projection_checkpoints WHERE projection_id=$1",
            &[&projection_id],
        ))?
        .map(|row| row_to_projection_checkpoint(&row))
        .transpose()
    }

    fn projection_checkpoints_with_prefix(
        &self,
        prefix: &str,
    ) -> RuntimeEventStoreResult<Vec<RuntimeProjectionCheckpoint>> {
        if prefix.trim().is_empty() {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "projection checkpoint prefix must not be empty".to_string(),
            ));
        }
        let escaped = prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("{escaped}%");
        let mut connection = self.checkout_event_read()?;
        pg(connection.query(
            "SELECT projection_id, source_cursor, revision, payload, updated_at_ms
               FROM runtime_projection_checkpoints
              WHERE projection_id LIKE $1 ESCAPE '\\'
              ORDER BY projection_id ASC",
            &[&pattern],
        ))?
        .iter()
        .map(row_to_projection_checkpoint)
        .collect()
    }

    fn put_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        payload: &Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        validate_projection_id(projection_id)?;
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let lock_key = format!("cowd-runtime-projection:{projection_id}");
        pg(tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&lock_key],
        ))?;
        let current = pg(tx.query_opt(
            "SELECT projection_id, source_cursor, revision, payload, updated_at_ms
               FROM runtime_projection_checkpoints
              WHERE projection_id=$1 FOR UPDATE",
            &[&projection_id],
        ))?
        .map(|row| row_to_projection_checkpoint(&row))
        .transpose()?;
        if let Some(current) = current {
            if source_cursor < current.source_cursor {
                return Err(RuntimeEventStoreError::StaleRevision {
                    stream_id: format!("projection:{projection_id}"),
                    expected: source_cursor,
                    actual: current.source_cursor,
                });
            }
            if source_cursor == current.source_cursor {
                if current.payload == *payload {
                    return Ok(current);
                }
                return Err(RuntimeEventStoreError::TransactionConflict {
                    transaction_id: format!("projection:{projection_id}:{source_cursor}"),
                });
            }
            let row = pg(tx.query_one(
                "UPDATE runtime_projection_checkpoints
                    SET source_cursor=$1, revision=revision+1, payload=$2, updated_at_ms=$3
                  WHERE projection_id=$4
              RETURNING projection_id, source_cursor, revision, payload, updated_at_ms",
                &[
                    &to_i64(source_cursor, "source_cursor")?,
                    payload,
                    &to_i64(updated_at_ms, "updated_at_ms")?,
                    &projection_id,
                ],
            ))?;
            let checkpoint = row_to_projection_checkpoint(&row)?;
            pg(tx.commit())?;
            return Ok(checkpoint);
        }
        let row = pg(tx.query_one(
            "INSERT INTO runtime_projection_checkpoints
                (projection_id, source_cursor, revision, payload, updated_at_ms)
             VALUES ($1,$2,1,$3,$4)
             RETURNING projection_id, source_cursor, revision, payload, updated_at_ms",
            &[
                &projection_id,
                &to_i64(source_cursor, "source_cursor")?,
                payload,
                &to_i64(updated_at_ms, "updated_at_ms")?,
            ],
        ))?;
        let checkpoint = row_to_projection_checkpoint(&row)?;
        pg(tx.commit())?;
        Ok(checkpoint)
    }

    fn compare_and_put_projection_checkpoint(
        &self,
        projection_id: &str,
        source_cursor: u64,
        expected_revision: u64,
        payload: &Value,
        updated_at_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
        validate_projection_id(projection_id)?;
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let lock_key = format!("cowd-runtime-projection:{projection_id}");
        pg(tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&lock_key],
        ))?;
        let current = pg(tx.query_opt(
            "SELECT projection_id, source_cursor, revision, payload, updated_at_ms
               FROM runtime_projection_checkpoints
              WHERE projection_id=$1 FOR UPDATE",
            &[&projection_id],
        ))?
        .map(|row| row_to_projection_checkpoint(&row))
        .transpose()?;
        let checkpoint = match current {
            Some(current) => {
                if current.revision != expected_revision {
                    return Err(RuntimeEventStoreError::StaleRevision {
                        stream_id: format!("projection:{projection_id}"),
                        expected: expected_revision,
                        actual: current.revision,
                    });
                }
                if source_cursor < current.source_cursor {
                    return Err(RuntimeEventStoreError::StaleRevision {
                        stream_id: format!("projection-source:{projection_id}"),
                        expected: source_cursor,
                        actual: current.source_cursor,
                    });
                }
                if source_cursor == current.source_cursor && current.payload == *payload {
                    return Ok(current);
                }
                let row = pg(tx.query_one(
                    "UPDATE runtime_projection_checkpoints
                        SET source_cursor=$1, revision=revision+1, payload=$2, updated_at_ms=$3
                      WHERE projection_id=$4 AND revision=$5
                  RETURNING projection_id, source_cursor, revision, payload, updated_at_ms",
                    &[
                        &to_i64(source_cursor, "source_cursor")?,
                        payload,
                        &to_i64(updated_at_ms, "updated_at_ms")?,
                        &projection_id,
                        &to_i64(expected_revision, "expected_revision")?,
                    ],
                ))?;
                row_to_projection_checkpoint(&row)?
            }
            None => {
                if expected_revision != 0 {
                    return Err(RuntimeEventStoreError::StaleRevision {
                        stream_id: format!("projection:{projection_id}"),
                        expected: expected_revision,
                        actual: 0,
                    });
                }
                let row = pg(tx.query_one(
                    "INSERT INTO runtime_projection_checkpoints
                        (projection_id, source_cursor, revision, payload, updated_at_ms)
                     VALUES ($1,$2,1,$3,$4)
                     RETURNING projection_id, source_cursor, revision, payload, updated_at_ms",
                    &[
                        &projection_id,
                        &to_i64(source_cursor, "source_cursor")?,
                        payload,
                        &to_i64(updated_at_ms, "updated_at_ms")?,
                    ],
                ))?;
                row_to_projection_checkpoint(&row)?
            }
        };
        pg(tx.commit())?;
        Ok(checkpoint)
    }

    fn delete_projection_checkpoint(&self, projection_id: &str) -> RuntimeEventStoreResult<bool> {
        validate_projection_id(projection_id)?;
        let mut connection = self.checkout_event_write()?;
        Ok(pg(connection.execute(
            "DELETE FROM runtime_projection_checkpoints WHERE projection_id=$1",
            &[&projection_id],
        ))? > 0)
    }

    fn event_by_idempotency_key(
        &self,
        stream_id: &str,
        idempotency_key: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>> {
        let mut connection = self.checkout_event_read()?;
        pg(connection.query_opt(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 AND idempotency_key=$2"),
            &[&stream_id, &idempotency_key],
        ))?
        .map(|row| row_to_event(&row))
        .transpose()
    }

    fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64> {
        let mut connection = self.checkout_event_read()?;
        stream_head(&mut connection, stream_id)
    }

    fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        pg(connection.query(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 ORDER BY sequence ASC"),
            &[&stream_id],
        ))
        .and_then(rows_to_events)
        .map_err(|error| error.to_string())
    }

    fn list_stream_page_desc(
        &self,
        stream_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
        let offset = to_i64(offset as u64, "offset").map_err(|error| error.to_string())?;
        pg(connection.query(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 ORDER BY sequence DESC LIMIT $2 OFFSET $3"),
            &[&stream_id, &limit, &offset],
        ))
        .and_then(rows_to_events)
        .map_err(|error| error.to_string())
    }

    fn stream_event_count(&self, stream_id: &str) -> Result<usize, String> {
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let count: i64 = pg(connection.query_one(
            "SELECT COUNT(*) FROM runtime_events WHERE stream_id=$1",
            &[&stream_id],
        ))
        .and_then(|row| pg(row.try_get(0)))
        .map_err(|error| error.to_string())?;
        usize::try_from(count).map_err(|_| "runtime stream event count overflow".to_string())
    }

    fn execution_events_for_session(
        &self,
        session_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if session_id.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let ref_filter = serde_json::json!([{"kind": "session", "id": session_id}]);
        let direct_refs = {
            let mut connection = self
                .checkout_event_read()
                .map_err(|error| error.to_string())?;
            let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
            let rows = match after_position {
                Some((cursor, transaction_index)) => pg(connection.query(
                    &format!(
                        "SELECT {EVENT_COLUMNS} FROM runtime_events
                         WHERE refs @> $1
                           AND (commit_cursor > $2
                                OR (commit_cursor = $2 AND transaction_index > $3))
                         ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $4"
                    ),
                    &[
                        &ref_filter,
                        &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                        &i64::from(transaction_index),
                        &limit,
                    ],
                )),
                None => pg(connection.query(
                    &format!(
                        "SELECT {EVENT_COLUMNS} FROM runtime_events
                         WHERE refs @> $1
                         ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $2"
                    ),
                    &[&ref_filter, &limit],
                )),
            }
            .and_then(rows_to_events)
            .map_err(|error| error.to_string())?;
            rows
        };
        let terminal_requests = {
            let mut connection = self
                .checkout_event_read()
                .map_err(|error| error.to_string())?;
            let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
            let event_kind = "runtime.session.terminal_requested";
            let rows = match after_position {
                Some((cursor, transaction_index)) => pg(connection.query(
                    &format!(
                        "SELECT {EVENT_COLUMNS} FROM runtime_events
                         WHERE refs @> $1 AND kind = $2
                           AND (commit_cursor > $3
                                OR (commit_cursor = $3 AND transaction_index > $4))
                         ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $5"
                    ),
                    &[
                        &ref_filter,
                        &event_kind,
                        &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                        &i64::from(transaction_index),
                        &limit,
                    ],
                )),
                None => pg(connection.query(
                    &format!(
                        "SELECT {EVENT_COLUMNS} FROM runtime_events
                         WHERE refs @> $1 AND kind = $2
                         ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $3"
                    ),
                    &[&ref_filter, &event_kind, &limit],
                )),
            };
            rows.and_then(rows_to_events)
                .map_err(|error| error.to_string())?
        };
        let graph_ids = terminal_requests
            .iter()
            .flat_map(|event| event.refs.iter())
            .filter(|reference| reference.kind == "execution_graph")
            .map(|reference| reference.id.clone())
            .collect::<BTreeSet<_>>();
        let mut related = direct_refs;
        related.extend(terminal_requests);
        let mut pending = graph_ids.into_iter().collect::<VecDeque<_>>();
        let mut visited = BTreeSet::new();
        while let Some(graph_id) = pending.pop_front() {
            if visited.len() >= limit || !visited.insert(graph_id.clone()) {
                continue;
            }
            related.extend(RuntimeEventStoreBackend::list_stream(self, &graph_id)?);
            let lineage_stream = format!("execution-lineage:{graph_id}");
            let lineage_events = RuntimeEventStoreBackend::list_stream(self, &lineage_stream)?;
            for event in &lineage_events {
                if event.kind != "execution.lineage.child_registered.v1" {
                    continue;
                }
                if let Some(child_id) = event
                    .payload
                    .get("child_execution_id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                {
                    pending.push_back(child_id.to_string());
                }
            }
            related.extend(lineage_events);
        }
        related.sort_by_key(|event| (event.commit_cursor, event.transaction_index));
        related.dedup_by(|left, right| left.event_id == right.event_id);
        Ok(related
            .into_iter()
            .filter(|event| {
                after_position.is_none_or(|position| {
                    (event.commit_cursor, event.transaction_index) > position
                })
            })
            .take(limit)
            .collect())
    }

    fn events_for_root_execution(
        &self,
        root_execution_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if root_execution_id.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
        let rows = match after_position {
            Some((cursor, transaction_index)) => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE root_execution_id=$1
                       AND (commit_cursor > $2
                            OR (commit_cursor = $2 AND transaction_index > $3))
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $4"
                ),
                &[
                    &root_execution_id,
                    &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                    &i64::from(transaction_index),
                    &limit,
                ],
            )),
            None => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE root_execution_id=$1
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $2"
                ),
                &[&root_execution_id, &limit],
            )),
        };
        rows.and_then(rows_to_events)
            .map_err(|error| error.to_string())
    }

    fn events_for_root_execution_kind(
        &self,
        root_execution_id: &str,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if root_execution_id.trim().is_empty() || kind.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
        let rows = match after_position {
            Some((cursor, transaction_index)) => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE root_execution_id=$1 AND kind=$2
                       AND (commit_cursor > $3
                            OR (commit_cursor = $3 AND transaction_index > $4))
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $5"
                ),
                &[
                    &root_execution_id,
                    &kind,
                    &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                    &i64::from(transaction_index),
                    &limit,
                ],
            )),
            None => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE root_execution_id=$1 AND kind=$2
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $3"
                ),
                &[&root_execution_id, &kind, &limit],
            )),
        };
        rows.and_then(rows_to_events)
            .map_err(|error| error.to_string())
    }

    fn events_for_activity(
        &self,
        activity_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if activity_id.trim().is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
        let rows = match after_position {
            Some((cursor, transaction_index)) => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE activity_id=$1
                       AND (commit_cursor > $2
                            OR (commit_cursor = $2 AND transaction_index > $3))
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $4"
                ),
                &[
                    &activity_id,
                    &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                    &i64::from(transaction_index),
                    &limit,
                ],
            )),
            None => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE activity_id=$1
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $2"
                ),
                &[&activity_id, &limit],
            )),
        };
        rows.and_then(rows_to_events)
            .map_err(|error| error.to_string())
    }

    fn list_scope(
        &self,
        scope: RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        pg(connection.query(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE scope=$1 ORDER BY commit_cursor DESC, transaction_index DESC LIMIT $2"),
            &[&scope.as_str(), &to_i64(limit as u64, "limit").map_err(|error| error.to_string())?],
        ))
        .and_then(rows_to_events)
        .map_err(|error| error.to_string())
    }

    fn list_scope_page_asc(
        &self,
        scope: RuntimeEventScope,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
        let rows = match after_position {
            Some((cursor, transaction_index)) => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE scope=$1
                       AND (commit_cursor > $2
                            OR (commit_cursor = $2 AND transaction_index > $3))
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $4"
                ),
                &[
                    &scope.as_str(),
                    &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                    &i64::from(transaction_index),
                    &limit,
                ],
            )),
            None => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events WHERE scope=$1
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $2"
                ),
                &[&scope.as_str(), &limit],
            )),
        };
        rows.and_then(rows_to_events)
            .map_err(|error| error.to_string())
    }

    fn list_scope_stream_prefix_page_asc(
        &self,
        scope: RuntimeEventScope,
        stream_prefix: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if stream_prefix.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
        let rows = match after_position {
            Some((cursor, transaction_index)) => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE scope=$1 AND starts_with(stream_id, $2)
                       AND (commit_cursor > $3
                            OR (commit_cursor = $3 AND transaction_index > $4))
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $5"
                ),
                &[
                    &scope.as_str(),
                    &stream_prefix,
                    &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                    &i64::from(transaction_index),
                    &limit,
                ],
            )),
            None => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE scope=$1 AND starts_with(stream_id, $2)
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $3"
                ),
                &[&scope.as_str(), &stream_prefix, &limit],
            )),
        };
        rows.and_then(rows_to_events)
            .map_err(|error| error.to_string())
    }

    fn list_scope_kind_page_asc(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<DurableRuntimeEvent>, String> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        let limit = to_i64(limit as u64, "limit").map_err(|error| error.to_string())?;
        let rows = match after_position {
            Some((cursor, transaction_index)) => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE scope=$1 AND kind=$2
                       AND (commit_cursor > $3
                            OR (commit_cursor = $3 AND transaction_index > $4))
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $5"
                ),
                &[
                    &scope.as_str(),
                    &kind,
                    &to_i64(cursor, "after cursor").map_err(|error| error.to_string())?,
                    &i64::from(transaction_index),
                    &limit,
                ],
            )),
            None => pg(connection.query(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM runtime_events
                     WHERE scope=$1 AND kind=$2
                     ORDER BY commit_cursor ASC, transaction_index ASC LIMIT $3"
                ),
                &[&scope.as_str(), &kind, &limit],
            )),
        };
        rows.and_then(rows_to_events)
            .map_err(|error| error.to_string())
    }

    fn stream_ids_for_scope(
        &self,
        scope: RuntimeEventScope,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        let mut connection = self.checkout_event_read()?;
        let rows = pg(connection.query(
            "SELECT stream_id FROM runtime_events WHERE scope=$1
             GROUP BY stream_id ORDER BY MAX(commit_cursor) ASC, stream_id ASC",
            &[&scope.as_str()],
        ))?;
        rows.into_iter().map(|row| pg(row.try_get(0))).collect()
    }

    fn stream_ids_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<String>> {
        let sequence = to_i64(sequence, "runtime event sequence")?;
        let mut connection = self.checkout_event_read()?;
        let rows = pg(connection.query(
            "SELECT stream_id FROM runtime_events
             WHERE scope=$1 AND kind=$2 AND sequence=$3
             ORDER BY commit_cursor ASC, stream_id ASC",
            &[&scope.as_str(), &kind, &sequence],
        ))?;
        rows.into_iter().map(|row| pg(row.try_get(0))).collect()
    }

    fn latest_stream_statuses_for_scope_kind_at_sequence(
        &self,
        scope: RuntimeEventScope,
        kind: &str,
        sequence: u64,
    ) -> RuntimeEventStoreResult<Vec<(String, Option<String>)>> {
        let sequence = to_i64(sequence, "runtime event sequence")?;
        let mut connection = self.checkout_event_read()?;
        let rows = pg(connection.query(
            "WITH candidates AS (
                 SELECT stream_id FROM runtime_events
                  WHERE scope=$1 AND kind=$2 AND sequence=$3
             )
             SELECT stream_id, status
               FROM (
                   SELECT DISTINCT ON (event.stream_id)
                          event.stream_id, event.status, event.sequence
                     FROM runtime_events AS event
                     JOIN candidates USING(stream_id)
                    ORDER BY event.stream_id, event.sequence DESC
               ) AS latest
              ORDER BY stream_id ASC",
            &[&scope.as_str(), &kind, &sequence],
        ))?;
        rows.into_iter()
            .map(|row| Ok((pg(row.try_get(0))?, pg(row.try_get(1))?)))
            .collect()
    }

    fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        pg(connection.query(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events ORDER BY commit_cursor DESC, transaction_index DESC LIMIT $1"),
            &[&to_i64(limit as u64, "limit").map_err(|error| error.to_string())?],
        ))
        .and_then(rows_to_events)
        .map_err(|error| error.to_string())
    }

    fn latest_for_stream(&self, stream_id: &str) -> Result<Option<DurableRuntimeEvent>, String> {
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        pg(connection.query_opt(
            &format!("SELECT {EVENT_COLUMNS} FROM runtime_events WHERE stream_id=$1 ORDER BY sequence DESC LIMIT 1"),
            &[&stream_id],
        ))
        .and_then(|row| row.map(|row| row_to_event(&row)).transpose())
        .map_err(|error| error.to_string())
    }

    fn latest_for_stream_kind(
        &self,
        stream_id: &str,
        kind: &str,
    ) -> Result<Option<DurableRuntimeEvent>, String> {
        let mut connection = self
            .checkout_event_read()
            .map_err(|error| error.to_string())?;
        pg(connection.query_opt(
            &format!(
                "SELECT {EVENT_COLUMNS} FROM runtime_events
                  WHERE stream_id=$1 AND kind=$2
                  ORDER BY sequence DESC LIMIT 1"
            ),
            &[&stream_id, &kind],
        ))
        .and_then(|row| row.map(|row| row_to_event(&row)).transpose())
        .map_err(|error| error.to_string())
    }

    fn enqueue_session_terminal(
        &self,
        terminal_id: &str,
        message_id: &str,
        session_id: &str,
        commit_cursor: u64,
        payload_ref: &str,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        let _ = (
            terminal_id,
            message_id,
            session_id,
            commit_cursor,
            payload_ref,
        );
        Err(RuntimeEventStoreError::InvalidTransaction(
            "unfenced terminal enqueue is test-only; use append_transaction_with_terminal"
                .to_string(),
        ))
    }

    fn claim_session_terminals(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let expires = now_ms.saturating_add(lease_ms);
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let rows = pg(tx.query(
            "WITH candidates AS (
                SELECT terminal_id FROM runtime_session_outbox
                 WHERE ((status IN ('pending','retry_scheduled') AND COALESCE(next_attempt_at, 0) <= $1)
                    OR (status='claimed' AND claim_expires_at <= $1))
                 ORDER BY commit_cursor, terminal_id
                 FOR UPDATE SKIP LOCKED LIMIT $2
             ), claimed AS (
                UPDATE runtime_session_outbox outbox
                   SET status='claimed', attempts=outbox.attempts+1,
                       claim_owner=$3, claim_expires_at=$4, revision=outbox.revision+1
                  FROM candidates
                 WHERE outbox.terminal_id=candidates.terminal_id
                RETURNING outbox.terminal_id, outbox.message_id, outbox.session_id,
                    outbox.commit_cursor, outbox.payload_ref, outbox.execution_id, outbox.turn_id,
                    outbox.request_id, outbox.session_generation, outbox.input_sequence, outbox.input_claim_owner,
                    outbox.input_claim_token, outbox.input_claim_revision,
                    outbox.status, outbox.attempts, outbox.next_attempt_at, outbox.claim_owner,
                    outbox.claim_expires_at, outbox.failure_class, outbox.last_error,
                    outbox.materialized_at, outbox.revision
             ) SELECT * FROM claimed ORDER BY commit_cursor, terminal_id",
            &[
                &to_i64(now_ms, "now_ms")?,
                &to_i64(limit as u64, "limit")?,
                &worker_id,
                &to_i64(expires, "claim_expires_at")?,
            ],
        ))?;
        let records = rows
            .iter()
            .map(row_to_runtime_session_outbox)
            .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
        pg(tx.commit())?;
        Ok(records)
    }

    fn session_terminal(
        &self,
        terminal_id: &str,
    ) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
        let mut connection = self.checkout_event_read()?;
        query_runtime_session_outbox(&mut connection, terminal_id)
    }

    fn has_unsettled_session_terminals(&self, session_id: &str) -> RuntimeEventStoreResult<bool> {
        let mut connection = self.checkout_event_read()?;
        let row = pg(connection.query_one(
            "SELECT EXISTS(
                 SELECT 1 FROM runtime_session_outbox
                  WHERE session_id=$1 AND status!='materialized'
             )",
            &[&session_id],
        ))?;
        pg(row.try_get(0))
    }

    fn materialized_session_terminals_after(
        &self,
        session_id: &str,
        after_commit_cursor: u64,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        let mut connection = self.checkout_event_read()?;
        let rows = pg(connection.query(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                    request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                    input_claim_revision, status,
                    attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                    last_error, materialized_at, revision
               FROM runtime_session_outbox WHERE session_id=$1 AND status='materialized'
                 AND commit_cursor>$2 ORDER BY commit_cursor, terminal_id LIMIT $3",
            &[
                &session_id,
                &to_i64(after_commit_cursor, "after_commit_cursor")?,
                &to_i64(limit.clamp(1, 500) as u64, "limit")?,
            ],
        ))?;
        rows.iter().map(row_to_runtime_session_outbox).collect()
    }

    fn session_terminal_health(&self) -> RuntimeEventStoreResult<RuntimeSessionOutboxHealth> {
        let mut connection = self.checkout_event_read()?;
        let rows = pg(connection.query(
            "SELECT status, COUNT(*) FROM runtime_session_outbox GROUP BY status",
            &[],
        ))?;
        let mut health = RuntimeSessionOutboxHealth::default();
        for row in rows {
            let status: String = pg(row.try_get(0))?;
            let count = from_i64(pg(row.try_get(1))?, "outbox count")?;
            match status.as_str() {
                "pending" => health.pending = count,
                "claimed" => health.claimed = count,
                "retry_scheduled" => health.retry_scheduled = count,
                "materialized" => health.materialized = count,
                "blocked" => health.blocked = count,
                _ => {}
            }
        }
        Ok(health)
    }

    fn blocked_session_terminals(
        &self,
        limit: usize,
    ) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>> {
        let mut connection = self.checkout_event_read()?;
        let rows = pg(connection.query(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                    request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                    input_claim_revision, status,
                    attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                    last_error, materialized_at, revision
               FROM runtime_session_outbox WHERE status='blocked'
               ORDER BY COALESCE(next_attempt_at, 0), commit_cursor, terminal_id LIMIT $1",
            &[&to_i64(limit.clamp(1, 500) as u64, "limit")?],
        ))?;
        rows.iter().map(row_to_runtime_session_outbox).collect()
    }

    fn retry_session_terminal(
        &self,
        terminal_id: &str,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "manual terminal retry requires actor and reason".to_string(),
            ));
        }
        let mut connection = self.checkout_event_write()?;
        let changed = pg(connection.execute(
            "UPDATE runtime_session_outbox SET status='retry_scheduled', next_attempt_at=$1,
             claim_owner=NULL, claim_expires_at=NULL, failure_class=NULL, last_error=$2,
             revision=revision+1 WHERE terminal_id=$3 AND status='blocked'",
            &[
                &to_i64(now_ms, "now_ms")?,
                &format!("manual retry by {actor}: {reason}"),
                &terminal_id,
            ],
        ))?;
        if changed != 1 {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{terminal_id}` is not blocked"
            )));
        }
        query_runtime_session_outbox(&mut connection, terminal_id)?.ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!("terminal `{terminal_id}` vanished"))
        })
    }

    fn ack_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        self.transition_session_terminal(
            terminal_id,
            worker_id,
            expected_revision,
            "materialized",
            None,
            None,
            now_ms,
        )
    }

    fn adopt_session_terminal_fence(
        &self,
        request: &RuntimeSessionTerminalFenceAdoption,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        if request.terminal_id.trim().is_empty()
            || request.request_id.trim().is_empty()
            || request.session_id.trim().is_empty()
            || request.turn_id.trim().is_empty()
            || request.claim_owner.trim().is_empty()
            || request.claim_token.trim().is_empty()
            || request.session_generation == 0
            || request.claim_revision == 0
            || request.claim_expires_at_ms <= request.adopted_at_ms
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "terminal fence adoption requires live terminal, request, session, turn and claim identities"
                    .to_string(),
            ));
        }
        let mut connection = self.checkout_event_write()?;
        let mut tx = pg(connection.transaction())?;
        let current = pg(tx.query_opt(
            "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                    request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                    input_claim_revision, status, attempts, next_attempt_at, claim_owner,
                    claim_expires_at, failure_class, last_error, materialized_at, revision
               FROM runtime_session_outbox WHERE terminal_id=$1 FOR UPDATE",
            &[&request.terminal_id],
        ))?
        .map(|row| row_to_runtime_session_outbox(&row))
        .transpose()?
        .ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!(
                "terminal `{}` is missing",
                request.terminal_id
            ))
        })?;
        if current.request_id.as_deref() != Some(request.request_id.as_str())
            || current.session_id != request.session_id
            || current.turn_id.as_deref() != Some(request.turn_id.as_str())
            || current.session_generation != Some(request.session_generation)
            || current.input_sequence != Some(request.input_sequence)
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{}` identity does not match the current Session claim",
                request.terminal_id
            )));
        }
        let already_adopted = current.input_claim_owner.as_deref()
            == Some(request.claim_owner.as_str())
            && current.input_claim_token.as_deref() == Some(request.claim_token.as_str())
            && current.input_claim_revision == Some(request.claim_revision)
            && current.input_sequence == Some(request.input_sequence);
        if already_adopted {
            return Ok(current);
        }
        if current.revision != request.expected_terminal_revision {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: format!("session-terminal:{}", request.terminal_id),
                expected: request.expected_terminal_revision,
                actual: current.revision,
            });
        }
        if current.status == "materialized" {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "materialized terminal `{}` cannot adopt a different Session claim",
                request.terminal_id
            )));
        }
        if !matches!(
            current.status.as_str(),
            "pending" | "retry_scheduled" | "blocked" | "claimed"
        ) {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{}` in state `{}` cannot adopt a Session claim",
                request.terminal_id, current.status
            )));
        }
        if current
            .input_claim_revision
            .is_some_and(|revision| request.claim_revision <= revision)
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{}` cannot regress Session claim revision",
                request.terminal_id
            )));
        }
        if current.status == "claimed"
            && !current
                .claim_expires_at_ms
                .is_some_and(|expires| expires <= request.adopted_at_ms)
        {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "terminal `{}` has an active delivery claim",
                request.terminal_id
            )));
        }
        let changed = pg(tx.execute(
            "UPDATE runtime_session_outbox
                SET input_sequence=$1, input_claim_owner=$2, input_claim_token=$3, input_claim_revision=$4,
                    status='pending', next_attempt_at=0, claim_owner=NULL,
                    claim_expires_at=NULL, failure_class=NULL, last_error=NULL,
                    materialized_at=NULL, revision=revision+1
              WHERE terminal_id=$5 AND revision=$6",
            &[
                &to_i64(request.input_sequence, "input_sequence")?,
                &request.claim_owner,
                &request.claim_token,
                &to_i64(request.claim_revision, "claim_revision")?,
                &request.terminal_id,
                &to_i64(
                    request.expected_terminal_revision,
                    "expected_terminal_revision",
                )?,
            ],
        ))?;
        if changed != 1 {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: format!("session-terminal:{}", request.terminal_id),
                expected: request.expected_terminal_revision,
                actual: current.revision,
            });
        }
        let adopted =
            query_runtime_session_outbox(&mut tx, &request.terminal_id)?.ok_or_else(|| {
                RuntimeEventStoreError::Corrupt(format!(
                    "terminal `{}` vanished after fence adoption",
                    request.terminal_id
                ))
            })?;
        pg(tx.commit())?;
        Ok(adopted)
    }

    fn fail_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        class: RuntimeSessionOutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        let current = self.session_terminal(terminal_id)?.ok_or_else(|| {
            RuntimeEventStoreError::Corrupt(format!("terminal `{terminal_id}` missing"))
        })?;
        let retry = class == RuntimeSessionOutboxFailureClass::Retryable
            && current.attempts < max_attempts.max(1);
        self.transition_session_terminal(
            terminal_id,
            worker_id,
            expected_revision,
            if retry { "retry_scheduled" } else { "blocked" },
            Some((failure_class_as_str(class), error)),
            retry.then_some(retry_at_ms),
            now_ms,
        )
    }

    fn export_migration_snapshot(&self) -> RuntimeEventStoreResult<RuntimeEventStoreSnapshot> {
        let mut connection = self.executor.checkout_background()?;
        export_postgres_migration_snapshot(&mut connection)
    }

    fn import_migration_snapshot(
        &self,
        snapshot: &RuntimeEventStoreSnapshot,
    ) -> RuntimeEventStoreResult<()> {
        let mut connection = self.executor.checkout_background()?;
        import_postgres_migration_snapshot(&mut connection, snapshot)
    }
}

impl PostgresRuntimeEventStore {
    #[allow(clippy::too_many_arguments)]
    fn transition_session_terminal(
        &self,
        terminal_id: &str,
        worker_id: &str,
        expected_revision: u64,
        status: &str,
        failure: Option<(&str, &str)>,
        retry_at_ms: Option<u64>,
        now_ms: u64,
    ) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
        let mut connection = self.executor.checkout_critical()?;
        let (failure_class, last_error) = failure.unzip();
        let row = pg(connection.query_opt(
            "UPDATE runtime_session_outbox SET status=$1, next_attempt_at=$2,
             claim_owner=NULL, claim_expires_at=NULL, failure_class=$3, last_error=$4,
             materialized_at=CASE WHEN $1='materialized' THEN $5 ELSE materialized_at END,
             revision=revision+1 WHERE terminal_id=$6 AND status='claimed'
             AND claim_owner=$7 AND revision=$8
              RETURNING terminal_id, message_id, session_id, commit_cursor, payload_ref,
                 execution_id, turn_id, request_id, session_generation, input_sequence, input_claim_owner,
                 input_claim_token, input_claim_revision, status, attempts, next_attempt_at, claim_owner,
                 claim_expires_at, failure_class, last_error, materialized_at, revision",
            &[
                &status,
                &retry_at_ms
                    .map(|value| to_i64(value, "retry_at_ms"))
                    .transpose()?,
                &failure_class,
                &last_error,
                &to_i64(now_ms, "now_ms")?,
                &terminal_id,
                &worker_id,
                &to_i64(expected_revision, "expected_revision")?,
            ],
        ))?;
        if let Some(row) = row {
            return row_to_runtime_session_outbox(&row);
        }
        let actual = query_runtime_session_outbox(&mut connection, terminal_id)?
            .map_or(0, |record| record.revision);
        Err(RuntimeEventStoreError::StaleRevision {
            stream_id: format!("terminal:{terminal_id}"),
            expected: expected_revision,
            actual,
        })
    }
}

/// Copy a quiesced RuntimeEvent ledger exactly once, prove canonical digest
/// equality, then atomically write a backend-neutral cutover manifest.
pub fn copy_quiesced_runtime_event_store(
    source: &RuntimeEventStore,
    target: &RuntimeEventStore,
    manifest_path: impl AsRef<Path>,
) -> RuntimeEventStoreResult<RuntimeEventMigrationManifest> {
    let snapshot = source.export_migration_snapshot()?;
    snapshot.validate()?;
    let source_digest = snapshot.canonical_digest()?;
    target.import_migration_snapshot(&snapshot)?;
    let target_snapshot = target.export_migration_snapshot()?;
    let target_digest = target_snapshot.canonical_digest()?;
    if source_digest != target_digest {
        return Err(RuntimeEventStoreError::Corrupt(
            "runtime event migration digest mismatch".to_string(),
        ));
    }
    let manifest = RuntimeEventMigrationManifest {
        domain: RUNTIME_EVENT_DOMAIN.to_string(),
        source_digest,
        target_digest,
        commit_count: snapshot.commits.len(),
        event_count: snapshot.events.len(),
        terminal_count: snapshot.session_outbox.len(),
        decision_lease_count: snapshot.decision_leases.len(),
    };
    write_migration_manifest(manifest_path.as_ref(), &manifest)?;
    Ok(manifest)
}

fn write_migration_manifest(
    manifest_path: &Path,
    manifest: &RuntimeEventMigrationManifest,
) -> RuntimeEventStoreResult<()> {
    if let Some(parent) = manifest_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary_path = PathBuf::from(format!(
        "{}.{}.tmp",
        manifest_path.display(),
        uuid::Uuid::new_v4()
    ));
    fs::write(&temporary_path, serde_json::to_vec_pretty(manifest)?)?;
    fs::rename(temporary_path, manifest_path)?;
    Ok(())
}

fn export_postgres_migration_snapshot(
    connection: &mut PostgresConnection,
) -> RuntimeEventStoreResult<RuntimeEventStoreSnapshot> {
    let commits = pg(connection.query(
        "SELECT commit_cursor, transaction_id, request_hash, created_at_ms
           FROM runtime_commits ORDER BY commit_cursor ASC",
        &[],
    ))?
    .iter()
    .map(|row| {
        Ok(RuntimeEventCommitSnapshot {
            commit_cursor: from_i64(pg(row.try_get(0))?, "commit_cursor")?,
            transaction_id: pg(row.try_get(1))?,
            request_hash: pg(row.try_get(2))?,
            created_at_ms: from_i64(pg(row.try_get(3))?, "created_at_ms")?,
        })
    })
    .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
    let events = rows_to_events(pg(connection.query(
        &format!("SELECT {EVENT_COLUMNS} FROM runtime_events ORDER BY commit_cursor ASC, transaction_index ASC"),
        &[],
    ))?)?;
    let transaction_streams = pg(connection.query(
        "SELECT transaction_id, stream_id, expected_revision, committed_revision
           FROM runtime_transaction_streams ORDER BY transaction_id ASC, stream_id ASC",
        &[],
    ))?
    .iter()
    .map(|row| {
        Ok(RuntimeEventTransactionStreamSnapshot {
            transaction_id: pg(row.try_get(0))?,
            stream_id: pg(row.try_get(1))?,
            expected_revision: from_i64(pg(row.try_get(2))?, "expected_revision")?,
            committed_revision: from_i64(pg(row.try_get(3))?, "committed_revision")?,
        })
    })
    .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
    let stream_heads = pg(connection.query(
        "SELECT stream_id, revision FROM runtime_stream_heads ORDER BY stream_id ASC",
        &[],
    ))?
    .iter()
    .map(|row| {
        Ok(RuntimeEventStreamHeadSnapshot {
            stream_id: pg(row.try_get(0))?,
            revision: from_i64(pg(row.try_get(1))?, "revision")?,
        })
    })
    .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
    let session_outbox = pg(connection.query(
        "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                input_claim_revision, status,
                attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                last_error, materialized_at, revision
           FROM runtime_session_outbox ORDER BY terminal_id ASC",
        &[],
    ))?
    .iter()
    .map(row_to_runtime_session_outbox)
    .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
    let decision_leases = pg(connection.query(
        "SELECT lease_id, principal_id, review_id, action, scope, evidence_digest,
                credential_epoch, consumed_at_ms
           FROM runtime_consumed_decision_leases ORDER BY lease_id ASC",
        &[],
    ))?
    .iter()
    .map(|row| {
        Ok(RuntimeDecisionLeaseSnapshot {
            lease_id: pg(row.try_get(0))?,
            principal_id: pg(row.try_get(1))?,
            review_id: pg(row.try_get(2))?,
            action: pg(row.try_get(3))?,
            scope: pg(row.try_get(4))?,
            evidence_digest: pg(row.try_get(5))?,
            credential_epoch: from_i64(pg(row.try_get(6))?, "credential_epoch")?,
            consumed_at_ms: from_i64(pg(row.try_get(7))?, "consumed_at_ms")?,
        })
    })
    .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
    let snapshot = RuntimeEventStoreSnapshot {
        commits,
        events,
        transaction_streams,
        stream_heads,
        session_outbox,
        decision_leases,
    };
    snapshot.validate()?;
    Ok(snapshot)
}

fn import_postgres_migration_snapshot(
    connection: &mut PostgresConnection,
    snapshot: &RuntimeEventStoreSnapshot,
) -> RuntimeEventStoreResult<()> {
    snapshot.validate()?;
    let snapshot = canonical_snapshot(snapshot);
    let mut tx = pg(connection.transaction())?;
    for table in [
        "runtime_commits",
        "runtime_events",
        "runtime_transaction_streams",
        "runtime_stream_heads",
        "runtime_session_outbox",
        "runtime_consumed_decision_leases",
    ] {
        let row = pg(tx.query_one(&format!("SELECT COUNT(*) FROM {table}"), &[]))?;
        let count = from_i64(pg(row.try_get(0))?, "target row count")?;
        if count != 0 {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "runtime event migration target table `{table}` is not empty"
            )));
        }
    }
    for commit in &snapshot.commits {
        pg(tx.execute(
            "INSERT INTO runtime_commits(commit_cursor, transaction_id, request_hash, created_at_ms)
             VALUES ($1,$2,$3,$4)",
            &[
                &to_i64(commit.commit_cursor, "commit_cursor")?,
                &commit.transaction_id,
                &commit.request_hash,
                &to_i64(commit.created_at_ms, "created_at_ms")?,
            ],
        ))?;
    }
    for event in &snapshot.events {
        let refs = serde_json::to_value(&event.refs)?;
        let activity_binding = event.activity_binding();
        let root_execution_id = activity_binding
            .as_ref()
            .map(|binding| binding.root_execution_id.as_str());
        let activity_id = activity_binding
            .as_ref()
            .map(|binding| binding.activity_id.as_str());
        pg(tx.execute(
            "INSERT INTO runtime_events (event_id, stream_id, sequence, scope, kind, status, actor,
                payload, refs, created_at_ms, commit_cursor, transaction_id, transaction_index,
                schema_version, idempotency_key, root_execution_id, activity_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
            &[
                &event.event_id,
                &event.stream_id,
                &to_i64(event.sequence, "sequence")?,
                &event.scope.as_str(),
                &event.kind,
                &event.status,
                &event.actor,
                &event.payload,
                &refs,
                &to_i64(event.created_at_ms, "created_at_ms")?,
                &to_i64(event.commit_cursor, "commit_cursor")?,
                &event.transaction_id,
                &i64::from(event.transaction_index),
                &i64::from(event.schema_version),
                &event.idempotency_key,
                &root_execution_id,
                &activity_id,
            ],
        ))?;
    }
    for stream in &snapshot.transaction_streams {
        pg(tx.execute(
            "INSERT INTO runtime_transaction_streams
             (transaction_id, stream_id, expected_revision, committed_revision)
             VALUES ($1,$2,$3,$4)",
            &[
                &stream.transaction_id,
                &stream.stream_id,
                &to_i64(stream.expected_revision, "expected_revision")?,
                &to_i64(stream.committed_revision, "committed_revision")?,
            ],
        ))?;
    }
    for head in &snapshot.stream_heads {
        pg(tx.execute(
            "INSERT INTO runtime_stream_heads(stream_id, revision) VALUES ($1,$2)",
            &[&head.stream_id, &to_i64(head.revision, "revision")?],
        ))?;
    }
    for terminal in &snapshot.session_outbox {
        let next_attempt_at = terminal
            .next_attempt_at_ms
            .map(|value| to_i64(value, "next_attempt_at"))
            .transpose()?;
        let claim_expires_at = terminal
            .claim_expires_at_ms
            .map(|value| to_i64(value, "claim_expires_at"))
            .transpose()?;
        let materialized_at = terminal
            .materialized_at_ms
            .map(|value| to_i64(value, "materialized_at"))
            .transpose()?;
        let session_generation = terminal
            .session_generation
            .map(|value| to_i64(value, "session_generation"))
            .transpose()?;
        let input_claim_revision = terminal
            .input_claim_revision
            .map(|value| to_i64(value, "input_claim_revision"))
            .transpose()?;
        let input_sequence = terminal
            .input_sequence
            .map(|value| to_i64(value, "input_sequence"))
            .transpose()?;
        pg(tx.execute(
            "INSERT INTO runtime_session_outbox
             (terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
              request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
              input_claim_revision, status, attempts,
              next_attempt_at, claim_owner, claim_expires_at, failure_class, last_error,
              materialized_at, revision)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)",
            &[
                &terminal.terminal_id,
                &terminal.message_id,
                &terminal.session_id,
                &to_i64(terminal.commit_cursor, "commit_cursor")?,
                &terminal.payload_ref,
                &terminal.execution_id,
                &terminal.turn_id,
                &terminal.request_id,
                &session_generation,
                &input_sequence,
                &terminal.input_claim_owner,
                &terminal.input_claim_token,
                &input_claim_revision,
                &terminal.status,
                &i64::from(terminal.attempts),
                &next_attempt_at,
                &terminal.claim_owner,
                &claim_expires_at,
                &terminal.failure_class,
                &terminal.last_error,
                &materialized_at,
                &to_i64(terminal.revision, "revision")?,
            ],
        ))?;
    }
    for lease in &snapshot.decision_leases {
        pg(tx.execute(
            "INSERT INTO runtime_consumed_decision_leases
             (lease_id, principal_id, review_id, action, scope, evidence_digest, credential_epoch, consumed_at_ms)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            &[
                &lease.lease_id,
                &lease.principal_id,
                &lease.review_id,
                &lease.action,
                &lease.scope,
                &lease.evidence_digest,
                &to_i64(lease.credential_epoch, "credential_epoch")?,
                &to_i64(lease.consumed_at_ms, "consumed_at_ms")?,
            ],
        ))?;
    }
    if let Some(commit) = snapshot.commits.last() {
        pg(tx.query_one(
            "SELECT setval(pg_get_serial_sequence('runtime_commits', 'commit_cursor'), $1, true)",
            &[&to_i64(commit.commit_cursor, "commit_cursor")?],
        ))?;
    }
    pg(tx.commit())?;
    Ok(())
}

fn canonical_snapshot(snapshot: &RuntimeEventStoreSnapshot) -> RuntimeEventStoreSnapshot {
    let mut canonical = snapshot.clone();
    canonical.commits.sort_by_key(|commit| commit.commit_cursor);
    canonical
        .events
        .sort_by_key(|event| (event.commit_cursor, event.transaction_index));
    canonical.transaction_streams.sort_by(|left, right| {
        (&left.transaction_id, &left.stream_id).cmp(&(&right.transaction_id, &right.stream_id))
    });
    canonical
        .stream_heads
        .sort_by(|left, right| left.stream_id.cmp(&right.stream_id));
    canonical
        .session_outbox
        .sort_by(|left, right| left.terminal_id.cmp(&right.terminal_id));
    canonical
        .decision_leases
        .sort_by(|left, right| left.lease_id.cmp(&right.lease_id));
    canonical
}

fn append_transaction_in_tx(
    tx: &mut PostgresTransaction<'_>,
    request: &AppendTransactionRequest,
    terminal: Option<&SessionTerminalInput>,
) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
    validate_transaction(request)?;
    if let Some(terminal) = terminal {
        validate_fenced_terminal(terminal)?;
        if serde_json::to_vec(&(request, terminal))?.len() > MAX_TRANSACTION_BYTES {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "serialized terminal transaction exceeds hard limit {MAX_TRANSACTION_BYTES} bytes"
            )));
        }
    }
    let request_hash = request_hash_with_terminal(request, terminal)?;
    let transaction_lock = format!("cowd-runtime-event-transaction:{}", request.transaction_id);
    pg(tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        &[&transaction_lock],
    ))?;
    if let Some(row) = pg(tx.query_opt(
        "SELECT request_hash FROM runtime_commits WHERE transaction_id=$1",
        &[&request.transaction_id],
    ))? {
        let committed_hash: String = pg(row.try_get(0))?;
        if committed_hash != request_hash {
            return Err(RuntimeEventStoreError::TransactionConflict {
                transaction_id: request.transaction_id.clone(),
            });
        }
        let receipt = load_receipt(tx, &request.transaction_id, true)?;
        if let Some(terminal) = terminal {
            verify_terminal_for_commit(tx, terminal, receipt.commit_cursor)?;
        }
        return Ok(receipt);
    }

    let mut streams = request.expected_streams.iter().collect::<Vec<_>>();
    streams.sort_by(|left, right| left.stream_id.cmp(&right.stream_id));
    for stream in &streams {
        let stream_lock = format!("cowd-runtime-event-stream:{}", stream.stream_id);
        pg(tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&stream_lock],
        ))?;
    }
    let expected = request
        .expected_streams
        .iter()
        .map(|stream| (stream.stream_id.as_str(), stream.expected_revision))
        .collect::<BTreeMap<_, _>>();
    for stream in &request.expected_streams {
        let actual = stream_head(tx, &stream.stream_id)?;
        if actual != stream.expected_revision {
            return Err(RuntimeEventStoreError::StaleRevision {
                stream_id: stream.stream_id.clone(),
                expected: stream.expected_revision,
                actual,
            });
        }
    }

    let created_at_ms = now_ms();
    let row = pg(tx.query_one(
        "INSERT INTO runtime_commits(transaction_id, request_hash, created_at_ms)
         VALUES ($1,$2,$3) RETURNING commit_cursor",
        &[
            &request.transaction_id,
            &request_hash,
            &to_i64(created_at_ms, "created_at_ms")?,
        ],
    ))?;
    let commit_cursor = from_i64(pg(row.try_get(0))?, "commit_cursor")?;
    if let Some(terminal) = terminal {
        insert_terminal_in_tx(tx, terminal, commit_cursor)?;
    }
    let mut increments = BTreeMap::<&str, u64>::new();
    let mut event_ids = Vec::with_capacity(request.events.len());
    for (transaction_index, input) in request.events.iter().enumerate() {
        let stream_id = input.event.stream_id.as_str();
        let offset = increments.entry(stream_id).or_default();
        *offset += 1;
        let sequence = expected[stream_id] + *offset;
        let event_id = format!("runtime-event-{}", uuid::Uuid::new_v4());
        let refs = serde_json::to_value(&input.event.refs)?;
        let activity_binding = input.event.activity_binding();
        let root_execution_id = activity_binding
            .as_ref()
            .map(|binding| binding.root_execution_id.as_str());
        let activity_id = activity_binding
            .as_ref()
            .map(|binding| binding.activity_id.as_str());
        pg(tx.execute(
            "INSERT INTO runtime_events (event_id, stream_id, sequence, scope, kind, status, actor,
                payload, refs, created_at_ms, commit_cursor, transaction_id, transaction_index,
                schema_version, idempotency_key, root_execution_id, activity_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
            &[
                &event_id,
                &input.event.stream_id,
                &to_i64(sequence, "sequence")?,
                &input.event.scope.as_str(),
                &input.event.kind,
                &input.event.status,
                &input.event.actor,
                &input.event.payload,
                &refs,
                &to_i64(created_at_ms, "created_at_ms")?,
                &to_i64(commit_cursor, "commit_cursor")?,
                &request.transaction_id,
                &to_i64(transaction_index as u64, "transaction_index")?,
                &i64::from(input.schema_version),
                &input.idempotency_key,
                &root_execution_id,
                &activity_id,
            ],
        ))?;
        event_ids.push(event_id);
    }
    let mut stream_revisions = Vec::with_capacity(request.expected_streams.len());
    for stream in &request.expected_streams {
        let committed_revision = stream.expected_revision
            + increments
                .get(stream.stream_id.as_str())
                .copied()
                .unwrap_or_default();
        pg(tx.execute(
            "INSERT INTO runtime_stream_heads(stream_id, revision) VALUES ($1,$2)
             ON CONFLICT(stream_id) DO UPDATE SET revision=EXCLUDED.revision",
            &[
                &stream.stream_id,
                &to_i64(committed_revision, "committed_revision")?,
            ],
        ))?;
        pg(tx.execute(
            "INSERT INTO runtime_transaction_streams
             (transaction_id, stream_id, expected_revision, committed_revision)
             VALUES ($1,$2,$3,$4)",
            &[
                &request.transaction_id,
                &stream.stream_id,
                &to_i64(stream.expected_revision, "expected_revision")?,
                &to_i64(committed_revision, "committed_revision")?,
            ],
        ))?;
        stream_revisions.push(CommittedStreamRevision {
            stream_id: stream.stream_id.clone(),
            expected_revision: stream.expected_revision,
            committed_revision,
        });
    }
    Ok(AppendTransactionReceipt {
        commit_cursor,
        transaction_id: request.transaction_id.clone(),
        request_hash,
        stream_revisions,
        event_ids,
        duplicate: false,
    })
}

fn insert_terminal_in_tx(
    tx: &mut PostgresTransaction<'_>,
    terminal: &SessionTerminalInput,
    commit_cursor: u64,
) -> RuntimeEventStoreResult<()> {
    pg(tx.execute(
        "INSERT INTO runtime_session_outbox
         (terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
          request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
          input_claim_revision, status, revision)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'pending',0)
         ON CONFLICT(terminal_id) DO NOTHING",
        &[
            &terminal.terminal_id,
            &terminal.message_id,
            &terminal.session_id,
            &to_i64(commit_cursor, "commit_cursor")?,
            &terminal.payload_ref,
            &terminal.execution_id,
            &terminal.turn_id,
            &terminal.request_id,
            &terminal
                .session_generation
                .map(|value| to_i64(value, "session_generation"))
                .transpose()?,
            &terminal
                .input_sequence
                .map(|value| to_i64(value, "input_sequence"))
                .transpose()?,
            &terminal.input_claim_owner,
            &terminal.input_claim_token,
            &terminal
                .input_claim_revision
                .map(|value| to_i64(value, "input_claim_revision"))
                .transpose()?,
        ],
    ))?;
    let stored = query_runtime_session_outbox(tx, &terminal.terminal_id)?.ok_or_else(|| {
        RuntimeEventStoreError::Corrupt(format!(
            "terminal outbox `{}` disappeared during commit",
            terminal.terminal_id
        ))
    })?;
    if stored.message_id != terminal.message_id
        || stored.session_id != terminal.session_id
        || stored.commit_cursor != commit_cursor
        || stored.payload_ref != terminal.payload_ref
        || stored.execution_id != terminal.execution_id
        || stored.turn_id != terminal.turn_id
        || stored.request_id != terminal.request_id
        || stored.session_generation != terminal.session_generation
        || stored.input_sequence != terminal.input_sequence
        || stored.input_claim_owner != terminal.input_claim_owner
        || stored.input_claim_token != terminal.input_claim_token
        || stored.input_claim_revision != terminal.input_claim_revision
    {
        return Err(RuntimeEventStoreError::TransactionConflict {
            transaction_id: terminal.terminal_id.clone(),
        });
    }
    Ok(())
}

fn verify_terminal_for_commit(
    client: &mut impl PostgresClient,
    terminal: &SessionTerminalInput,
    commit_cursor: u64,
) -> RuntimeEventStoreResult<()> {
    let rows = pg(client.query(
        "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                input_claim_revision, status, attempts, next_attempt_at, claim_owner,
                claim_expires_at, failure_class, last_error, materialized_at, revision
           FROM runtime_session_outbox WHERE commit_cursor=$1
           ORDER BY terminal_id LIMIT 2",
        &[&to_i64(commit_cursor, "commit_cursor")?],
    ))?;
    if rows.len() != 1 {
        return Err(RuntimeEventStoreError::TransactionConflict {
            transaction_id: terminal.terminal_id.clone(),
        });
    }
    let stored = row_to_runtime_session_outbox(&rows[0])?;
    if stored.terminal_id != terminal.terminal_id
        || stored.message_id != terminal.message_id
        || stored.session_id != terminal.session_id
        || stored.payload_ref != terminal.payload_ref
        || stored.execution_id != terminal.execution_id
        || stored.turn_id != terminal.turn_id
        || stored.request_id != terminal.request_id
        || stored.session_generation != terminal.session_generation
        || stored.input_sequence != terminal.input_sequence
    {
        return Err(RuntimeEventStoreError::TransactionConflict {
            transaction_id: terminal.terminal_id.clone(),
        });
    }
    Ok(())
}

fn load_receipt(
    client: &mut impl PostgresClient,
    transaction_id: &str,
    duplicate: bool,
) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
    let row = pg(client.query_one(
        "SELECT commit_cursor, request_hash FROM runtime_commits WHERE transaction_id=$1",
        &[&transaction_id],
    ))?;
    let commit_cursor = from_i64(pg(row.try_get(0))?, "commit_cursor")?;
    let request_hash: String = pg(row.try_get(1))?;
    let stream_revisions = pg(client.query(
        "SELECT stream_id, expected_revision, committed_revision FROM runtime_transaction_streams
         WHERE transaction_id=$1 ORDER BY stream_id ASC",
        &[&transaction_id],
    ))?
    .iter()
    .map(|row| {
        Ok(CommittedStreamRevision {
            stream_id: pg(row.try_get(0))?,
            expected_revision: from_i64(pg(row.try_get(1))?, "expected_revision")?,
            committed_revision: from_i64(pg(row.try_get(2))?, "committed_revision")?,
        })
    })
    .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
    let event_ids = pg(client.query(
        "SELECT event_id FROM runtime_events WHERE transaction_id=$1 ORDER BY transaction_index ASC",
        &[&transaction_id],
    ))?
    .iter()
    .map(|row| pg(row.try_get(0)))
    .collect::<RuntimeEventStoreResult<Vec<_>>>()?;
    Ok(AppendTransactionReceipt {
        commit_cursor,
        transaction_id: transaction_id.to_string(),
        request_hash,
        stream_revisions,
        event_ids,
        duplicate,
    })
}

fn stream_head(client: &mut impl PostgresClient, stream_id: &str) -> RuntimeEventStoreResult<u64> {
    pg(client.query_opt(
        "SELECT revision FROM runtime_stream_heads WHERE stream_id=$1",
        &[&stream_id],
    ))?
    .map(|row| from_i64(pg(row.try_get(0))?, "revision"))
    .transpose()
    .map(|value| value.unwrap_or_default())
}

fn query_runtime_session_outbox(
    client: &mut impl PostgresClient,
    terminal_id: &str,
) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>> {
    pg(client.query_opt(
        "SELECT terminal_id, message_id, session_id, commit_cursor, payload_ref, execution_id, turn_id,
                request_id, session_generation, input_sequence, input_claim_owner, input_claim_token,
                input_claim_revision, status,
                attempts, next_attempt_at, claim_owner, claim_expires_at, failure_class,
                last_error, materialized_at, revision
           FROM runtime_session_outbox WHERE terminal_id=$1",
        &[&terminal_id],
    ))?
    .map(|row| row_to_runtime_session_outbox(&row))
    .transpose()
}

fn rows_to_events(rows: Vec<Row>) -> RuntimeEventStoreResult<Vec<DurableRuntimeEvent>> {
    rows.iter().map(row_to_event).collect()
}

fn row_to_event(row: &Row) -> RuntimeEventStoreResult<DurableRuntimeEvent> {
    let scope: String = pg(row.try_get(3))?;
    Ok(DurableRuntimeEvent {
        event_id: pg(row.try_get(0))?,
        stream_id: pg(row.try_get(1))?,
        sequence: from_i64(pg(row.try_get(2))?, "sequence")?,
        scope: RuntimeEventScope::parse(&scope)?,
        kind: pg(row.try_get(4))?,
        status: pg(row.try_get(5))?,
        actor: pg(row.try_get(6))?,
        payload: pg(row.try_get(7))?,
        refs: serde_json::from_value(pg(row.try_get::<_, Value>(8))?)?,
        created_at_ms: from_i64(pg(row.try_get(9))?, "created_at_ms")?,
        commit_cursor: from_i64(pg(row.try_get(10))?, "commit_cursor")?,
        transaction_id: pg(row.try_get(11))?,
        transaction_index: u32::try_from(from_i64(pg(row.try_get(12))?, "transaction_index")?)
            .map_err(|_| {
                RuntimeEventStoreError::Corrupt("transaction_index exceeds u32".to_string())
            })?,
        schema_version: u32::try_from(from_i64(pg(row.try_get(13))?, "schema_version")?).map_err(
            |_| RuntimeEventStoreError::Corrupt("schema_version exceeds u32".to_string()),
        )?,
        idempotency_key: pg(row.try_get(14))?,
    })
}

fn row_to_projection_checkpoint(row: &Row) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
    Ok(RuntimeProjectionCheckpoint {
        projection_id: pg(row.try_get(0))?,
        source_cursor: from_i64(pg(row.try_get(1))?, "source_cursor")?,
        revision: from_i64(pg(row.try_get(2))?, "revision")?,
        payload: pg(row.try_get(3))?,
        updated_at_ms: from_i64(pg(row.try_get(4))?, "updated_at_ms")?,
    })
}

fn validate_projection_id(projection_id: &str) -> RuntimeEventStoreResult<()> {
    if projection_id.trim().is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "projection id must not be empty".to_string(),
        ));
    }
    if projection_id.len() > 512 {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "projection id exceeds 512 bytes".to_string(),
        ));
    }
    Ok(())
}

fn group_committed_events(events: Vec<RuntimeEventRecord>) -> Vec<CommittedEventBatch> {
    let mut batches = Vec::<CommittedEventBatch>::new();
    for event in events {
        if let Some(batch) = batches.last_mut() {
            if batch.commit_cursor == event.commit_cursor {
                batch.events.push(event);
                continue;
            }
        }
        batches.push(CommittedEventBatch {
            commit_cursor: event.commit_cursor,
            transaction_id: event.transaction_id.clone(),
            events: vec![event],
        });
    }
    batches
}

fn row_to_runtime_session_outbox(row: &Row) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord> {
    Ok(RuntimeSessionOutboxRecord {
        terminal_id: pg(row.try_get(0))?,
        message_id: pg(row.try_get(1))?,
        session_id: pg(row.try_get(2))?,
        commit_cursor: from_i64(pg(row.try_get(3))?, "commit_cursor")?,
        payload_ref: pg(row.try_get(4))?,
        execution_id: pg(row.try_get(5))?,
        turn_id: pg(row.try_get(6))?,
        request_id: pg(row.try_get(7))?,
        session_generation: pg(row.try_get::<_, Option<i64>>(8))?
            .map(|value| from_i64(value, "session_generation"))
            .transpose()?,
        input_sequence: pg(row.try_get::<_, Option<i64>>(9))?
            .map(|value| from_i64(value, "input_sequence"))
            .transpose()?,
        input_claim_owner: pg(row.try_get(10))?,
        input_claim_token: pg(row.try_get(11))?,
        input_claim_revision: pg(row.try_get::<_, Option<i64>>(12))?
            .map(|value| from_i64(value, "input_claim_revision"))
            .transpose()?,
        status: pg(row.try_get(13))?,
        attempts: u32::try_from(from_i64(pg(row.try_get(14))?, "attempts")?)
            .map_err(|_| RuntimeEventStoreError::Corrupt("attempts exceeds u32".to_string()))?,
        next_attempt_at_ms: pg(row.try_get::<_, Option<i64>>(15))?
            .map(|value| from_i64(value, "next_attempt_at"))
            .transpose()?,
        claim_owner: pg(row.try_get(16))?,
        claim_expires_at_ms: pg(row.try_get::<_, Option<i64>>(17))?
            .map(|value| from_i64(value, "claim_expires_at"))
            .transpose()?,
        failure_class: pg(row.try_get(18))?,
        last_error: pg(row.try_get(19))?,
        materialized_at_ms: pg(row.try_get::<_, Option<i64>>(20))?
            .map(|value| from_i64(value, "materialized_at"))
            .transpose()?,
        revision: from_i64(pg(row.try_get(21))?, "revision")?,
    })
}

fn validate_transaction(request: &AppendTransactionRequest) -> RuntimeEventStoreResult<()> {
    if request.transaction_id.trim().is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "transaction_id must not be empty".to_string(),
        ));
    }
    if request.events.is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "events must not be empty".to_string(),
        ));
    }
    if request.events.len() > MAX_TRANSACTION_EVENTS {
        return Err(RuntimeEventStoreError::InvalidTransaction(format!(
            "event count exceeds hard limit {MAX_TRANSACTION_EVENTS}"
        )));
    }
    if serde_json::to_vec(request)?.len() > MAX_TRANSACTION_BYTES {
        return Err(RuntimeEventStoreError::InvalidTransaction(format!(
            "serialized transaction exceeds hard limit {MAX_TRANSACTION_BYTES} bytes"
        )));
    }
    let mut expected = BTreeSet::new();
    for stream in &request.expected_streams {
        if stream.stream_id.trim().is_empty() || !expected.insert(stream.stream_id.as_str()) {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "expected streams must be non-empty and unique".to_string(),
            ));
        }
    }
    for event in &request.events {
        validate_event(&event.event)?;
        if event.schema_version == 0 {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "event schema_version must be positive".to_string(),
            ));
        }
        if !expected.contains(event.event.stream_id.as_str()) {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "event stream `{}` has no expected revision",
                event.event.stream_id
            )));
        }
    }
    Ok(())
}

fn validate_fenced_terminal(terminal: &SessionTerminalInput) -> RuntimeEventStoreResult<()> {
    let required = [
        terminal.terminal_id.as_str(),
        terminal.message_id.as_str(),
        terminal.session_id.as_str(),
        terminal.payload_ref.as_str(),
        terminal.execution_id.as_deref().unwrap_or_default(),
        terminal.turn_id.as_deref().unwrap_or_default(),
        terminal.request_id.as_deref().unwrap_or_default(),
        terminal.input_claim_owner.as_deref().unwrap_or_default(),
        terminal.input_claim_token.as_deref().unwrap_or_default(),
    ];
    if required.iter().any(|value| value.trim().is_empty())
        || terminal
            .session_generation
            .is_none_or(|generation| generation == 0)
        || terminal.input_sequence.is_none()
        || terminal
            .input_claim_revision
            .is_none_or(|revision| revision == 0)
    {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "terminal transaction requires complete execution, turn and Session claim fences"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_event(input: &RuntimeEventInput) -> RuntimeEventStoreResult<()> {
    if input.stream_id.trim().is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "event stream_id must not be empty".to_string(),
        ));
    }
    if input.kind.trim().is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "event kind must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_decision_lease_claims(
    lease_id: &str,
    principal_id: &str,
    review_id: &str,
    action: &str,
    scope: &str,
    evidence_digest: &str,
) -> RuntimeEventStoreResult<()> {
    if lease_id.trim().is_empty()
        || principal_id.trim().is_empty()
        || review_id.trim().is_empty()
        || action.trim().is_empty()
        || scope.trim().is_empty()
        || evidence_digest.trim().is_empty()
    {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "decision lease consumption requires non-empty bound claims".to_string(),
        ));
    }
    Ok(())
}

const fn failure_class_as_str(class: RuntimeSessionOutboxFailureClass) -> &'static str {
    match class {
        RuntimeSessionOutboxFailureClass::Retryable => "retryable",
        RuntimeSessionOutboxFailureClass::Permanent => "permanent",
        RuntimeSessionOutboxFailureClass::AuthorizationBlocked => "authorization_blocked",
        RuntimeSessionOutboxFailureClass::CorruptPayload => "corrupt_payload",
    }
}

fn request_hash(request: &AppendTransactionRequest) -> RuntimeEventStoreResult<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(request)?)
    ))
}

fn request_hash_with_terminal(
    request: &AppendTransactionRequest,
    terminal: Option<&SessionTerminalInput>,
) -> RuntimeEventStoreResult<String> {
    terminal.map_or_else(
        || request_hash(request),
        |terminal| {
            Ok(format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&(request, terminal))?)
            ))
        },
    )
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn pg<T>(result: Result<T, postgres::Error>) -> RuntimeEventStoreResult<T> {
    result.map_err(|error| RuntimeEventStoreError::Storage(StorageError::Other(error.to_string())))
}

fn from_i64(value: i64, field: &str) -> RuntimeEventStoreResult<u64> {
    u64::try_from(value).map_err(|_| {
        RuntimeEventStoreError::Corrupt(format!("runtime event `{field}` is negative"))
    })
}

fn to_i64(value: u64, field: &str) -> RuntimeEventStoreResult<i64> {
    i64::try_from(value).map_err(|_| {
        RuntimeEventStoreError::InvalidTransaction(format!("runtime event `{field}` exceeds i64"))
    })
}

/// Complete PostgreSQL implementation of the Task control-plane backend.
///
/// The store locks only the task being updated. Independent task lifecycles
/// can therefore use separate PostgreSQL connections concurrently; task-level
/// transitions remain atomic even across gateway processes.
#[derive(Clone, Debug)]
pub struct PostgresTaskStore {
    executor: PostgresExecutor,
}

/// Immutable proof written only after an explicit quiesced task copy reaches
/// canonical digest equality. It intentionally carries no backend URL/path.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TaskMigrationManifest {
    pub domain: String,
    pub source_digest: String,
    pub target_digest: String,
    pub task_count: usize,
}

impl PostgresTaskStore {
    pub fn new(executor: PostgresExecutor) -> Result<Self, String> {
        executor
            .apply_migrations(TASK_DOMAIN, TASK_MIGRATIONS)
            .map_err(|error| error.to_string())?;
        Ok(Self { executor })
    }

    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> Result<Self, String> {
        Self::new(PostgresExecutor::connect(config, resolver).map_err(|error| error.to_string())?)
    }

    #[must_use]
    pub fn executor(&self) -> &PostgresExecutor {
        &self.executor
    }

    #[must_use]
    pub fn into_task_service(self) -> TaskAggregateService {
        TaskAggregateService::from_backend(Arc::new(self))
    }
}

impl TaskStoreBackend for PostgresTaskStore {
    fn list(&self) -> Result<Vec<TaskAggregate>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let rows = connection
            .query(
                "SELECT record_json FROM runtime_tasks ORDER BY created_at_ms ASC, task_id ASC",
                &[],
            )
            .map_err(|error| error.to_string())?;
        rows.iter().map(task_record_from_row).collect()
    }

    fn get(&self, task_id: &str) -> Result<Option<TaskAggregate>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        connection
            .query_opt(
                "SELECT record_json FROM runtime_tasks WHERE task_id=$1",
                &[&task_id],
            )
            .map_err(|error| error.to_string())?
            .map(|row| task_record_from_row(&row))
            .transpose()
    }

    fn organization_candidates(&self, limit: usize) -> Result<Vec<TaskAggregate>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = connection
            .query(
                "SELECT record_json FROM runtime_tasks
                  WHERE status IN ('pending','running','reviewing','blocked')
                    AND record_json ->> 'kind' = 'root'
                    AND record_json ->> 'origin' <> 'system'
                    AND record_json ->> 'mission_assignment' <> 'explicit_locked'
                  ORDER BY updated_at_ms DESC,task_id ASC LIMIT $1",
                &[&limit],
            )
            .map_err(|error| error.to_string())?;
        rows.iter().map(task_record_from_row).collect()
    }

    fn unorganized_candidates(&self, limit: usize) -> Result<Vec<TaskAggregate>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = connection
            .query(
                "SELECT task.record_json FROM runtime_tasks AS task
                  WHERE task.status IN ('pending','running','reviewing','blocked')
                    AND task.record_json ->> 'kind' = 'root'
                    AND task.record_json ->> 'origin' <> 'system'
                    AND task.record_json ->> 'mission_assignment' <> 'explicit_locked'
                    AND NOT EXISTS (
                        SELECT 1 FROM runtime_mission_organization_decisions AS decision
                         WHERE decision.decision_id = 'mission-organization:' || task.task_id
                    )
                  ORDER BY task.updated_at_ms DESC,task.task_id ASC LIMIT $1",
                &[&limit],
            )
            .map_err(|error| error.to_string())?;
        rows.iter().map(task_record_from_row).collect()
    }

    fn open_root_candidates(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<TaskAggregate>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = connection
            .query(
                "SELECT DISTINCT task.record_json,task.updated_at_ms,task.task_id
                   FROM runtime_task_turn_bindings AS binding
                   JOIN runtime_tasks AS task ON task.task_id=binding.task_id
                  WHERE binding.session_id=$1
                    AND task.status IN ('pending','running','reviewing','blocked')
                    AND task.record_json ->> 'kind' = 'root'
                  ORDER BY task.updated_at_ms DESC,task.task_id ASC LIMIT $2",
                &[&session_id, &limit],
            )
            .map_err(|error| error.to_string())?;
        rows.iter().map(task_record_from_row).collect()
    }

    fn for_graphs(&self, graph_ids: &[String]) -> Result<Vec<TaskAggregate>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let mut tasks = std::collections::BTreeMap::new();
        for graph_id in graph_ids {
            let rows = connection
                .query(
                    "SELECT task.record_json
                       FROM runtime_task_graph_refs AS reference
                       JOIN runtime_tasks AS task ON task.task_id=reference.task_id
                      WHERE reference.graph_id=$1",
                    &[graph_id],
                )
                .map_err(|error| error.to_string())?;
            for row in rows {
                let task = task_record_from_row(&row)?;
                tasks.insert(task.task_id.clone(), task);
            }
        }
        Ok(tasks.into_values().collect())
    }

    fn bind_turn(&self, binding: &TaskTurnBinding) -> Result<TaskTurnBinding, String> {
        runtime::task::validate_binding(binding)?;
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let record_json = serde_json::to_value(binding).map_err(|error| error.to_string())?;
        connection
            .execute(
                "INSERT INTO runtime_task_turn_bindings(
                    binding_id,task_id,session_id,turn_id,role,input_id,bound_at_ms,record_json
                 ) VALUES($1,$2,$3,$4,$5,$6,$7,$8)
                 ON CONFLICT(task_id,session_id,turn_id) DO NOTHING",
                &[
                    &binding.binding_id,
                    &binding.task_id,
                    &binding.session_id,
                    &binding.turn_id,
                    &task_turn_role_name(binding.role),
                    &binding.input_id,
                    &task_time_i64(binding.bound_at_ms, "bound_at_ms")?,
                    &record_json,
                ],
            )
            .map_err(|error| error.to_string())?;
        let row = connection
            .query_one(
                "SELECT record_json FROM runtime_task_turn_bindings
                  WHERE task_id=$1 AND session_id=$2 AND turn_id=$3",
                &[&binding.task_id, &binding.session_id, &binding.turn_id],
            )
            .map_err(|error| error.to_string())?;
        let stored = task_binding_from_row(&row)?;
        if stored != *binding {
            return Err(format!(
                "turn `{}` is already bound to task `{}` with different data",
                binding.turn_id, binding.task_id
            ));
        }
        Ok(stored)
    }

    fn create_with_origin_binding(
        &self,
        aggregate: &TaskAggregate,
        mutation: &TaskMutation,
        binding: &TaskTurnBinding,
    ) -> Result<(TaskMutationResult, TaskTurnBinding), String> {
        validate_task_aggregate_for_backend(aggregate)?;
        runtime::task::validate_binding(binding)?;
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let lock_key = format!("cowd-runtime-task:{}", aggregate.task_id);
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&lock_key],
            )
            .map_err(|error| error.to_string())?;
        let current = transaction
            .query_opt(
                "SELECT record_json FROM runtime_tasks WHERE task_id=$1 FOR UPDATE",
                &[&aggregate.task_id],
            )
            .map_err(|error| error.to_string())?
            .map(|row| task_record_from_row(&row))
            .transpose()?;
        let (stored_task, outbox) = if let Some(current) = current {
            if !runtime::task::same_immutable_task_creation(&current, aggregate) {
                return Err(format!(
                    "task id `{}` is already bound to different immutable creation data",
                    aggregate.task_id
                ));
            }
            let row = transaction
                .query_one(
                    "SELECT record_json FROM runtime_task_evidence_outbox
                      WHERE task_id=$1 AND revision=$2",
                    &[
                        &current.task_id,
                        &task_time_i64(current.revision, "revision")?,
                    ],
                )
                .map_err(|error| error.to_string())?;
            (current, task_outbox_from_row(&row)?)
        } else {
            let outbox = validate_backend_mutation(&aggregate.task_id, None, aggregate, mutation)?
                .ok_or_else(|| "Task creation requires an evidence outbox".to_string())?;
            let record_json = serde_json::to_value(aggregate).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_tasks(
                        task_id,status,created_at_ms,updated_at_ms,record_json
                     ) VALUES($1,$2,$3,$4,$5)",
                    &[
                        &aggregate.task_id,
                        &aggregate.status.as_str(),
                        &task_time_i64(aggregate.created_at_ms, "created_at_ms")?,
                        &task_time_i64(aggregate.updated_at_ms, "updated_at_ms")?,
                        &record_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
            sync_task_graph_refs_postgres(&mut transaction, aggregate)?;
            let outbox_json = serde_json::to_value(&outbox).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_task_evidence_outbox(
                        outbox_id,task_id,revision,event_kind,created_at_ms,record_json
                     ) VALUES($1,$2,$3,$4,$5,$6)",
                    &[
                        &outbox.outbox_id,
                        &outbox.task_id,
                        &task_time_i64(outbox.revision, "revision")?,
                        &outbox.event_kind,
                        &task_time_i64(outbox.created_at_ms, "created_at_ms")?,
                        &outbox_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
            (aggregate.clone(), outbox)
        };
        let binding_json = serde_json::to_value(binding).map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO runtime_task_turn_bindings(
                    binding_id,task_id,session_id,turn_id,role,input_id,bound_at_ms,record_json
                 ) VALUES($1,$2,$3,$4,$5,$6,$7,$8)
                 ON CONFLICT(task_id,session_id,turn_id) DO NOTHING",
                &[
                    &binding.binding_id,
                    &binding.task_id,
                    &binding.session_id,
                    &binding.turn_id,
                    &task_turn_role_name(binding.role),
                    &binding.input_id,
                    &task_time_i64(binding.bound_at_ms, "bound_at_ms")?,
                    &binding_json,
                ],
            )
            .map_err(|error| error.to_string())?;
        let row = transaction
            .query_one(
                "SELECT record_json FROM runtime_task_turn_bindings
                  WHERE task_id=$1 AND session_id=$2 AND turn_id=$3",
                &[&binding.task_id, &binding.session_id, &binding.turn_id],
            )
            .map_err(|error| error.to_string())?;
        let stored_binding = task_binding_from_row(&row)?;
        if stored_binding != *binding {
            return Err(format!(
                "turn `{}` has a conflicting origin Task binding",
                binding.turn_id
            ));
        }
        transaction.commit().map_err(|error| error.to_string())?;
        Ok((
            TaskMutationResult::from_backend_commit(stored_task, mutation, Some(outbox)),
            stored_binding,
        ))
    }

    fn bindings_for_task(&self, task_id: &str) -> Result<Vec<TaskTurnBinding>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let rows = connection
            .query(
                "SELECT record_json FROM runtime_task_turn_bindings
                  WHERE task_id=$1 ORDER BY bound_at_ms ASC,binding_id ASC",
                &[&task_id],
            )
            .map_err(|error| error.to_string())?;
        rows.iter().map(task_binding_from_row).collect()
    }

    fn bindings_for_turn(
        &self,
        session_id: &str,
        turn_id: &str,
    ) -> Result<Vec<TaskTurnBinding>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let rows = connection
            .query(
                "SELECT record_json FROM runtime_task_turn_bindings
                  WHERE session_id=$1 AND turn_id=$2
                  ORDER BY CASE role WHEN 'primary' THEN 0 ELSE 1 END,
                           bound_at_ms ASC,binding_id ASC",
                &[&session_id, &turn_id],
            )
            .map_err(|error| error.to_string())?;
        rows.iter().map(task_binding_from_row).collect()
    }

    fn assign_mission_batch(
        &self,
        command: &TaskMissionAssignmentCommand,
    ) -> Result<TaskMissionAssignmentReceipt, String> {
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&format!(
                    "cowd-task-mission-assignment:{}",
                    command.operation_id
                )],
            )
            .map_err(|error| error.to_string())?;
        if let Some(row) = transaction
            .query_opt(
                "SELECT record_json FROM runtime_task_mission_assignment_outbox WHERE operation_id=$1",
                &[&command.operation_id],
            )
            .map_err(|error| error.to_string())?
        {
            let value: Value = row.try_get(0).map_err(|error| error.to_string())?;
            let record: TaskMissionAssignmentOutboxRecord =
                serde_json::from_value(value).map_err(|error| error.to_string())?;
            validate_task_assignment_replay(command, &record.receipt)?;
            return Ok(record.receipt);
        }
        if command.task_ids.is_empty()
            || command.expected_task_revisions.len() != command.task_ids.len()
        {
            return Err(
                "task mission assignment requires Tasks and expected revisions".to_string(),
            );
        }
        let applied_at_ms = task_now_ms();
        let mut updated = Vec::with_capacity(command.task_ids.len());
        for task_id in &command.task_ids {
            let task_id_value = task_id.as_str();
            let row = transaction
                .query_opt(
                    "SELECT record_json FROM runtime_tasks WHERE task_id=$1 FOR UPDATE",
                    &[&task_id_value],
                )
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("task `{task_id_value}` not found"))?;
            let mut task = task_record_from_row(&row)?;
            let expected = command
                .expected_task_revisions
                .get(task_id_value)
                .copied()
                .ok_or_else(|| format!("task `{task_id_value}` has no expected revision"))?;
            if task.revision != expected {
                return Err(format!(
                    "task `{task_id_value}` revision conflict: expected {expected}, actual {}",
                    task.revision
                ));
            }
            if task.mission_assignment == TaskMissionAssignment::ExplicitLocked
                && command.assignment != TaskMissionAssignment::ExplicitLocked
            {
                return Err(format!(
                    "task `{task_id_value}` has an explicit Mission lock"
                ));
            }
            task.mission_id.clone_from(&command.target_mission_id);
            task.mission_assignment = command.assignment;
            task.mission_assignment_revision = task.mission_assignment_revision.saturating_add(1);
            task.mission_assigned_by.clone_from(&command.actor);
            task.mission_assignment_evidence_refs = command.evidence_refs.clone();
            task.revision = task.revision.saturating_add(1);
            task.updated_at_ms = applied_at_ms;
            validate_task_aggregate_for_backend(&task)?;
            updated.push(task);
        }
        let selected = updated
            .iter()
            .map(|task| task.task_id.as_str())
            .collect::<BTreeSet<_>>();
        for task in &updated {
            if task.kind == TaskKind::Delegated && !selected.contains(task.root_task_id.as_str()) {
                let row = transaction
                    .query_one(
                        "SELECT record_json FROM runtime_tasks WHERE task_id=$1 FOR UPDATE",
                        &[&task.root_task_id],
                    )
                    .map_err(|error| error.to_string())?;
                let root = task_record_from_row(&row)?;
                if root.mission_id != command.target_mission_id {
                    return Err(format!(
                        "delegated task `{}` cannot leave root task `{}` in another Mission",
                        task.task_id, task.root_task_id
                    ));
                }
            }
        }
        let mut task_revisions = BTreeMap::new();
        for task in &updated {
            let record_json = serde_json::to_value(task).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "UPDATE runtime_tasks SET status=$2,updated_at_ms=$3,record_json=$4 WHERE task_id=$1",
                    &[
                        &task.task_id,
                        &task.status.as_str(),
                        &task_time_i64(task.updated_at_ms, "updated_at_ms")?,
                        &record_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
            let outbox = TaskEvidenceOutboxRecord {
                outbox_id: format!("task-outbox:{}:{}", task.task_id, task.revision),
                task_id: task.task_id.clone(),
                revision: task.revision,
                event_kind: "task.mission_assigned".to_string(),
                status: task.status,
                evidence_refs: command.evidence_refs.clone(),
                created_at_ms: applied_at_ms,
                projected_at_ms: None,
            };
            let outbox_json = serde_json::to_value(&outbox).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_task_evidence_outbox(
                        outbox_id,task_id,revision,event_kind,created_at_ms,record_json
                     ) VALUES($1,$2,$3,$4,$5,$6)",
                    &[
                        &outbox.outbox_id,
                        &outbox.task_id,
                        &task_time_i64(outbox.revision, "revision")?,
                        &outbox.event_kind,
                        &task_time_i64(outbox.created_at_ms, "created_at_ms")?,
                        &outbox_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
            task_revisions.insert(task.task_id.clone(), task.revision);
        }
        let receipt = TaskMissionAssignmentReceipt {
            operation_id: command.operation_id.clone(),
            target_mission_id: command.target_mission_id.clone(),
            task_revisions,
            assignment: command.assignment,
            applied_at_ms,
            evidence_refs: command.evidence_refs.clone(),
        };
        let record = TaskMissionAssignmentOutboxRecord {
            operation_id: command.operation_id.clone(),
            receipt: receipt.clone(),
            created_at_ms: applied_at_ms,
            projected_at_ms: None,
        };
        let record_json = serde_json::to_value(&record).map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO runtime_task_mission_assignment_outbox(
                    operation_id,created_at_ms,record_json
                 ) VALUES($1,$2,$3)",
                &[
                    &record.operation_id,
                    &task_time_i64(record.created_at_ms, "created_at_ms")?,
                    &record_json,
                ],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(receipt)
    }

    fn assignment_receipt(
        &self,
        operation_id: &str,
    ) -> Result<Option<TaskMissionAssignmentReceipt>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        connection
            .query_opt(
                "SELECT record_json FROM runtime_task_mission_assignment_outbox WHERE operation_id=$1",
                &[&operation_id],
            )
            .map_err(|error| error.to_string())?
            .map(|row| {
                let value: Value = row.try_get(0).map_err(|error| error.to_string())?;
                serde_json::from_value::<TaskMissionAssignmentOutboxRecord>(value)
                    .map(|record| record.receipt)
                    .map_err(|error| error.to_string())
            })
            .transpose()
    }

    fn save_organization_decision(
        &self,
        decision: &MissionOrganizationDecision,
        expected_revision: Option<u64>,
    ) -> Result<MissionOrganizationDecision, String> {
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let existing = transaction
            .query_opt(
                "SELECT record_json FROM runtime_mission_organization_decisions
                  WHERE decision_id=$1 FOR UPDATE",
                &[&decision.decision_id],
            )
            .map_err(|error| error.to_string())?
            .map(|row| {
                let value: Value = row.try_get(0).map_err(|error| error.to_string())?;
                serde_json::from_value::<MissionOrganizationDecision>(value)
                    .map_err(|error| error.to_string())
            })
            .transpose()?;
        match (existing.as_ref(), expected_revision) {
            (None, None) => {}
            (Some(existing), Some(expected)) if existing.revision == expected => {}
            (Some(existing), None)
                if existing.decision_id == decision.decision_id
                    && existing.workspace_id == decision.workspace_id
                    && existing.canonical_root_task_id() == decision.canonical_root_task_id() =>
            {
                return Ok(existing.clone());
            }
            (Some(existing), _) => {
                return Err(format!(
                    "organization decision `{}` revision conflict at {}",
                    decision.decision_id, existing.revision
                ));
            }
            (None, Some(_)) => {
                return Err(format!(
                    "organization decision `{}` does not exist",
                    decision.decision_id
                ));
            }
        }
        let record_json = serde_json::to_value(decision).map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO runtime_mission_organization_decisions(
                    decision_id,status,next_attempt_at_ms,created_at_ms,updated_at_ms,record_json
                 ) VALUES($1,$2,$3,$4,$5,$6)
                 ON CONFLICT(decision_id) DO UPDATE SET
                    status=EXCLUDED.status,next_attempt_at_ms=EXCLUDED.next_attempt_at_ms,
                    updated_at_ms=EXCLUDED.updated_at_ms,record_json=EXCLUDED.record_json",
                &[
                    &decision.decision_id,
                    &task_organization_status_name(decision.status),
                    &task_time_i64(decision.next_attempt_at_ms, "next_attempt_at_ms")?,
                    &task_time_i64(decision.created_at_ms, "created_at_ms")?,
                    &task_time_i64(decision.updated_at_ms, "updated_at_ms")?,
                    &record_json,
                ],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(decision.clone())
    }

    fn organization_decisions(
        &self,
        status: Option<MissionOrganizationStatus>,
        limit: usize,
    ) -> Result<Vec<MissionOrganizationDecision>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = if let Some(status) = status {
            connection
                .query(
                    "SELECT record_json FROM runtime_mission_organization_decisions
                      WHERE status=$1 ORDER BY created_at_ms ASC,decision_id ASC LIMIT $2",
                    &[&task_organization_status_name(status), &limit],
                )
                .map_err(|error| error.to_string())?
        } else {
            connection
                .query(
                    "SELECT record_json FROM runtime_mission_organization_decisions
                      ORDER BY created_at_ms ASC,decision_id ASC LIMIT $1",
                    &[&limit],
                )
                .map_err(|error| error.to_string())?
        };
        rows.into_iter()
            .map(|row| {
                let value: Value = row.try_get(0).map_err(|error| error.to_string())?;
                serde_json::from_value(value).map_err(|error| error.to_string())
            })
            .collect()
    }

    fn mutate_task(
        &self,
        task_id: &str,
        mutation: &TaskMutation,
        updater: &mut dyn FnMut(Option<TaskAggregate>) -> Result<TaskAggregate, String>,
    ) -> Result<TaskMutationResult, String> {
        if task_id.trim().is_empty() {
            return Err("task id is required".to_string());
        }
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let lock_key = format!("cowd-runtime-task:{task_id}");
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&lock_key],
            )
            .map_err(|error| error.to_string())?;
        let current = transaction
            .query_opt(
                "SELECT record_json FROM runtime_tasks WHERE task_id=$1 FOR UPDATE",
                &[&task_id],
            )
            .map_err(|error| error.to_string())?
            .map(|row| task_record_from_row(&row))
            .transpose()?;
        let next = updater(current.clone())?;
        if current.as_ref() == Some(&next) {
            validate_task_aggregate_for_backend(&next)?;
            let revision = task_time_i64(next.revision, "revision")?;
            let row = transaction
                .query_opt(
                    "SELECT record_json FROM runtime_task_evidence_outbox
                     WHERE task_id=$1 AND revision=$2",
                    &[&task_id, &revision],
                )
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    format!(
                        "idempotent task replay `{task_id}` revision {} has no durable outbox",
                        next.revision
                    )
                })?;
            let outbox = task_outbox_from_row(&row)?;
            transaction.commit().map_err(|error| error.to_string())?;
            return Ok(TaskMutationResult::from_backend_commit(
                next,
                mutation,
                Some(outbox),
            ));
        }
        let outbox = validate_backend_mutation(task_id, current.as_ref(), &next, mutation)?;
        if outbox.is_none() {
            transaction.commit().map_err(|error| error.to_string())?;
            return Ok(TaskMutationResult::from_backend_commit(
                next, mutation, None,
            ));
        }
        let record_json = serde_json::to_value(&next).map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO runtime_tasks
                    (task_id, status, created_at_ms, updated_at_ms, record_json)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT(task_id) DO UPDATE SET
                    status=EXCLUDED.status,
                    created_at_ms=EXCLUDED.created_at_ms,
                    updated_at_ms=EXCLUDED.updated_at_ms,
                    record_json=EXCLUDED.record_json",
                &[
                    &next.task_id,
                    &next.status.as_str(),
                    &task_time_i64(next.created_at_ms, "created_at_ms")?,
                    &task_time_i64(next.updated_at_ms, "updated_at_ms")?,
                    &record_json,
                ],
            )
            .map_err(|error| error.to_string())?;
        sync_task_graph_refs_postgres(&mut transaction, &next)?;
        let outbox = outbox.ok_or_else(|| {
            format!("task `{task_id}` changed without a durable evidence outbox record")
        })?;
        let outbox_json = serde_json::to_value(&outbox).map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO runtime_task_evidence_outbox
                    (outbox_id, task_id, revision, event_kind, created_at_ms, record_json)
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &outbox.outbox_id,
                    &outbox.task_id,
                    &task_time_i64(outbox.revision, "revision")?,
                    &outbox.event_kind,
                    &task_time_i64(outbox.created_at_ms, "created_at_ms")?,
                    &outbox_json,
                ],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(TaskMutationResult::from_backend_commit(
            next,
            mutation,
            Some(outbox),
        ))
    }

    fn pending_outbox(
        &self,
        task_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TaskEvidenceOutboxRecord>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let limit = i64::try_from(limit.min(i64::MAX as usize)).unwrap_or(i64::MAX);
        let rows = if let Some(task_id) = task_id {
            connection
                .query(
                    "SELECT record_json FROM runtime_task_evidence_outbox
                     WHERE projected_at_ms IS NULL AND task_id=$1
                     ORDER BY revision ASC LIMIT $2",
                    &[&task_id, &limit],
                )
                .map_err(|error| error.to_string())?
        } else {
            connection
                .query(
                    "SELECT record_json FROM runtime_task_evidence_outbox
                     WHERE projected_at_ms IS NULL
                     ORDER BY created_at_ms ASC, outbox_id ASC LIMIT $1",
                    &[&limit],
                )
                .map_err(|error| error.to_string())?
        };
        rows.iter().map(task_outbox_from_row).collect()
    }

    fn list_outbox(&self) -> Result<Vec<TaskEvidenceOutboxRecord>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let rows = connection
            .query(
                "SELECT record_json FROM runtime_task_evidence_outbox
                 ORDER BY created_at_ms ASC, outbox_id ASC",
                &[],
            )
            .map_err(|error| error.to_string())?;
        rows.iter().map(task_outbox_from_row).collect()
    }

    fn list_assignment_outbox(&self) -> Result<Vec<TaskMissionAssignmentOutboxRecord>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let rows = connection
            .query(
                "SELECT record_json FROM runtime_task_mission_assignment_outbox
                 ORDER BY created_at_ms ASC, operation_id ASC",
                &[],
            )
            .map_err(|error| error.to_string())?;
        rows.into_iter()
            .map(|row| {
                let value: Value = row.try_get(0).map_err(|error| error.to_string())?;
                serde_json::from_value(value).map_err(|error| error.to_string())
            })
            .collect()
    }

    fn mark_outbox_projected(&self, outbox_id: &str, projected_at_ms: u64) -> Result<(), String> {
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let row = transaction
            .query_opt(
                "SELECT record_json FROM runtime_task_evidence_outbox
                 WHERE outbox_id=$1 FOR UPDATE",
                &[&outbox_id],
            )
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("task evidence outbox `{outbox_id}` not found"))?;
        let mut record = task_outbox_from_row(&row)?;
        record.projected_at_ms = Some(projected_at_ms);
        let record_json = serde_json::to_value(&record).map_err(|error| error.to_string())?;
        transaction
            .execute(
                "UPDATE runtime_task_evidence_outbox
                 SET projected_at_ms=$2, record_json=$3 WHERE outbox_id=$1",
                &[
                    &outbox_id,
                    &task_time_i64(projected_at_ms, "projected_at_ms")?,
                    &record_json,
                ],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())
    }

    fn import_migration_snapshot(&self, snapshot: &TaskStoreSnapshot) -> Result<(), String> {
        snapshot.validate()?;
        let mut connection = self
            .executor
            .checkout_background()
            .map_err(|error| error.to_string())?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        transaction
            .batch_execute(
                "LOCK TABLE runtime_tasks IN EXCLUSIVE MODE;
                 LOCK TABLE runtime_task_graph_refs IN EXCLUSIVE MODE;
                 LOCK TABLE runtime_task_evidence_outbox IN EXCLUSIVE MODE;
                 LOCK TABLE runtime_task_turn_bindings IN EXCLUSIVE MODE;
                 LOCK TABLE runtime_task_mission_assignment_outbox IN EXCLUSIVE MODE;
                 LOCK TABLE runtime_mission_organization_decisions IN EXCLUSIVE MODE",
            )
            .map_err(|error| error.to_string())?;
        let existing_tasks: i64 = transaction
            .query_one("SELECT COUNT(*) FROM runtime_tasks", &[])
            .map_err(|error| error.to_string())?
            .get(0);
        let existing_outbox: i64 = transaction
            .query_one("SELECT COUNT(*) FROM runtime_task_evidence_outbox", &[])
            .map_err(|error| error.to_string())?
            .get(0);
        let existing_bindings: i64 = transaction
            .query_one("SELECT COUNT(*) FROM runtime_task_turn_bindings", &[])
            .map_err(|error| error.to_string())?
            .get(0);
        let existing_assignments: i64 = transaction
            .query_one(
                "SELECT COUNT(*) FROM runtime_task_mission_assignment_outbox",
                &[],
            )
            .map_err(|error| error.to_string())?
            .get(0);
        let existing_decisions: i64 = transaction
            .query_one(
                "SELECT COUNT(*) FROM runtime_mission_organization_decisions",
                &[],
            )
            .map_err(|error| error.to_string())?
            .get(0);
        if existing_tasks != 0
            || existing_bindings != 0
            || existing_outbox != 0
            || existing_assignments != 0
            || existing_decisions != 0
        {
            return Err("task migration target must be empty".to_string());
        }
        for task in &snapshot.tasks {
            let record_json = serde_json::to_value(task).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_tasks
                        (task_id, status, created_at_ms, updated_at_ms, record_json)
                     VALUES ($1, $2, $3, $4, $5)",
                    &[
                        &task.task_id,
                        &task.status.as_str(),
                        &task_time_i64(task.created_at_ms, "created_at_ms")?,
                        &task_time_i64(task.updated_at_ms, "updated_at_ms")?,
                        &record_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
            sync_task_graph_refs_postgres(&mut transaction, task)?;
        }
        for binding in &snapshot.bindings {
            let record_json = serde_json::to_value(binding).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_task_turn_bindings(
                        binding_id,task_id,session_id,turn_id,role,input_id,bound_at_ms,record_json
                     ) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
                    &[
                        &binding.binding_id,
                        &binding.task_id,
                        &binding.session_id,
                        &binding.turn_id,
                        &task_turn_role_name(binding.role),
                        &binding.input_id,
                        &task_time_i64(binding.bound_at_ms, "bound_at_ms")?,
                        &record_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
        }
        for record in &snapshot.outbox {
            let record_json = serde_json::to_value(record).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_task_evidence_outbox
                        (outbox_id, task_id, revision, event_kind, created_at_ms,
                         projected_at_ms, record_json)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    &[
                        &record.outbox_id,
                        &record.task_id,
                        &task_time_i64(record.revision, "revision")?,
                        &record.event_kind,
                        &task_time_i64(record.created_at_ms, "created_at_ms")?,
                        &record
                            .projected_at_ms
                            .map(|value| task_time_i64(value, "projected_at_ms"))
                            .transpose()?,
                        &record_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
        }
        for record in &snapshot.assignment_outbox {
            let record_json = serde_json::to_value(record).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_task_mission_assignment_outbox(
                        operation_id,created_at_ms,projected_at_ms,record_json
                     ) VALUES($1,$2,$3,$4)",
                    &[
                        &record.operation_id,
                        &task_time_i64(record.created_at_ms, "created_at_ms")?,
                        &record
                            .projected_at_ms
                            .map(|value| task_time_i64(value, "projected_at_ms"))
                            .transpose()?,
                        &record_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
        }
        for decision in &snapshot.organization_decisions {
            let record_json = serde_json::to_value(decision).map_err(|error| error.to_string())?;
            transaction
                .execute(
                    "INSERT INTO runtime_mission_organization_decisions(
                        decision_id,status,next_attempt_at_ms,created_at_ms,updated_at_ms,record_json
                     ) VALUES($1,$2,$3,$4,$5,$6)",
                    &[
                        &decision.decision_id,
                        &task_organization_status_name(decision.status),
                        &task_time_i64(decision.next_attempt_at_ms, "next_attempt_at_ms")?,
                        &task_time_i64(decision.created_at_ms, "created_at_ms")?,
                        &task_time_i64(decision.updated_at_ms, "updated_at_ms")?,
                        &record_json,
                    ],
                )
                .map_err(|error| error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())
    }
}

/// Copy a quiesced Task control plane exactly once, prove canonical digest
/// equality, then atomically write a backend-neutral cutover manifest.
pub fn copy_quiesced_task_service(
    source: &TaskAggregateService,
    target: &TaskAggregateService,
    manifest_path: impl AsRef<Path>,
) -> Result<TaskMigrationManifest, String> {
    let snapshot = source.export_migration_snapshot()?;
    snapshot.validate()?;
    let source_digest = snapshot.canonical_digest()?;
    target.import_migration_snapshot(&snapshot)?;
    let target_snapshot = target.export_migration_snapshot()?;
    let target_digest = target_snapshot.canonical_digest()?;
    if source_digest != target_digest {
        return Err("task migration digest mismatch".to_string());
    }
    let manifest = TaskMigrationManifest {
        domain: TASK_DOMAIN.to_string(),
        source_digest,
        target_digest,
        task_count: snapshot.tasks.len(),
    };
    write_task_migration_manifest(manifest_path.as_ref(), &manifest)?;
    Ok(manifest)
}

fn sync_task_graph_refs_postgres(
    transaction: &mut impl PostgresClient,
    task: &TaskAggregate,
) -> Result<(), String> {
    transaction
        .execute(
            "DELETE FROM runtime_task_graph_refs WHERE task_id=$1",
            &[&task.task_id],
        )
        .map_err(|error| error.to_string())?;
    for reference in &task.graph_refs {
        transaction
            .execute(
                "INSERT INTO runtime_task_graph_refs(task_id, graph_id, graph_revision)
                 VALUES ($1, $2, $3)",
                &[
                    &task.task_id,
                    &reference.graph_id,
                    &task_time_i64(reference.revision, "graph_revision")?,
                ],
            )
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn task_record_from_row(row: &Row) -> Result<TaskAggregate, String> {
    let record_json: Value = row.try_get(0).map_err(|error| error.to_string())?;
    serde_json::from_value(record_json).map_err(|error| error.to_string())
}

fn task_outbox_from_row(row: &Row) -> Result<TaskEvidenceOutboxRecord, String> {
    let record_json: Value = row.try_get(0).map_err(|error| error.to_string())?;
    serde_json::from_value(record_json).map_err(|error| error.to_string())
}

fn task_binding_from_row(row: &Row) -> Result<TaskTurnBinding, String> {
    let record_json: Value = row.try_get(0).map_err(|error| error.to_string())?;
    serde_json::from_value(record_json).map_err(|error| error.to_string())
}

const fn task_turn_role_name(role: runtime::TaskTurnRole) -> &'static str {
    match role {
        runtime::TaskTurnRole::Primary => "primary",
        runtime::TaskTurnRole::Additional => "additional",
        runtime::TaskTurnRole::Review => "review",
        runtime::TaskTurnRole::Handoff => "handoff",
    }
}

fn task_time_i64(value: u64, field: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("task `{field}` exceeds i64"))
}

fn task_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn validate_task_assignment_replay(
    command: &TaskMissionAssignmentCommand,
    receipt: &TaskMissionAssignmentReceipt,
) -> Result<(), String> {
    let requested = command.task_ids.iter().collect::<BTreeSet<_>>();
    let committed = receipt.task_revisions.keys().collect::<BTreeSet<_>>();
    if receipt.operation_id != command.operation_id
        || receipt.target_mission_id != command.target_mission_id
        || receipt.assignment != command.assignment
        || requested != committed
    {
        return Err(format!(
            "task Mission assignment operation `{}` was reused with a different command",
            command.operation_id
        ));
    }
    Ok(())
}

const fn task_organization_status_name(status: MissionOrganizationStatus) -> &'static str {
    match status {
        MissionOrganizationStatus::Pending => "pending",
        MissionOrganizationStatus::Claimed => "claimed",
        MissionOrganizationStatus::Applied => "applied",
        MissionOrganizationStatus::Rejected => "rejected",
        MissionOrganizationStatus::Failed => "failed",
    }
}

fn write_task_migration_manifest(
    manifest_path: &Path,
    manifest: &TaskMigrationManifest,
) -> Result<(), String> {
    if let Some(parent) = manifest_path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let temporary_path = PathBuf::from(format!(
        "{}.{}.tmp",
        manifest_path.display(),
        uuid::Uuid::new_v4()
    ));
    fs::write(
        &temporary_path,
        serde_json::to_vec_pretty(manifest).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    fs::rename(temporary_path, manifest_path).map_err(|error| error.to_string())
}

/// PostgreSQL compact-tier and metadata adapter for Runtime artifacts.
#[derive(Clone, Debug)]
pub struct PostgresArtifactRepository {
    executor: PostgresExecutor,
}

impl PostgresArtifactRepository {
    pub fn new(executor: PostgresExecutor) -> Result<Self, String> {
        executor
            .apply_migrations(ARTIFACT_DOMAIN, ARTIFACT_MIGRATIONS)
            .map_err(|error| error.to_string())?;
        Ok(Self { executor })
    }
}

impl runtime::ArtifactMetadataRepository for PostgresArtifactRepository {
    fn put_object(&self, object: &runtime::ArtifactObjectRecord) -> Result<bool, String> {
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        connection
            .execute(
                "INSERT INTO artifact_objects
                 (sha256, bytes, tier, compact_body, created_at_ms)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT(sha256) DO NOTHING",
                &[
                    &object.sha256,
                    &artifact_to_i64(object.bytes)?,
                    &artifact_tier_name(&object.tier),
                    &object.compact_body,
                    &artifact_to_i64(object.created_at_ms)?,
                ],
            )
            .map(|changed| changed == 1)
            .map_err(|error| error.to_string())
    }

    fn object(&self, sha256: &str) -> Result<Option<runtime::ArtifactObjectRecord>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        connection
            .query_opt(
                "SELECT sha256, bytes, tier, compact_body, created_at_ms
                 FROM artifact_objects WHERE sha256=$1",
                &[&sha256],
            )
            .map_err(|error| error.to_string())?
            .map(|row| artifact_object_from_row(&row))
            .transpose()
    }

    fn put_record(&self, record: &runtime::ArtifactRecord) -> Result<(), String> {
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        connection
            .execute(
                "INSERT INTO artifact_records
                 (artifact_id, sha256, bytes, media_type, visibility_scope, tier,
                  created_at_ms, last_access_at_ms)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                &[
                    &record.artifact_id,
                    &record.sha256,
                    &artifact_to_i64(record.bytes)?,
                    &record.media_type,
                    &record.visibility_scope,
                    &artifact_tier_name(&record.tier),
                    &artifact_to_i64(record.created_at_ms)?,
                    &artifact_to_i64(record.last_access_at_ms)?,
                ],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn record(&self, artifact_id: &str) -> Result<Option<runtime::ArtifactRecord>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        connection
            .query_opt(
                "SELECT artifact_id, sha256, bytes, media_type, visibility_scope, tier,
                        created_at_ms, last_access_at_ms
                 FROM artifact_records WHERE artifact_id=$1",
                &[&artifact_id],
            )
            .map_err(|error| error.to_string())?
            .map(|row| artifact_record_from_row(&row))
            .transpose()
    }

    fn touch(&self, artifact_id: &str, at_ms: u64) -> Result<(), String> {
        self.executor
            .checkout_critical()
            .map_err(|error| error.to_string())?
            .execute(
                "UPDATE artifact_records SET last_access_at_ms=$2 WHERE artifact_id=$1",
                &[&artifact_id, &artifact_to_i64(at_ms)?],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn remove_record(&self, artifact_id: &str) -> Result<(), String> {
        self.executor
            .checkout_critical()
            .map_err(|error| error.to_string())?
            .execute(
                "DELETE FROM artifact_records WHERE artifact_id=$1",
                &[&artifact_id],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn unreferenced_objects_before(
        &self,
        before_ms: u64,
        limit: usize,
    ) -> Result<Vec<runtime::ArtifactObjectRecord>, String> {
        self.executor
            .checkout_background()
            .map_err(|error| error.to_string())?
            .query(
                "SELECT object.sha256, object.bytes, object.tier, object.compact_body,
                        object.created_at_ms
                 FROM artifact_objects object
                 LEFT JOIN artifact_records record ON record.sha256=object.sha256
                 WHERE record.artifact_id IS NULL AND object.created_at_ms <= $1
                 ORDER BY object.created_at_ms ASC LIMIT $2",
                &[
                    &artifact_to_i64(before_ms)?,
                    &artifact_to_i64(limit as u64)?,
                ],
            )
            .map_err(|error| error.to_string())?
            .iter()
            .map(artifact_object_from_row)
            .collect()
    }

    fn remove_object(&self, sha256: &str) -> Result<(), String> {
        self.executor
            .checkout_critical()
            .map_err(|error| error.to_string())?
            .execute(
                "DELETE FROM artifact_objects
                 WHERE sha256=$1
                 AND NOT EXISTS (
                    SELECT 1 FROM artifact_records WHERE artifact_records.sha256=$1
                 )",
                &[&sha256],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn pin(&self, artifact_id: &str, owner: &str, until_ms: u64) -> Result<(), String> {
        self.executor
            .checkout_critical()
            .map_err(|error| error.to_string())?
            .execute(
                "INSERT INTO artifact_pins (artifact_id, owner, until_ms)
                 VALUES ($1, $2, $3)
                 ON CONFLICT(artifact_id, owner)
                 DO UPDATE SET until_ms=EXCLUDED.until_ms",
                &[&artifact_id, &owner, &artifact_to_i64(until_ms)?],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn unpin(&self, artifact_id: &str, owner: &str) -> Result<(), String> {
        self.executor
            .checkout_critical()
            .map_err(|error| error.to_string())?
            .execute(
                "DELETE FROM artifact_pins WHERE artifact_id=$1 AND owner=$2",
                &[&artifact_id, &owner],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn is_pinned(&self, artifact_id: &str, at_ms: u64) -> Result<bool, String> {
        self.executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?
            .query_one(
                "SELECT EXISTS(
                    SELECT 1 FROM artifact_pins
                    WHERE artifact_id=$1 AND until_ms>$2
                 )",
                &[&artifact_id, &artifact_to_i64(at_ms)?],
            )
            .map(|row| row.get(0))
            .map_err(|error| error.to_string())
    }

    fn stats(&self, at_ms: u64) -> Result<runtime::ArtifactStoreStats, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let object_row = connection
            .query_one(
                "SELECT COUNT(*), COALESCE(SUM(bytes), 0)::BIGINT,
                        COALESCE(SUM(CASE WHEN tier='compact' THEN bytes ELSE 0 END), 0)::BIGINT,
                        COALESCE(SUM(CASE WHEN tier='blob' THEN bytes ELSE 0 END), 0)::BIGINT
                 FROM artifact_objects",
                &[],
            )
            .map_err(|error| error.to_string())?;
        let artifacts = connection
            .query_one("SELECT COUNT(*) FROM artifact_records", &[])
            .map_err(|error| error.to_string())?
            .get::<_, i64>(0);
        let pins = connection
            .query_one(
                "SELECT COUNT(*) FROM artifact_pins WHERE until_ms>$1",
                &[&artifact_to_i64(at_ms)?],
            )
            .map_err(|error| error.to_string())?
            .get::<_, i64>(0);
        Ok(runtime::ArtifactStoreStats {
            objects: artifact_from_i64(object_row.get(0), "objects")?,
            artifacts: artifact_from_i64(artifacts, "artifacts")?,
            physical_bytes: artifact_from_i64(object_row.get(1), "physical_bytes")?,
            compact_bytes: artifact_from_i64(object_row.get(2), "compact_bytes")?,
            blob_bytes: artifact_from_i64(object_row.get(3), "blob_bytes")?,
            pins: artifact_from_i64(pins, "pins")?,
        })
    }
}

fn artifact_object_from_row(row: &Row) -> Result<runtime::ArtifactObjectRecord, String> {
    Ok(runtime::ArtifactObjectRecord {
        sha256: row.get(0),
        bytes: artifact_from_i64(row.get(1), "bytes")?,
        tier: artifact_tier(row.get::<_, String>(2).as_str())?,
        compact_body: row.get(3),
        created_at_ms: artifact_from_i64(row.get(4), "created_at_ms")?,
    })
}

fn artifact_record_from_row(row: &Row) -> Result<runtime::ArtifactRecord, String> {
    Ok(runtime::ArtifactRecord {
        artifact_id: row.get(0),
        sha256: row.get(1),
        bytes: artifact_from_i64(row.get(2), "bytes")?,
        media_type: row.get(3),
        visibility_scope: row.get(4),
        tier: artifact_tier(row.get::<_, String>(5).as_str())?,
        created_at_ms: artifact_from_i64(row.get(6), "created_at_ms")?,
        last_access_at_ms: artifact_from_i64(row.get(7), "last_access_at_ms")?,
    })
}

fn artifact_tier_name(tier: &runtime::ArtifactObjectTier) -> &'static str {
    match tier {
        runtime::ArtifactObjectTier::Compact => "compact",
        runtime::ArtifactObjectTier::Blob => "blob",
    }
}

fn artifact_tier(value: &str) -> Result<runtime::ArtifactObjectTier, String> {
    match value {
        "compact" => Ok(runtime::ArtifactObjectTier::Compact),
        "blob" => Ok(runtime::ArtifactObjectTier::Blob),
        value => Err(format!("unknown artifact tier `{value}`")),
    }
}

fn artifact_to_i64(value: u64) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("artifact integer {value} exceeds PostgreSQL BIGINT"))
}

fn artifact_from_i64(value: i64, field: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("artifact field `{field}` is negative"))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use runtime::RuntimeServices;
    use storage::StaticSecretRefResolver;

    use super::*;

    #[test]
    fn runtime_event_initial_migration_remains_immutable() {
        let initial = RUNTIME_EVENT_MIGRATIONS
            .iter()
            .find(|migration| migration.id == "runtime_event.0001.initial")
            .expect("initial Runtime event migration exists");

        assert_eq!(
            initial.checksum(),
            "c29d153132dcd497b6665b9f7a1cbe376d5ce1f39f2f37db308963bb1bc3bd3d"
        );
        assert!(RUNTIME_EVENT_MIGRATIONS
            .iter()
            .any(|migration| migration.id == "runtime_event.0010.activity-identity-index"));
    }

    fn input(stream_id: &str, scope: RuntimeEventScope, kind: &str) -> RuntimeEventInput {
        RuntimeEventInput {
            stream_id: stream_id.to_string(),
            scope,
            kind: kind.to_string(),
            status: Some("running".to_string()),
            actor: Some("runtime-postgres-test".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({"kind": kind}),
        }
    }

    fn open_real_store() -> (RuntimeEventStore, String) {
        let url =
            std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
        let resolver = StaticSecretRefResolver::new([("test.pg".to_string(), url.clone())]);
        let store = PostgresRuntimeEventStore::connect(
            PostgresConnectionConfig::new(
                "runtime-event-test",
                "test.pg",
                "cowd-runtime-event-postgres-contract",
            ),
            &resolver,
        )
        .expect("postgres runtime event store opens")
        .into_runtime_event_store();
        (store, url)
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn projection_work_class_maps_background_without_downgrading_recovery() {
        let url =
            std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
        let resolver = StaticSecretRefResolver::new([("projection-lanes.pg".to_string(), url)]);
        let pool_set = storage::PostgresPoolSet::connect(
            storage::PostgresPoolSetConfig {
                connection: PostgresConnectionConfig::new(
                    "runtime-projection-lanes",
                    "projection-lanes.pg",
                    "cowd-runtime-projection-lanes",
                ),
                server_reserve: 1,
                critical: storage::PostgresPoolLaneConfig::new(2, Some(1), 1_000),
                online_read: storage::PostgresPoolLaneConfig::new(2, Some(1), 1_000),
                background: storage::PostgresPoolLaneConfig::new(1, Some(1), 250),
            },
            &resolver,
        )
        .expect("isolated pool set");
        let executor = pool_set.executor();
        let store = PostgresRuntimeEventStore::new(executor.clone())
            .expect("runtime store")
            .into_runtime_event_store();
        let before = executor.health();
        store.run_projection_work(runtime::RuntimeProjectionWorkClass::Background, || {
            store
                .append(input(
                    "projection:background",
                    RuntimeEventScope::Evolution,
                    "projection.background",
                ))
                .unwrap();
            store.events_after_cursor(0, 1).unwrap();
            store
                .put_projection_checkpoint(
                    "projector:lane-proof",
                    1,
                    &serde_json::json!({"ok": true}),
                    1,
                )
                .unwrap();
        });
        let after_background = executor.health();
        let delta = |health: &storage::PostgresExecutorHealth,
                     workload: storage::PostgresWorkloadClass| {
            let current = health
                .lanes
                .iter()
                .find(|lane| lane.workload == workload)
                .unwrap()
                .metrics
                .checkout_count;
            let prior = before
                .lanes
                .iter()
                .find(|lane| lane.workload == workload)
                .unwrap()
                .metrics
                .checkout_count;
            current.saturating_sub(prior)
        };
        assert!(
            delta(
                &after_background,
                storage::PostgresWorkloadClass::Background
            ) >= 3
        );
        assert_eq!(
            delta(&after_background, storage::PostgresWorkloadClass::Critical),
            0
        );
        assert_eq!(
            delta(
                &after_background,
                storage::PostgresWorkloadClass::OnlineRead
            ),
            0
        );

        store.run_projection_work(runtime::RuntimeProjectionWorkClass::Recovery, || {
            store.events_after_cursor(0, 1).unwrap();
            store
                .append(input(
                    "projection:recovery",
                    RuntimeEventScope::Recovery,
                    "projection.recovery",
                ))
                .unwrap();
        });
        let after_recovery = executor.health();
        assert!(
            after_recovery
                .lanes
                .iter()
                .find(|lane| lane.workload == storage::PostgresWorkloadClass::OnlineRead)
                .unwrap()
                .metrics
                .checkout_count
                > after_background
                    .lanes
                    .iter()
                    .find(|lane| lane.workload == storage::PostgresWorkloadClass::OnlineRead)
                    .unwrap()
                    .metrics
                    .checkout_count
        );
        assert!(
            after_recovery
                .lanes
                .iter()
                .find(|lane| lane.workload == storage::PostgresWorkloadClass::Critical)
                .unwrap()
                .metrics
                .checkout_count
                > after_background
                    .lanes
                    .iter()
                    .find(|lane| lane.workload == storage::PostgresWorkloadClass::Critical)
                    .unwrap()
                    .metrics
                    .checkout_count
        );
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn postgres_runtime_event_store_preserves_fences_outbox_restart_and_runtime_composition() {
        let (store, url) = open_real_store();
        let sqlite_source = RuntimeEventStore::try_open_in_memory().expect("SQLite source opens");
        sqlite_source
            .append_transaction(AppendTransactionRequest {
                transaction_id: "copy-source-transaction".to_string(),
                expected_streams: vec![
                    ExpectedStreamRevision {
                        stream_id: "copy:stream".to_string(),
                        expected_revision: 0,
                    },
                    ExpectedStreamRevision {
                        stream_id: "copy:empty-stream".to_string(),
                        expected_revision: 0,
                    },
                ],
                events: vec![input(
                    "copy:stream",
                    RuntimeEventScope::Recovery,
                    "migration.source_seeded",
                )
                .into()],
            })
            .expect("source event");
        let manifest_root = tempfile::tempdir().expect("migration manifest root");
        let manifest_path = manifest_root.path().join("runtime-event-cutover.json");
        let copy = copy_quiesced_runtime_event_store(&sqlite_source, &store, &manifest_path)
            .expect("SQLite to PostgreSQL migration copy");
        assert_eq!(copy.source_digest, copy.target_digest);
        assert!(manifest_path.is_file());
        assert_eq!(
            store
                .export_migration_snapshot()
                .expect("target snapshot")
                .canonical_digest()
                .expect("target digest"),
            sqlite_source
                .export_migration_snapshot()
                .expect("source snapshot")
                .canonical_digest()
                .expect("source digest")
        );
        let store = Arc::new(store);
        store
            .append(input(
                "graph:concurrent",
                RuntimeEventScope::ExecutionGraph,
                "graph.seeded",
            ))
            .expect("seed append");
        store
            .append(input(
                "evolution:signal:prefix-contract",
                RuntimeEventScope::Evolution,
                "evolution.signal.recorded.v1",
            ))
            .expect("prefix target append");
        store
            .append(input(
                "evolution:signal-other",
                RuntimeEventScope::Evolution,
                "evolution.signal.recorded.v1",
            ))
            .expect("prefix neighbour append");
        store
            .append(input(
                "evolution:mission:prefix-contract",
                RuntimeEventScope::Evolution,
                "evolution.mission.created.v1",
            ))
            .expect("different prefix append");
        let prefix_events = store
            .replay_scope_stream_prefix(RuntimeEventScope::Evolution, "evolution:signal:")
            .expect("prefix replay must be independent of database collation");
        assert_eq!(prefix_events.len(), 1);
        assert_eq!(
            prefix_events[0].stream_id,
            "evolution:signal:prefix-contract"
        );

        let barrier = Arc::new(Barrier::new(2));
        let writers = (0..2)
            .map(|writer| {
                let barrier = Arc::clone(&barrier);
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    barrier.wait();
                    store.append_transaction(AppendTransactionRequest {
                        transaction_id: format!("concurrent-writer-{writer}"),
                        expected_streams: vec![ExpectedStreamRevision {
                            stream_id: "graph:concurrent".to_string(),
                            expected_revision: 1,
                        }],
                        events: vec![RuntimeTransactionEventInput {
                            event: input(
                                "graph:concurrent",
                                RuntimeEventScope::ExecutionGraph,
                                "graph.concurrent",
                            ),
                            idempotency_key: Some(format!("writer-{writer}")),
                            schema_version: 1,
                        }],
                    })
                })
            })
            .collect::<Vec<_>>();
        let outcomes = writers
            .into_iter()
            .map(|writer| writer.join().expect("writer thread"))
            .collect::<Vec<_>>();
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(RuntimeEventStoreError::StaleRevision { .. })
                ))
                .count(),
            1
        );
        assert_eq!(store.stream_revision("graph:concurrent").unwrap(), 2);
        let commit_cursor_before_checkpoint = *store.subscribe_commits().borrow();
        let checkpoint = store
            .put_projection_checkpoint(
                "projector:postgres-contract",
                commit_cursor_before_checkpoint,
                &serde_json::json!({"cursor": commit_cursor_before_checkpoint}),
                100,
            )
            .expect("mutable projection checkpoint");
        assert_eq!(checkpoint.revision, 1);
        assert_eq!(
            *store.subscribe_commits().borrow(),
            commit_cursor_before_checkpoint,
            "mutable projection checkpoints must not emit journal commits"
        );
        assert_eq!(
            store
                .projection_checkpoint("projector:postgres-contract")
                .expect("read checkpoint")
                .expect("checkpoint exists"),
            checkpoint
        );
        assert!(matches!(
            store.put_projection_checkpoint(
                "projector:postgres-contract",
                commit_cursor_before_checkpoint.saturating_sub(1),
                &serde_json::json!({"cursor": "stale"}),
                101,
            ),
            Err(RuntimeEventStoreError::StaleRevision { .. })
        ));

        let terminal_request = AppendTransactionRequest {
            transaction_id: "terminal-transaction".to_string(),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: "session-input:real".to_string(),
                expected_revision: 0,
            }],
            events: vec![RuntimeTransactionEventInput {
                event: input(
                    "session-input:real",
                    RuntimeEventScope::SessionInput,
                    "runtime.session.terminal_requested",
                ),
                idempotency_key: Some("terminal-request".to_string()),
                schema_version: 1,
            }],
        };
        let terminal_input = SessionTerminalInput {
            terminal_id: "terminal-real".to_string(),
            message_id: "message-real".to_string(),
            session_id: "session-real".to_string(),
            execution_id: Some("execution-real".to_string()),
            turn_id: Some("turn-real".to_string()),
            request_id: Some("request-real".to_string()),
            session_generation: Some(1),
            input_sequence: Some(0),
            input_claim_owner: Some("worker-real".to_string()),
            input_claim_token: Some("claim-real".to_string()),
            input_claim_revision: Some(3),
            payload_ref: "payload-real".to_string(),
        };
        let terminal_receipt = store
            .append_transaction_with_terminal(terminal_request.clone(), terminal_input.clone())
            .expect("terminal transaction");
        assert!(terminal_receipt.commit_cursor > 0);

        let claim_barrier = Arc::new(Barrier::new(2));
        let workers = (0..2)
            .map(|worker| {
                let barrier = Arc::clone(&claim_barrier);
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    barrier.wait();
                    store.claim_session_terminals(&format!("worker-{worker}"), 100, 1_000, 1)
                })
            })
            .collect::<Vec<_>>();
        let claims = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker thread").expect("claim"))
            .collect::<Vec<_>>();
        let claimed = claims.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(claimed.len(), 1);
        let claim = &claimed[0];
        assert_eq!(claim.request_id.as_deref(), Some("request-real"));
        assert_eq!(claim.session_generation, Some(1));
        assert_eq!(claim.input_sequence, Some(0));
        assert_eq!(claim.input_claim_owner.as_deref(), Some("worker-real"));
        assert_eq!(claim.input_claim_token.as_deref(), Some("claim-real"));
        assert_eq!(claim.input_claim_revision, Some(3));
        assert_eq!(claim.status, "claimed");
        assert_eq!(claim.attempts, 1);
        assert_eq!(claim.claim_expires_at_ms, Some(1_100));
        drop(store);
        let crash_resolver = StaticSecretRefResolver::new([("test.pg".to_string(), url.clone())]);
        let store = Arc::new(
            PostgresRuntimeEventStore::connect(
                PostgresConnectionConfig::new(
                    "runtime-event-crash-recovery-test",
                    "test.pg",
                    "cowd-runtime-event-postgres-crash-recovery-contract",
                ),
                &crash_resolver,
            )
            .expect("postgres event store reopens after delivery crash")
            .into_runtime_event_store(),
        );
        assert!(matches!(
            store.adopt_session_terminal_fence(&RuntimeSessionTerminalFenceAdoption {
                terminal_id: claim.terminal_id.clone(),
                expected_terminal_revision: claim.revision,
                request_id: "request-real".to_string(),
                session_id: "session-real".to_string(),
                turn_id: "turn-real".to_string(),
                session_generation: 1,
                input_sequence: 0,
                claim_owner: "session-worker-reclaimed".to_string(),
                claim_token: "session-claim-reclaimed".to_string(),
                claim_revision: 5,
                claim_expires_at_ms: 2_000,
                adopted_at_ms: 1_099,
            }),
            Err(RuntimeEventStoreError::InvalidTransaction(_))
        ));
        let adoption = RuntimeSessionTerminalFenceAdoption {
            terminal_id: claim.terminal_id.clone(),
            expected_terminal_revision: claim.revision,
            request_id: "request-real".to_string(),
            session_id: "session-real".to_string(),
            turn_id: "turn-real".to_string(),
            session_generation: 1,
            input_sequence: 0,
            claim_owner: "session-worker-reclaimed".to_string(),
            claim_token: "session-claim-reclaimed".to_string(),
            claim_revision: 5,
            claim_expires_at_ms: 2_000,
            adopted_at_ms: 1_100,
        };
        let adopted = store
            .adopt_session_terminal_fence(&adoption)
            .expect("expired delivery claim adopts reclaimed Session fence");
        assert_eq!(adopted.status, "pending");
        assert_eq!(adopted.input_claim_revision, Some(5));
        let replay = store
            .append_transaction_with_terminal(terminal_request.clone(), terminal_input.clone())
            .expect("initial terminal transaction replays after fence adoption");
        assert!(replay.duplicate);
        let mut conflicting_initial_fence = terminal_input;
        conflicting_initial_fence.input_claim_token = Some("different-initial-fence".to_string());
        assert!(matches!(
            store.append_transaction_with_terminal(terminal_request, conflicting_initial_fence),
            Err(RuntimeEventStoreError::TransactionConflict { .. })
        ));
        assert_eq!(
            store
                .adopt_session_terminal_fence(&adoption)
                .expect("adoption replay")
                .revision,
            adopted.revision
        );
        let claim = store
            .claim_session_terminals("worker-after-adoption", 1_101, 1_000, 1)
            .expect("claim adopted terminal")
            .remove(0);
        let materialized = store
            .ack_session_terminal(
                &claim.terminal_id,
                claim.claim_owner.as_deref().expect("claim owner"),
                claim.revision,
                1_102,
            )
            .expect("ack claimed terminal");
        assert_eq!(materialized.status, "materialized");
        assert_eq!(
            store
                .materialized_session_terminals_after("session-real", 0, 10)
                .unwrap()
                .len(),
            1
        );
        assert!(matches!(
            store.enqueue_session_terminal(
                "terminal-unfenced",
                "message-unfenced",
                "session-real",
                terminal_receipt.commit_cursor,
                "payload-unfenced",
            ),
            Err(RuntimeEventStoreError::InvalidTransaction(_))
        ));

        let duplicate_request = AppendTransactionRequest {
            transaction_id: "duplicate-transaction".to_string(),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: "graph:duplicate".to_string(),
                expected_revision: 0,
            }],
            events: vec![RuntimeTransactionEventInput {
                event: input(
                    "graph:duplicate",
                    RuntimeEventScope::ExecutionGraph,
                    "graph.duplicate",
                ),
                idempotency_key: Some("duplicate".to_string()),
                schema_version: 1,
            }],
        };
        assert!(
            !store
                .append_transaction(duplicate_request.clone())
                .expect("first idempotent transaction")
                .duplicate
        );
        assert!(
            store
                .append_transaction(duplicate_request)
                .expect("duplicate transaction")
                .duplicate
        );

        drop(store);
        let resolver = StaticSecretRefResolver::new([("test.pg".to_string(), url)]);
        let reopened = Arc::new(
            PostgresRuntimeEventStore::connect(
                PostgresConnectionConfig::new(
                    "runtime-event-reopen-test",
                    "test.pg",
                    "cowd-runtime-event-postgres-reopen-contract",
                ),
                &resolver,
            )
            .expect("postgres event store reopens")
            .into_runtime_event_store(),
        );
        assert_eq!(reopened.stream_revision("graph:concurrent").unwrap(), 2);
        let terminal = reopened
            .session_terminal("terminal-real")
            .unwrap()
            .expect("terminal persists");
        assert_eq!(terminal.status, "materialized");
        assert_eq!(terminal.execution_id.as_deref(), Some("execution-real"));
        assert_eq!(terminal.turn_id.as_deref(), Some("turn-real"));

        let temp = tempfile::tempdir().expect("temporary Runtime host");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace exists");
        let services = RuntimeServices::builder(temp.path().join("home"), &workspace)
            .runtime_event_store(reopened)
            .build()
            .expect("RuntimeServices composes PostgreSQL event backend");
        let mission_id = services.mission_runtime().default_mission_id().to_string();
        services
            .task_runtime_port()
            .create(harness_contract::task::TaskCreateCommand {
                task_id: "postgres-composed".to_string(),
                mission_id,
                kind: TaskKind::Root,
                origin: TaskOrigin::User,
                origin_session_id: "session:postgres-composed".to_string(),
                origin_turn_id: "turn:postgres-composed".to_string(),
                root_task_id: "postgres-composed".to_string(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: TaskMissionAssignment::Default,
                mission_assigned_by: "test".to_string(),
                spec: harness_contract::task::TaskSpec::new(
                    "prove canonical Task outbox reaches PostgreSQL event store",
                ),
                evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                    "test_fixture",
                    "test://runtime-postgres/composed-task",
                )],
            })
            .expect("canonical Task outbox reaches PostgreSQL event backend");
        assert!(services
            .event_reader()
            .list_stream("task:postgres-composed")
            .expect("read composed event")
            .iter()
            .any(|event| event.kind == "task.created"));
    }

    #[test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    fn postgres_task_store_preserves_migration_restart_and_per_task_concurrency() {
        let url =
            std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
        let temp = tempfile::tempdir().expect("temporary task migration root");
        let source_path = temp.path().join("source-tasks.db");
        let source = TaskAggregateService::open(source_path).expect("SQLite task source opens");
        let source_task = source
            .create(TaskCreateCommand {
                task_id: "task-pg-migration".to_string(),
                mission_id: "mission-pg-migration".to_string(),
                kind: TaskKind::Root,
                origin: TaskOrigin::User,
                origin_session_id: "session-pg-migration".to_string(),
                origin_turn_id: "turn-pg-migration".to_string(),
                root_task_id: "task-pg-migration".to_string(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: TaskMissionAssignment::Default,
                mission_assigned_by: "test".to_string(),
                spec: TaskSpec::new("Migrate the task control plane"),
                evidence_refs: vec![EvidenceRef::observed(
                    "test_fixture",
                    "test://runtime-postgres/task-migration",
                )],
            })
            .expect("source task starts")
            .aggregate;
        let phase = source
            .start_phase(
                &source_task.task_id,
                source_task.revision,
                TaskPhaseSpec {
                    name: "postgres-verification".to_string(),
                    objective: "prove target preserves the task record".to_string(),
                    dependency_refs: Vec::new(),
                    plan: vec!["copy task snapshot".to_string()],
                    acceptance: vec!["digest equality".to_string()],
                    test_commands: vec!["real PostgreSQL task test".to_string()],
                },
                Vec::new(),
            )
            .expect("source phase starts")
            .aggregate;
        let phase_id = phase.phases.last().expect("phase exists").phase_id.clone();
        source
            .record_phase_artifact(
                &source_task.task_id,
                phase.revision,
                &phase_id,
                "evidence",
                "migration",
                "source snapshot is canonical",
                Vec::new(),
            )
            .expect("source artifact persists");

        let resolver = StaticSecretRefResolver::new([("task.pg".to_string(), url.clone())]);
        let pg_store = PostgresTaskStore::connect(
            PostgresConnectionConfig::new(
                "runtime-task-test",
                "task.pg",
                "cowd-runtime-task-postgres-contract",
            ),
            &resolver,
        )
        .expect("postgres task store opens");
        let executor = pg_store.executor().clone();
        let target = Arc::new(pg_store.into_task_service());
        let manifest_path = temp.path().join("task-migration-manifest.json");
        let manifest = copy_quiesced_task_service(&source, target.as_ref(), &manifest_path)
            .expect("quiesced SQLite to PostgreSQL copy succeeds");
        assert_eq!(manifest.source_digest, manifest.target_digest);
        assert_eq!(manifest.task_count, 1);
        assert!(manifest_path.is_file());
        assert_eq!(
            source
                .export_migration_snapshot()
                .expect("source snapshot")
                .canonical_digest()
                .expect("source digest"),
            target
                .export_migration_snapshot()
                .expect("target snapshot")
                .canonical_digest()
                .expect("target digest")
        );

        let barrier = Arc::new(Barrier::new(2));
        let workers = (0..2)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                let target = Arc::clone(&target);
                thread::spawn(move || {
                    barrier.wait();
                    target.create(TaskCreateCommand {
                        task_id: "task-pg-concurrent".to_string(),
                        mission_id: "mission-pg-concurrent".to_string(),
                        kind: TaskKind::Root,
                        origin: TaskOrigin::User,
                        origin_session_id: "session-pg-concurrent".to_string(),
                        origin_turn_id: "turn-pg-concurrent".to_string(),
                        root_task_id: "task-pg-concurrent".to_string(),
                        parent_task_id: None,
                        predecessor_task_id: None,
                        mission_assignment: TaskMissionAssignment::Default,
                        mission_assigned_by: "test".to_string(),
                        spec: TaskSpec::new("one governed concurrent task"),
                        evidence_refs: Vec::new(),
                    })
                })
            })
            .collect::<Vec<_>>();
        let results = workers
            .into_iter()
            .map(|worker| worker.join().expect("task worker joins"))
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 2);
        let receipts = results
            .into_iter()
            .map(|result| result.expect("concurrent create replays canonical receipt"))
            .collect::<Vec<_>>();
        assert_eq!(receipts[0].receipt, receipts[1].receipt);
        assert_eq!(receipts[0].outbox, receipts[1].outbox);
        let concurrent = target
            .list()
            .expect("target task list")
            .into_iter()
            .find(|task| task.task_id == "task-pg-concurrent")
            .expect("one concurrent task persists");
        assert_eq!(concurrent.objective, "one governed concurrent task");
        assert!(target
            .create(TaskCreateCommand {
                task_id: "task-pg-concurrent".to_string(),
                mission_id: "mission-pg-concurrent".to_string(),
                kind: TaskKind::Root,
                origin: TaskOrigin::User,
                origin_session_id: "session-pg-concurrent".to_string(),
                origin_turn_id: "turn-pg-concurrent".to_string(),
                root_task_id: "task-pg-concurrent".to_string(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: TaskMissionAssignment::Default,
                mission_assigned_by: "test".to_string(),
                spec: TaskSpec::new("a conflicting objective"),
                evidence_refs: Vec::new(),
            })
            .is_err());

        let organization = MissionOrganizationDecision {
            decision_id: "mission-organization:task-pg-concurrent".to_string(),
            workspace_id: "workspace-pg-contract".to_string(),
            root_task_id: "task-pg-concurrent".to_string(),
            affected_task_ids: vec!["task-pg-concurrent".to_string()],
            action: MissionOrganizationAction::KeepDefault,
            target_mission_id: "mission-pg-concurrent".to_string(),
            proposed_objective: None,
            status: MissionOrganizationStatus::Pending,
            reason: "verify immutable organization root".to_string(),
            candidate_count: 0,
            provider_invoked: false,
            provider_model: None,
            provider_input_tokens: 0,
            provider_output_tokens: 0,
            elapsed_ms: 0,
            rejected_reason: None,
            evidence_refs: vec![EvidenceRef::observed(
                "test_fixture",
                "test://runtime-postgres/organization-root",
            )],
            attempt: 0,
            next_attempt_at_ms: 1,
            claim_token: None,
            revision: 1,
            created_at_ms: 1,
            updated_at_ms: 1,
        };
        target
            .save_organization_decision(&organization, None)
            .expect("initial PostgreSQL organization decision persists");
        let mut clustered_replay = organization.clone();
        clustered_replay
            .affected_task_ids
            .push("task-pg-migration".to_string());
        let retained = target
            .save_organization_decision(&clustered_replay, None)
            .expect("mutable cluster membership does not break Root idempotency");
        assert_eq!(retained, organization);
        let mut foreign_root = clustered_replay;
        foreign_root.root_task_id = "task-pg-foreign".to_string();
        assert!(target
            .save_organization_decision(&foreign_root, None)
            .is_err());

        let reopened_resolver = StaticSecretRefResolver::new([("task.pg".to_string(), url)]);
        let reopened = PostgresTaskStore::connect(
            PostgresConnectionConfig::new(
                "runtime-task-reopen-test",
                "task.pg",
                "cowd-runtime-task-postgres-reopen-contract",
            ),
            &reopened_resolver,
        )
        .expect("postgres task store reopens")
        .into_task_service();
        let restored = reopened
            .list()
            .expect("reopened task list")
            .into_iter()
            .find(|task| task.task_id == source_task.task_id)
            .expect("migrated task survives reopen");
        assert!(restored
            .phases
            .iter()
            .any(|candidate| candidate.phase_id == phase_id && !candidate.artifacts.is_empty()));
        assert!(
            copy_quiesced_task_service(&source, &reopened, temp.path().join("rejected.json"))
                .is_err()
        );
        assert!(executor.health().metrics.checkout_count > 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    async fn postgres_artifact_repository_matches_sqlite_selector_and_scope_contract() {
        let url =
            std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let resolver = StaticSecretRefResolver::new([("artifact.pg".to_string(), url)]);
        let executor = PostgresExecutor::connect(
            PostgresConnectionConfig::new(
                format!("runtime-artifact-{suffix}"),
                "artifact.pg",
                format!("cowd-artifact-test-{suffix}"),
            ),
            &resolver,
        )
        .expect("PostgreSQL artifact executor opens");
        let repository = Arc::new(
            PostgresArtifactRepository::new(executor)
                .expect("PostgreSQL artifact migrations apply"),
        );
        let root = tempfile::tempdir().expect("artifact blob root");
        let store = runtime::ArtifactStore::new(
            root.path(),
            repository.clone(),
            runtime::ArtifactStoreConfig {
                compact_threshold_bytes: 8,
                max_object_bytes: 2 * 1024 * 1024,
                total_quota_bytes: 4 * 1024 * 1024,
                gc_high_water_bytes: 3 * 1024 * 1024,
                gc_low_water_bytes: 2 * 1024 * 1024,
                orphan_grace_ms: 0,
            },
        )
        .expect("PostgreSQL artifact store composes");
        let scope = format!("session:artifact-{suffix}");
        let artifact = store
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "application/octet-stream".to_string(),
                    visibility_scope: scope.clone(),
                    expected_bytes: Some(32),
                    original_name: Some("postgres.bin".to_string()),
                },
                &[0x44; 32],
            )
            .await
            .expect("PostgreSQL artifact write");
        assert!(artifact.selector.starts_with("artifact://"));
        assert_eq!(
            store
                .read(&artifact, &scope, Some(4..12))
                .await
                .expect("PostgreSQL artifact range read"),
            vec![0x44; 8]
        );
        assert!(matches!(
            store.read(&artifact, "session:other", None).await,
            Err(runtime::ArtifactError::Unauthorized)
        ));
        let second_root = tempfile::tempdir().expect("second artifact blob root");
        let second_repository: Arc<dyn runtime::ArtifactMetadataRepository> = repository.clone();
        let second_store = runtime::ArtifactStore::new(
            second_root.path(),
            second_repository,
            runtime::ArtifactStoreConfig {
                compact_threshold_bytes: 8,
                max_object_bytes: 2 * 1024 * 1024,
                total_quota_bytes: 4 * 1024 * 1024,
                gc_high_water_bytes: 3 * 1024 * 1024,
                gc_low_water_bytes: 2 * 1024 * 1024,
                orphan_grace_ms: 0,
            },
        )
        .expect("second PostgreSQL artifact store composes");
        let repeated = second_store
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "application/octet-stream".to_string(),
                    visibility_scope: scope.clone(),
                    expected_bytes: Some(32),
                    original_name: Some("postgres-repeat.bin".to_string()),
                },
                &[0x44; 32],
            )
            .await
            .expect("repeated hash repairs the selected local blob root");
        assert_eq!(
            second_store
                .read(&repeated, &scope, None)
                .await
                .expect("repaired PostgreSQL artifact read"),
            vec![0x44; 32]
        );
        store
            .pin(
                &artifact,
                "postgres-parity",
                runtime::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
            )
            .expect("PostgreSQL artifact pin");
        assert!(store.delete(&artifact, &scope).is_err());
        store
            .unpin(&artifact, "postgres-parity")
            .expect("PostgreSQL artifact unpin");
        store
            .delete(&artifact, &scope)
            .expect("PostgreSQL artifact record delete");
        second_store
            .delete(&repeated, &scope)
            .expect("second PostgreSQL artifact record delete");
    }
}
