//! PostgreSQL owners for Cowd Memory and Knowledge.
//!
//! Both adapters receive the host-owned bounded [`storage::PostgresExecutor`].
//! They intentionally do not accept a path or a database URL and never fall
//! back to another durable backend.

use harness_contract::knowledge::{KnowledgeConflictRecord, KnowledgePack, KnowledgeUsageSignal};
use memory::{
    code_indexer::{CodeSymbol, SymbolEdge, SymbolEdgeType, SymbolKind},
    entity::{Entity, Triple},
    knowledge::{
        KnowledgeIngestionReceipt, KnowledgeSnapshot, KnowledgeStore, KnowledgeStoreError,
    },
    project_scope::MemoryScope,
    same_memory_key,
    store::{
        AuthorityLookup, FtsSearchOptions, FtsSearchResult, MemoryCleanupPending,
        MemoryDiscoveryPage, MemoryDiscoveryQuery, MemoryKeyValue, MemoryLayerAggregate,
        MemoryLifecycleMutation, MemoryLifecycleReceipt, MemoryScanCursor, MemoryScanPage,
        MemorySourceFence, MemorySourceFenceReader, MemoryStore, MemoryStoreAggregate,
        MemoryStoreCapabilities, Result as MemoryResult, SymbolMemoryReference, TaggedLookup,
        VerbatimEntry,
    },
    MaintenanceCandidate, MaintenanceCandidateFilter, MaintenanceCandidateStatus,
    MaintenanceQueueBackend, MemoryCategory, MemoryEntry, MemoryError, MemoryId, MemoryLayer,
    MemoryMeta,
};
use postgres::{types::ToSql, Row};
use serde::{de::DeserializeOwned, Serialize};
use storage::{
    PostgresClient, PostgresConnectionConfig, PostgresExecutor, PostgresMigrationMode,
    PostgresMigrationSpec, PostgresTransaction, SecretRefResolver,
};

const MEMORY_DOMAIN: &str = "memory";
const KNOWLEDGE_DOMAIN: &str = "knowledge";
const MEMORY_MIGRATIONS: &[PostgresMigrationSpec] = &[PostgresMigrationSpec {
    id: "memory.0001.durable-owner",
    domain: MEMORY_DOMAIN,
    version: 1,
    description: "create normalized durable memory, graph, code-index and auxiliary owners",
    statements: &[
        "CREATE TABLE IF NOT EXISTS memory_entries (
            id TEXT PRIMARY KEY,
            layer TEXT NOT NULL,
            category TEXT NOT NULL,
            priority TEXT NOT NULL,
            source TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            scope_key TEXT NOT NULL,
            session_id TEXT,
            source_agent TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            payload JSONB NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_memory_entries_scope_updated ON memory_entries(scope_key, updated_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_memory_entries_layer_created ON memory_entries(layer, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_memory_entries_category_created ON memory_entries(category, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_memory_entries_session ON memory_entries(session_id)",
        "CREATE INDEX IF NOT EXISTS idx_memory_entries_fts ON memory_entries USING GIN(to_tsvector('simple', title || ' ' || content || ' ' || coalesce(payload ->> 'tags', '')))",
        "CREATE TABLE IF NOT EXISTS memory_scope_migration_reports (
            memory_id TEXT PRIMARY KEY,
            raw_scope TEXT,
            held_scope TEXT NOT NULL,
            reason TEXT NOT NULL,
            migrated_at TEXT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS memory_entities (id TEXT PRIMARY KEY, payload JSONB NOT NULL)",
        "CREATE TABLE IF NOT EXISTS memory_triples (id TEXT PRIMARY KEY, subject_key TEXT NOT NULL, object_key TEXT NOT NULL, payload JSONB NOT NULL)",
        "CREATE INDEX IF NOT EXISTS idx_memory_triples_subject ON memory_triples(subject_key)",
        "CREATE INDEX IF NOT EXISTS idx_memory_triples_object ON memory_triples(object_key)",
        "CREATE TABLE IF NOT EXISTS memory_kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS memory_verbatim (
            id TEXT PRIMARY KEY, content TEXT NOT NULL, source TEXT NOT NULL, layer INTEGER NOT NULL, timestamp TEXT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_memory_verbatim_timestamp ON memory_verbatim(timestamp DESC)",
        "CREATE TABLE IF NOT EXISTS memory_code_symbols (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL, file_path TEXT NOT NULL,
            line BIGINT NOT NULL, signature TEXT NOT NULL, doc TEXT, project_scope TEXT
        )",
        "CREATE INDEX IF NOT EXISTS idx_memory_code_symbols_file ON memory_code_symbols(file_path)",
        "CREATE INDEX IF NOT EXISTS idx_memory_code_symbols_fts ON memory_code_symbols USING GIN(to_tsvector('simple', name || ' ' || signature || ' ' || coalesce(doc, '')))",
        "CREATE TABLE IF NOT EXISTS memory_code_edges (
            edge_id BIGSERIAL PRIMARY KEY, source_id TEXT NOT NULL, target_id TEXT NOT NULL,
            edge_type TEXT NOT NULL, file_path TEXT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_memory_code_edges_target ON memory_code_edges(target_id, edge_type)",
        "CREATE INDEX IF NOT EXISTS idx_memory_code_edges_source ON memory_code_edges(source_id, edge_type)",
        "CREATE TABLE IF NOT EXISTS memory_symbol_references (
            reference_id BIGSERIAL PRIMARY KEY, symbol_id TEXT NOT NULL, memory_id TEXT NOT NULL,
            turn_index INTEGER, reference_type TEXT, timestamp BIGINT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_memory_symbol_refs_symbol ON memory_symbol_references(symbol_id, timestamp DESC)",
        "CREATE INDEX IF NOT EXISTS idx_memory_symbol_refs_memory ON memory_symbol_references(memory_id)",
    ],
}, PostgresMigrationSpec {
    id: "memory.0002.authority-and-maintenance-indexes",
    domain: MEMORY_DOMAIN,
    version: 2,
    description: "index authority, scoped checkpoint review and keyset maintenance",
    statements: &[
        "ALTER TABLE memory_entries ADD COLUMN IF NOT EXISTS authority_fingerprint TEXT",
        "CREATE INDEX IF NOT EXISTS idx_memory_entries_authority_scope
            ON memory_entries(scope_key, authority_fingerprint, updated_at DESC, id ASC)",
        "CREATE INDEX IF NOT EXISTS idx_memory_entries_scope_category_updated
            ON memory_entries(scope_key, category, updated_at DESC, id ASC)",
        "CREATE INDEX IF NOT EXISTS idx_memory_entries_updated_id
            ON memory_entries(updated_at DESC, id ASC)",
    ],
}, PostgresMigrationSpec {
    id: "memory.0003.tagged-governance-indexes",
    domain: MEMORY_DOMAIN,
    version: 3,
    description: "index subsystem governance tags without scanning all memories",
    statements: &[
        "CREATE INDEX IF NOT EXISTS idx_memory_entries_scope_source_updated
            ON memory_entries(scope_key, source_agent, updated_at DESC, id ASC)",
        "CREATE INDEX IF NOT EXISTS idx_memory_entries_tags
            ON memory_entries USING GIN ((payload -> 'tags'))",
    ],
}, PostgresMigrationSpec {
    id: "memory.0004.maintenance-queue",
    domain: MEMORY_DOMAIN,
    version: 4,
    description: "persist the governed memory maintenance review queue",
    statements: &[
        "CREATE TABLE IF NOT EXISTS memory_maintenance_candidates (
            id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            status TEXT NOT NULL,
            source TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            record_json JSONB NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_memory_maintenance_status_updated
            ON memory_maintenance_candidates(status, updated_at DESC, id)",
        "CREATE INDEX IF NOT EXISTS idx_memory_maintenance_kind_updated
            ON memory_maintenance_candidates(kind, updated_at DESC, id)",
        "CREATE INDEX IF NOT EXISTS idx_memory_maintenance_source_updated
            ON memory_maintenance_candidates(source, updated_at DESC, id)",
    ],
}, PostgresMigrationSpec {
    id: "memory.0005.discovery-revisions",
    domain: MEMORY_DOMAIN,
    version: 5,
    description: "atomically version scoped memory content and lifecycle discovery",
    statements: &[
        "CREATE TABLE IF NOT EXISTS memory_discovery_revisions (scope_key TEXT PRIMARY KEY, revision BIGINT NOT NULL)",
        "CREATE OR REPLACE FUNCTION memory_discovery_entry_changed() RETURNS trigger LANGUAGE plpgsql AS $$
         DECLARE old_scope TEXT; new_scope TEXT; affected_scope TEXT;
         BEGIN
           IF TG_OP='UPDATE' AND (OLD.payload - 'access_count' - 'last_accessed_at') = (NEW.payload - 'access_count' - 'last_accessed_at') AND OLD.scope_key=NEW.scope_key THEN RETURN NEW; END IF;
           IF TG_OP <> 'INSERT' THEN old_scope := OLD.scope_key; END IF;
           IF TG_OP <> 'DELETE' THEN new_scope := NEW.scope_key; END IF;
           FOR affected_scope IN SELECT DISTINCT scope FROM unnest(ARRAY[old_scope,new_scope]) AS scope WHERE scope IS NOT NULL ORDER BY scope LOOP
             INSERT INTO memory_discovery_revisions(scope_key,revision) VALUES(affected_scope,1)
             ON CONFLICT(scope_key) DO UPDATE SET revision=memory_discovery_revisions.revision+1;
           END LOOP;
           RETURN NULL;
         END $$",
        "CREATE TRIGGER memory_discovery_entry_revision AFTER INSERT OR UPDATE OR DELETE ON memory_entries FOR EACH ROW EXECUTE FUNCTION memory_discovery_entry_changed()",
        "CREATE OR REPLACE FUNCTION memory_discovery_lifecycle_changed() RETURNS trigger LANGUAGE plpgsql AS $$
         DECLARE old_key TEXT; new_key TEXT; affected_scope TEXT;
         BEGIN
           IF TG_OP='UPDATE' AND OLD.key=NEW.key AND OLD.value=NEW.value THEN RETURN NEW; END IF;
           IF TG_OP <> 'INSERT' THEN old_key := OLD.key; END IF;
           IF TG_OP <> 'DELETE' THEN new_key := NEW.key; END IF;
           FOR affected_scope IN SELECT DISTINCT scope_key FROM memory_entries WHERE id IN (
             SELECT substr(key, length('memory_lifecycle:')+1) FROM unnest(ARRAY[old_key,new_key]) AS key WHERE starts_with(key,'memory_lifecycle:')
           ) ORDER BY scope_key LOOP
             INSERT INTO memory_discovery_revisions(scope_key,revision) VALUES(affected_scope,1)
             ON CONFLICT(scope_key) DO UPDATE SET revision=memory_discovery_revisions.revision+1;
           END LOOP;
           RETURN NULL;
         END $$",
        "CREATE TRIGGER memory_discovery_lifecycle_revision AFTER INSERT OR UPDATE OR DELETE ON memory_kv FOR EACH ROW EXECUTE FUNCTION memory_discovery_lifecycle_changed()",
        "CREATE INDEX IF NOT EXISTS idx_memory_discovery_scope_id ON memory_entries(scope_key,id)",
    ],
}, PostgresMigrationSpec {
    id: "memory.0006.knowledge-dedup",
    domain: MEMORY_DOMAIN,
    version: 6,
    description: "deduplicate code edges / symbol references and enforce logical uniqueness",
    statements: &[
        // Remove historical duplicates, keeping the lowest surrogate key.
        "DELETE FROM memory_code_edges a USING memory_code_edges b
         WHERE a.edge_id > b.edge_id
           AND a.source_id = b.source_id AND a.target_id = b.target_id
           AND a.edge_type = b.edge_type AND a.file_path = b.file_path",
        "DELETE FROM memory_symbol_references a USING memory_symbol_references b
         WHERE a.reference_id > b.reference_id
           AND a.symbol_id = b.symbol_id AND a.memory_id = b.memory_id
           AND COALESCE(a.turn_index, -1) = COALESCE(b.turn_index, -1)
           AND COALESCE(a.reference_type, '') = COALESCE(b.reference_type, '')",
        // Logical uniqueness. The symbol-reference key is expression-based so a
        // NULL turn_index/reference_type still collides: a plain unique index
        // treats NULLs as distinct, which is exactly how `ON CONFLICT` silently
        // no-ops and rows accumulate (PLAN M-02 / W5 Step1).
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_memory_code_edges_logical
            ON memory_code_edges(source_id, target_id, edge_type, file_path)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_memory_symbol_refs_logical
            ON memory_symbol_references(symbol_id, memory_id, COALESCE(turn_index, -1), COALESCE(reference_type, ''))",
    ],
}, PostgresMigrationSpec {
    id: "memory.0007.knowledge-graph-scope",
    domain: MEMORY_DOMAIN,
    version: 7,
    description: "scope the knowledge graph so a snapshot replace cannot cross scopes",
    statements: &[
        // Existing rows predate scoping; the only graph they can belong to is
        // the global one, which is exactly MemoryScope::Global.scope_key().
        "ALTER TABLE memory_entities ADD COLUMN IF NOT EXISTS scope_key TEXT NOT NULL DEFAULT 'global'",
        "ALTER TABLE memory_triples ADD COLUMN IF NOT EXISTS scope_key TEXT NOT NULL DEFAULT 'global'",
        "CREATE INDEX IF NOT EXISTS idx_memory_entities_scope ON memory_entities(scope_key)",
        "CREATE INDEX IF NOT EXISTS idx_memory_triples_scope ON memory_triples(scope_key)",
        // Identity is now per-scope, so a snapshot replace only ever deletes
        // rows of its own scope.
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_memory_entities_scope_id ON memory_entities(scope_key, id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_memory_triples_scope_id ON memory_triples(scope_key, id)",
    ],
}, PostgresMigrationSpec {
    id: "memory.0008.source-fence-cleanup",
    domain: MEMORY_DOMAIN,
    version:8,
    description:"original lifecycle source fences, idempotent receipts and durable cleanup obligations",
    statements:&[
        "CREATE TABLE memory_source_fences(source_ref TEXT PRIMARY KEY,scope_key TEXT NOT NULL,delete_epoch BIGINT NOT NULL CHECK(delete_epoch>=0),payload JSONB NOT NULL)",
        "CREATE TABLE memory_lifecycle_receipts(mutation_id TEXT PRIMARY KEY,payload JSONB NOT NULL)",
        "CREATE TABLE memory_cleanup_pending(mutation_id TEXT PRIMARY KEY,source_ref TEXT NOT NULL,delete_epoch BIGINT NOT NULL,confirmed BOOLEAN NOT NULL DEFAULT FALSE,payload JSONB NOT NULL)",
        "CREATE INDEX memory_cleanup_pending_page ON memory_cleanup_pending(mutation_id) WHERE NOT confirmed",
    ],
}, PostgresMigrationSpec {
    id:"memory.0009.scoped-graph-primary-identities",domain:MEMORY_DOMAIN,version:9,
    description:"remove inherited global graph identity constraints in favor of the original scoped keys",
    statements:&[
        "ALTER TABLE memory_entities DROP CONSTRAINT memory_entities_pkey",
        "ALTER TABLE memory_entities ADD CONSTRAINT memory_entities_pkey PRIMARY KEY USING INDEX uq_memory_entities_scope_id",
        "ALTER TABLE memory_triples DROP CONSTRAINT memory_triples_pkey",
        "ALTER TABLE memory_triples ADD CONSTRAINT memory_triples_pkey PRIMARY KEY USING INDEX uq_memory_triples_scope_id",
    ],
}, PostgresMigrationSpec {
    id:"memory.0010.typed-knowledge-cleanup-ack",domain:MEMORY_DOMAIN,version:10,
    description:"replay older boolean cleanup confirmations through original typed Knowledge receipts",
    statements:&[
        "UPDATE memory_cleanup_pending SET confirmed=FALSE,payload=payload-'knowledge_confirmed' WHERE NOT(payload ? 'knowledge_receipt_digest')",
    ],
}];

const KNOWLEDGE_MIGRATIONS: &[PostgresMigrationSpec] = &[PostgresMigrationSpec {
    id: "knowledge.0001.durable-owner",
    domain: KNOWLEDGE_DOMAIN,
    version: 1,
    description: "create normalized knowledge corpus, pack, canon, conflict, chunk and usage owners",
    statements: &[
        "CREATE TABLE IF NOT EXISTS knowledge_corpus (
            corpus_id TEXT PRIMARY KEY, namespace_key TEXT NOT NULL, updated_at TEXT NOT NULL, payload JSONB NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_knowledge_corpus_namespace ON knowledge_corpus(namespace_key, updated_at DESC)",
        "CREATE TABLE IF NOT EXISTS knowledge_pack (
            pack_id TEXT PRIMARY KEY, namespace_key TEXT NOT NULL, state TEXT NOT NULL, updated_at TEXT NOT NULL, payload JSONB NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_knowledge_pack_namespace_state ON knowledge_pack(namespace_key, state, updated_at DESC)",
        "CREATE TABLE IF NOT EXISTS knowledge_canon (
            canon_id TEXT PRIMARY KEY, pack_id TEXT NOT NULL, updated_at TEXT NOT NULL, payload JSONB NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_knowledge_canon_pack ON knowledge_canon(pack_id)",
        "CREATE TABLE IF NOT EXISTS knowledge_conflict (
            conflict_id TEXT PRIMARY KEY, pack_id TEXT, state TEXT NOT NULL, detected_at TEXT NOT NULL, payload JSONB NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_knowledge_conflict_pack_state ON knowledge_conflict(pack_id, state, detected_at DESC)",
        "CREATE TABLE IF NOT EXISTS knowledge_chunk (
            chunk_id TEXT PRIMARY KEY, corpus_id TEXT NOT NULL, ordinal BIGINT NOT NULL, title TEXT NOT NULL, text TEXT NOT NULL, payload JSONB NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_knowledge_chunk_corpus_ordinal ON knowledge_chunk(corpus_id, ordinal)",
        "CREATE INDEX IF NOT EXISTS idx_knowledge_chunk_fts ON knowledge_chunk USING GIN(to_tsvector('simple', title || ' ' || text))",
        "CREATE TABLE IF NOT EXISTS knowledge_usage (
            signal_id TEXT PRIMARY KEY, pack_id TEXT NOT NULL, session_id TEXT NOT NULL, occurred_at TEXT NOT NULL, payload JSONB NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_knowledge_usage_pack_time ON knowledge_usage(pack_id, occurred_at DESC)",
    ],
}, PostgresMigrationSpec {
    id: "knowledge.0002.ingestion-receipts",
    domain: KNOWLEDGE_DOMAIN,
    version: 2,
    description: "commit immutable ingestion receipts with original knowledge writes",
    statements: &[
        "CREATE TABLE IF NOT EXISTS knowledge_ingestion_receipt (request_key TEXT PRIMARY KEY, input_digest TEXT NOT NULL, payload JSONB NOT NULL)",
    ],
}, PostgresMigrationSpec {
    id: "knowledge.0003.scoped-candidate-revisions",
    domain: KNOWLEDGE_DOMAIN,
    version: 3,
    description: "bind bounded Knowledge candidate pages to original scoped source revisions",
    statements: &[
        "CREATE TABLE IF NOT EXISTS knowledge_candidate_revision (scope_key TEXT PRIMARY KEY, revision BIGINT NOT NULL CHECK(revision>=0))",
        "CREATE TABLE knowledge_candidate_owner (singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton), store_id TEXT NOT NULL)",
        "INSERT INTO knowledge_candidate_owner(singleton,store_id) VALUES(TRUE,gen_random_uuid()::text)",
        "CREATE FUNCTION knowledge_bump_candidate_namespace(ns TEXT) RETURNS void LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO knowledge_candidate_revision(scope_key,revision)
           SELECT key,1 FROM unnest(ARRAY['ns:'||ns, CASE WHEN starts_with(ns,'project:') OR starts_with(ns,'project_group:') THEN 'act:'||ns ELSE 'act:shared' END]) AS key ORDER BY key
           ON CONFLICT(scope_key) DO UPDATE SET revision=knowledge_candidate_revision.revision+1;
         END $$",
        "CREATE FUNCTION knowledge_pack_candidates_changed() RETURNS trigger LANGUAGE plpgsql AS $$
         DECLARE old_ns TEXT; new_ns TEXT; ns TEXT;
         BEGIN
           IF TG_OP='UPDATE' AND OLD.payload=NEW.payload AND OLD.namespace_key=NEW.namespace_key THEN RETURN NULL; END IF;
           IF TG_OP <> 'INSERT' THEN old_ns := OLD.namespace_key; END IF;
           IF TG_OP <> 'DELETE' THEN new_ns := NEW.namespace_key; END IF;
           FOR ns IN SELECT DISTINCT value FROM unnest(ARRAY[old_ns,new_ns]) AS value WHERE value IS NOT NULL ORDER BY value LOOP
             PERFORM knowledge_bump_candidate_namespace(ns);
           END LOOP;
           RETURN NULL;
         END $$",
        "CREATE TRIGGER knowledge_pack_candidate_revision AFTER INSERT OR UPDATE OR DELETE ON knowledge_pack FOR EACH ROW EXECUTE FUNCTION knowledge_pack_candidates_changed()",
        "CREATE FUNCTION knowledge_canon_candidates_changed() RETURNS trigger LANGUAGE plpgsql AS $$
         DECLARE old_id TEXT; new_id TEXT; ns TEXT;
         BEGIN
           IF TG_OP='UPDATE' AND OLD.payload=NEW.payload AND OLD.canon_id=NEW.canon_id THEN RETURN NULL; END IF;
           IF TG_OP <> 'INSERT' THEN old_id := OLD.canon_id; END IF;
           IF TG_OP <> 'DELETE' THEN new_id := NEW.canon_id; END IF;
           FOR ns IN SELECT DISTINCT namespace_key FROM knowledge_pack WHERE payload->>'canon_pack_ref'=ANY(ARRAY[old_id,new_id]) ORDER BY namespace_key LOOP
             PERFORM knowledge_bump_candidate_namespace(ns);
           END LOOP;
           RETURN NULL;
         END $$",
        "CREATE TRIGGER knowledge_canon_candidate_revision AFTER INSERT OR UPDATE OR DELETE ON knowledge_canon FOR EACH ROW EXECUTE FUNCTION knowledge_canon_candidates_changed()",
    ],
}, PostgresMigrationSpec {
    id:"knowledge.0004.candidate-source-revisions",domain:KNOWLEDGE_DOMAIN,version:4,
    description:"fence candidate continuation on original source identity changes",
    statements:&[
        "CREATE FUNCTION knowledge_corpus_candidates_changed() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE ns TEXT; BEGIN IF TG_OP='UPDATE' AND OLD.payload->'source_ref'=NEW.payload->'source_ref' AND OLD.namespace_key=NEW.namespace_key THEN RETURN NULL; END IF; FOR ns IN SELECT DISTINCT namespace_key FROM knowledge_corpus WHERE corpus_id=CASE WHEN TG_OP='DELETE' THEN OLD.corpus_id ELSE NEW.corpus_id END LOOP PERFORM knowledge_bump_candidate_namespace(ns); END LOOP; IF TG_OP='DELETE' THEN PERFORM knowledge_bump_candidate_namespace(OLD.namespace_key); ELSIF TG_OP='UPDATE' AND OLD.namespace_key<>NEW.namespace_key THEN PERFORM knowledge_bump_candidate_namespace(OLD.namespace_key); END IF; RETURN NULL; END $$",
        "CREATE TRIGGER knowledge_corpus_candidate_revision AFTER INSERT OR UPDATE OR DELETE ON knowledge_corpus FOR EACH ROW EXECUTE FUNCTION knowledge_corpus_candidates_changed()",
    ],
}, PostgresMigrationSpec {
    id:"knowledge.0005.memory-retirement-receipts",domain:KNOWLEDGE_DOMAIN,version:5,
    description:"commit exact original Memory source and epoch quarantine receipts",
    statements:&[
        "CREATE TABLE knowledge_memory_retirement_receipts(source_ref TEXT NOT NULL,scope_key TEXT NOT NULL,delete_epoch BIGINT NOT NULL CHECK(delete_epoch>0),receipt_digest TEXT NOT NULL,payload JSONB NOT NULL,PRIMARY KEY(source_ref,scope_key,delete_epoch))",
        "CREATE INDEX knowledge_memory_retirement_source ON knowledge_memory_retirement_receipts(source_ref)",
    ],
}];

#[derive(Clone, Debug)]
pub struct PostgresMemoryStore {
    executor: PostgresExecutor,
}

#[derive(Clone, Debug)]
pub struct PostgresMaintenanceQueue {
    executor: PostgresExecutor,
}

impl PostgresMaintenanceQueue {
    pub fn new(executor: PostgresExecutor) -> MemoryResult<Self> {
        executor
            .apply_migrations(MEMORY_DOMAIN, MEMORY_MIGRATIONS)
            .map_err(storage_memory_error)?;
        Ok(Self { executor })
    }

    fn candidate(&self, id: &str) -> MemoryResult<Option<MaintenanceCandidate>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_memory_error)?;
        connection
            .query_opt(
                "SELECT record_json,status,created_at,updated_at
                 FROM memory_maintenance_candidates WHERE id=$1",
                &[&id],
            )
            .map_err(postgres_memory_error)?
            .map(|row| row_to_maintenance_candidate(&row))
            .transpose()
    }
}

impl MaintenanceQueueBackend for PostgresMaintenanceQueue {
    fn upsert_many(&self, candidates: &[MaintenanceCandidate]) -> MemoryResult<usize> {
        if candidates.is_empty() {
            return Ok(0);
        }
        let candidates = candidates
            .iter()
            .map(|candidate| (candidate.id.as_str(), candidate))
            .collect::<std::collections::BTreeMap<_, _>>();
        let candidate_ids = candidates
            .keys()
            .map(|candidate_id| (*candidate_id).to_string())
            .collect::<Vec<_>>();
        let records = serde_json::to_value(candidates.values().copied().collect::<Vec<_>>())
            .map_err(json_memory_error)?;
        let mut connection = self
            .executor
            .checkout_background()
            .map_err(storage_memory_error)?;
        let mut transaction = connection.transaction().map_err(postgres_memory_error)?;
        let existing = transaction
            .query_one(
                "SELECT COUNT(*)::BIGINT
                 FROM memory_maintenance_candidates WHERE id=ANY($1)",
                &[&candidate_ids],
            )
            .map_err(postgres_memory_error)?
            .try_get::<_, i64>(0)
            .map_err(postgres_memory_error)?;
        let existing = usize::try_from(existing).map_err(|_| {
            MemoryError::Store(
                "maintenance candidate existing-count query returned a negative value".to_string(),
            )
        })?;
        transaction
            .execute(
                "INSERT INTO memory_maintenance_candidates
                 (id,kind,status,source,created_at,updated_at,record_json)
                 SELECT record->>'id',
                        record->>'kind',
                        record->>'status',
                        record->>'source',
                        record->>'created_at',
                        record->>'updated_at',
                        record
                 FROM jsonb_array_elements($1::JSONB) AS record
                 ON CONFLICT(id) DO UPDATE SET
                   kind=EXCLUDED.kind,
                   source=EXCLUDED.source,
                   updated_at=EXCLUDED.updated_at,
                   record_json=EXCLUDED.record_json",
                &[&records],
            )
            .map_err(postgres_memory_error)?;
        transaction.commit().map_err(postgres_memory_error)?;
        Ok(candidates.len().saturating_sub(existing))
    }

    fn list(&self, filter: MaintenanceCandidateFilter) -> MemoryResult<Vec<MaintenanceCandidate>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_memory_error)?;
        let status = filter.status.map(|value| value.as_str().to_string());
        let kind = filter.kind.map(|value| value.as_str().to_string());
        let source = filter.source;
        let limit = limit_i64(filter.limit.unwrap_or(128).min(500))?;
        connection
            .query(
                "SELECT record_json,status,created_at,updated_at
                 FROM memory_maintenance_candidates
                 WHERE ($1::TEXT IS NULL OR status=$1)
                   AND ($2::TEXT IS NULL OR kind=$2)
                   AND ($3::TEXT IS NULL OR source=$3)
                 ORDER BY created_at DESC,id ASC LIMIT $4",
                &[&status, &kind, &source, &limit],
            )
            .map_err(postgres_memory_error)?
            .iter()
            .map(row_to_maintenance_candidate)
            .collect()
    }

    fn transition(
        &self,
        id: &str,
        status: MaintenanceCandidateStatus,
    ) -> MemoryResult<Option<MaintenanceCandidate>> {
        let mut connection = self
            .executor
            .checkout_background()
            .map_err(storage_memory_error)?;
        let updated_at = chrono::Utc::now().to_rfc3339();
        connection
            .execute(
                "UPDATE memory_maintenance_candidates
                 SET status=$1,updated_at=$2 WHERE id=$3",
                &[&status.as_str(), &updated_at, &id],
            )
            .map_err(postgres_memory_error)?;
        drop(connection);
        self.candidate(id)
    }
}

impl PostgresMemoryStore {
    pub fn new(executor: PostgresExecutor) -> MemoryResult<Self> {
        run_driver_sync(
            move || {
                executor
                    .apply_migrations(MEMORY_DOMAIN, MEMORY_MIGRATIONS)
                    .map_err(storage_memory_error)?;
                let store = Self { executor };
                if store.executor.migration_mode() == PostgresMigrationMode::Maintenance {
                    store.backfill_authority_fingerprints()?;
                }
                Ok(store)
            },
            || MemoryError::Store("PostgreSQL memory initialization thread panicked".to_string()),
        )
    }

    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> MemoryResult<Self> {
        run_driver_sync(
            move || {
                let executor =
                    PostgresExecutor::connect(config, resolver).map_err(storage_memory_error)?;
                executor
                    .apply_migrations(MEMORY_DOMAIN, MEMORY_MIGRATIONS)
                    .map_err(storage_memory_error)?;
                let store = Self { executor };
                if store.executor.migration_mode() == PostgresMigrationMode::Maintenance {
                    store.backfill_authority_fingerprints()?;
                }
                Ok(store)
            },
            || MemoryError::Store("PostgreSQL memory connection thread panicked".to_string()),
        )
    }

    #[must_use]
    pub fn executor(&self) -> &PostgresExecutor {
        &self.executor
    }

    fn entries(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> MemoryResult<Vec<MemoryEntry>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_memory_error)?;
        connection
            .query(sql, params)
            .map_err(postgres_memory_error)?
            .iter()
            .map(row_to_entry)
            .collect()
    }

    fn backfill_authority_fingerprints(&self) -> MemoryResult<()> {
        let mut connection = self
            .executor
            .checkout_background()
            .map_err(storage_memory_error)?;
        let rows = connection
            .query(
                "SELECT id,payload FROM memory_entries
                  WHERE authority_fingerprint IS NULL
                  ORDER BY updated_at DESC,id ASC",
                &[],
            )
            .map_err(postgres_memory_error)?;
        let entries = rows
            .iter()
            .map(|row| {
                let id = row.try_get::<_, String>(0).map_err(postgres_memory_error)?;
                let entry = entry_from_json(row.try_get(1).map_err(postgres_memory_error)?)?;
                Ok((id, same_memory_key(&entry)))
            })
            .collect::<MemoryResult<Vec<_>>>()?;
        for (id, fingerprint) in entries {
            connection
                .execute(
                    "UPDATE memory_entries SET authority_fingerprint=$2 WHERE id=$1",
                    &[&id, &fingerprint],
                )
                .map_err(postgres_memory_error)?;
        }
        Ok(())
    }

    fn symbols(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> MemoryResult<Vec<CodeSymbol>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_memory_error)?;
        connection
            .query(sql, params)
            .map_err(postgres_memory_error)?
            .iter()
            .map(row_to_symbol)
            .collect()
    }

    fn upsert_entry(&self, entry: &MemoryEntry) -> MemoryResult<()> {
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(storage_memory_error)?;
        let mut tx = connection.transaction().map_err(postgres_memory_error)?;
        write_entry(&mut tx, entry, true)?;
        tx.commit().map_err(postgres_memory_error)
    }

    fn update_entry(&self, entry: &MemoryEntry) -> MemoryResult<()> {
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(storage_memory_error)?;
        let mut tx = connection.transaction().map_err(postgres_memory_error)?;
        write_entry(&mut tx, entry, false)?;
        tx.commit().map_err(postgres_memory_error)
    }

    fn search_memory(
        &self,
        query: &str,
        scope: Option<String>,
        category: Option<MemoryCategory>,
        layer: Option<MemoryLayer>,
        limit: usize,
    ) -> MemoryResult<Vec<MemoryEntry>> {
        if query.trim().is_empty() {
            return Ok(Vec::new());
        }
        let category = category.map(|value| enum_label(&value)).transpose()?;
        let layer = layer.map(|value| enum_label(&value)).transpose()?;
        let limit = limit_i64(limit)?;
        self.entries(
            "SELECT payload FROM memory_entries
             WHERE (to_tsvector('simple', title || ' ' || content || ' ' || coalesce(payload ->> 'tags', ''))
                       @@ websearch_to_tsquery('simple', $1)
                    OR title ILIKE '%' || $1 || '%'
                    OR content ILIKE '%' || $1 || '%')
               AND ($2::TEXT IS NULL OR scope_key='global' OR scope_key=$2)
               AND ($3::TEXT IS NULL OR category=$3)
               AND ($4::TEXT IS NULL OR layer=$4)
             ORDER BY updated_at DESC, id ASC LIMIT $5",
            &[&query, &scope, &category, &layer, &limit],
        )
    }

    fn count_memory(
        &self,
        query: &str,
        category: Option<MemoryCategory>,
        layer: Option<MemoryLayer>,
    ) -> MemoryResult<usize> {
        if query.trim().is_empty() {
            return Ok(0);
        }
        let category = category.map(|value| enum_label(&value)).transpose()?;
        let layer = layer.map(|value| enum_label(&value)).transpose()?;
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_memory_error)?;
        let count: i64 = connection
            .query_one(
                "SELECT count(*) FROM memory_entries
                 WHERE (to_tsvector('simple', title || ' ' || content || ' ' || coalesce(payload ->> 'tags', ''))
                           @@ websearch_to_tsquery('simple', $1)
                        OR title ILIKE '%' || $1 || '%'
                        OR content ILIKE '%' || $1 || '%')
                   AND ($2::TEXT IS NULL OR category=$2)
                   AND ($3::TEXT IS NULL OR layer=$3)",
                &[&query, &category, &layer],
            )
            .map_err(postgres_memory_error)?
            .try_get(0)
            .map_err(postgres_memory_error)?;
        usize::try_from(count)
            .map_err(|_| MemoryError::Store("memory search count overflow".to_string()))
    }

    fn replace_json_rows<T: Serialize>(
        &self,
        scope_key: &str,
        table: &str,
        values: &[T],
        id: impl Fn(&serde_json::Value) -> Option<String>,
    ) -> MemoryResult<()> {
        let (delete_sql, insert_sql) = match table {
            "memory_entities" => (
                "DELETE FROM memory_entities WHERE scope_key=$1",
                "INSERT INTO memory_entities(scope_key,id,payload) VALUES($1,$2,$3)",
            ),
            _ => {
                return Err(MemoryError::Store(format!(
                    "unsupported memory JSON table `{table}`"
                )))
            }
        };
        let scope_key = scope_key.to_string();
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(storage_memory_error)?;
        let mut transaction = connection.transaction().map_err(postgres_memory_error)?;
        transaction
            .execute(
                "SELECT pg_advisory_xact_lock(hashtext('cowd-memory:knowledge-graph'))",
                &[],
            )
            .map_err(postgres_memory_error)?;
        transaction
            .execute(delete_sql, &[&scope_key])
            .map_err(postgres_memory_error)?;
        for value in values {
            let payload = serde_json::to_value(value).map_err(json_memory_error)?;
            let id = id(&payload)
                .ok_or_else(|| MemoryError::Store("durable JSON row has no id".to_string()))?;
            transaction
                .execute(insert_sql, &[&scope_key, &id, &payload])
                .map_err(postgres_memory_error)?;
        }
        transaction.commit().map_err(postgres_memory_error)?;
        Ok(())
    }

    fn load_json_rows<T: DeserializeOwned>(&self, scope_key: &str, table: &str) -> MemoryResult<Vec<T>> {
        let sql = match table {
            "memory_entities" => "SELECT payload FROM memory_entities WHERE scope_key=$1 ORDER BY id",
            "memory_triples" => "SELECT payload FROM memory_triples WHERE scope_key=$1 ORDER BY id",
            _ => {
                return Err(MemoryError::Store(format!(
                    "unsupported memory JSON table `{table}`"
                )))
            }
        };
        let scope_key = scope_key.to_string();
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_memory_error)?;
        connection
            .query(sql, &[&scope_key])
            .map_err(postgres_memory_error)?
            .iter()
            .map(|row| {
                let payload: serde_json::Value = row.try_get(0).map_err(postgres_memory_error)?;
                serde_json::from_value(payload).map_err(json_memory_error)
            })
            .collect()
    }

    fn replace_triples(&self, scope_key: &str, triples: &[Triple]) -> MemoryResult<()> {
        let scope_key = scope_key.to_string();
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(storage_memory_error)?;
        let mut transaction = connection.transaction().map_err(postgres_memory_error)?;
        transaction
            .execute(
                "SELECT pg_advisory_xact_lock(hashtext('cowd-memory:knowledge-graph'))",
                &[],
            )
            .map_err(postgres_memory_error)?;
        transaction
            .execute("DELETE FROM memory_triples WHERE scope_key=$1", &[&scope_key])
            .map_err(postgres_memory_error)?;
        for triple in triples {
            let payload = serde_json::to_value(triple).map_err(json_memory_error)?;
            transaction
                .execute(
                    "INSERT INTO memory_triples(scope_key,id,subject_key,object_key,payload) VALUES($1,$2,$3,$4,$5)",
                    &[&scope_key, &triple.id, &triple.subject_id, &triple.object_id, &payload],
                )
                .map_err(postgres_memory_error)?;
        }
        transaction.commit().map_err(postgres_memory_error)?;
        Ok(())
    }

    /// Persist entities and triples as one atomic, serialized snapshot.
    ///
    /// `save_entities` + `save_triples` are two independent transactions; under
    /// a multi-connection pool two turn-end writers could interleave and one
    /// could wipe the other's rows. Take the knowledge-graph advisory lock and
    /// perform both replacements in a single transaction, and refuse an empty
    /// snapshot so a load failure cannot silently clear durable state
    /// (PLAN M-02 / W5 Step2).
    fn replace_knowledge_graph(
        &self,
        scope_key: &str,
        entities: &[Entity],
        triples: &[Triple],
    ) -> MemoryResult<()> {
        if entities.is_empty() && triples.is_empty() {
            return Err(MemoryError::Store(
                "refusing to replace the knowledge graph with an empty snapshot".to_string(),
            ));
        }
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(storage_memory_error)?;
        let mut transaction = connection.transaction().map_err(postgres_memory_error)?;
        transaction
            .execute(
                "SELECT pg_advisory_xact_lock(hashtext('cowd-memory:knowledge-graph'))",
                &[],
            )
            .map_err(postgres_memory_error)?;
        let scope = scope_key.to_string();
        transaction
            .execute("DELETE FROM memory_entities WHERE scope_key=$1", &[&scope])
            .map_err(postgres_memory_error)?;
        for entity in entities {
            let payload = serde_json::to_value(entity).map_err(json_memory_error)?;
            let id = payload
                .get("id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| MemoryError::Store("durable entity has no id".to_string()))?;
            transaction
                .execute(
                    "INSERT INTO memory_entities(scope_key,id,payload) VALUES($1,$2,$3)",
                    &[&scope, &id, &payload],
                )
                .map_err(postgres_memory_error)?;
        }
        transaction
            .execute("DELETE FROM memory_triples WHERE scope_key=$1", &[&scope])
            .map_err(postgres_memory_error)?;
        for triple in triples {
            let payload = serde_json::to_value(triple).map_err(json_memory_error)?;
            transaction
                .execute(
                    "INSERT INTO memory_triples(scope_key,id,subject_key,object_key,payload) VALUES($1,$2,$3,$4,$5)",
                    &[&scope, &triple.id, &triple.subject_id, &triple.object_id, &payload],
                )
                .map_err(postgres_memory_error)?;
        }
        transaction.commit().map_err(postgres_memory_error)?;
        Ok(())
    }

    fn upsert_symbol(&self, symbol: &CodeSymbol) -> MemoryResult<()> {
        let line = i64::try_from(symbol.line)
            .map_err(|_| MemoryError::Store("code symbol line overflow".to_string()))?;
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(storage_memory_error)?;
        connection
            .execute(
                "INSERT INTO memory_code_symbols(id,name,kind,file_path,line,signature,doc,project_scope)
                 VALUES($1,$2,$3,$4,$5,$6,$7,NULL)
                 ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,kind=EXCLUDED.kind,
                    file_path=EXCLUDED.file_path,line=EXCLUDED.line,signature=EXCLUDED.signature,
                    doc=EXCLUDED.doc",
                &[
                    &symbol.id,
                    &symbol.name,
                    &symbol.kind.as_str(),
                    &symbol.file_path,
                    &line,
                    &symbol.signature,
                    &symbol.doc,
                ],
            )
            .map_err(postgres_memory_error)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl MemoryStore for PostgresMemoryStore {
    fn capabilities(&self) -> MemoryStoreCapabilities {
        MemoryStoreCapabilities {
            backend: "postgres",
            full_text_search: true,
            lexical_fallback: true,
            vector_search: false,
            code_index: true,
        }
    }

    async fn insert(&self, entry: &MemoryEntry) -> MemoryResult<MemoryId> {
        let store = self.clone();
        let entry = entry.clone();
        run_memory_blocking(move || {
            store.upsert_entry(&entry)?;
            Ok(entry.id)
        })
        .await
    }

    async fn get(&self, id: &MemoryId) -> MemoryResult<Option<MemoryEntry>> {
        let store = self.clone();
        let id = id.to_string();
        run_memory_blocking(move || {
            let mut connection = store
                .executor
                .checkout_online_read()
                .map_err(storage_memory_error)?;
            let row = connection
                .query_opt("SELECT payload FROM memory_entries WHERE id=$1", &[&id])
                .map_err(postgres_memory_error)?;
            let Some(row) = row else {
                return Ok(None);
            };
            let entry = entry_from_json(row.try_get(0).map_err(postgres_memory_error)?)?;
            // A lookup is not an accepted context-use observation. In
            // particular, status/source checks must not become durable writes.
            Ok(Some(entry))
        })
        .await
    }

    async fn update(&self, entry: &MemoryEntry) -> MemoryResult<()> {
        let store = self.clone();
        let entry = entry.clone();
        run_memory_blocking(move || store.update_entry(&entry)).await
    }
    async fn delete(&self, id: &MemoryId) -> MemoryResult<()> {
        let store = self.clone();
        let id = id.to_string();
        run_memory_blocking(move || {
            let mut connection = store
                .executor
                .checkout_critical()
                .map_err(storage_memory_error)?;
            let mut tx = connection.transaction().map_err(postgres_memory_error)?;
            tx.execute("DELETE FROM memory_entries WHERE id=$1", &[&id])
                .map_err(postgres_memory_error)?;
            tx.execute("DELETE FROM memory_verbatim WHERE id=$1", &[&id])
                .map_err(postgres_memory_error)?;
            tx.commit().map_err(postgres_memory_error)?;
            Ok(())
        })
        .await
    }

    async fn commit_lifecycle(
        &self,
        command: MemoryLifecycleMutation,
    ) -> MemoryResult<MemoryLifecycleReceipt> {
        let store = self.clone();
        run_memory_blocking(move || {
            let mut connection=store.executor.checkout_critical().map_err(storage_memory_error)?;
            let mut tx=connection.transaction().map_err(postgres_memory_error)?;
            // One per-target transaction lock also serializes the absent-history
            // case, where a SELECT FOR UPDATE of a missing KV row is insufficient.
            let target=command.event.memory_id.to_string();
            let row=tx.query_opt("SELECT payload FROM memory_entries WHERE id=$1 FOR UPDATE", &[&target]).map_err(postgres_memory_error)?;
            if let Some(row)=tx.query_opt("SELECT payload FROM memory_lifecycle_receipts WHERE mutation_id=$1", &[&command.mutation_id]).map_err(postgres_memory_error)? {
                let receipt:MemoryLifecycleReceipt=serde_json::from_value(row.try_get(0).map_err(postgres_memory_error)?).map_err(json_memory_error)?;
                command.validate_replay(&receipt)?;
                return Ok(receipt);
            }
            let entry=entry_from_json(row.ok_or_else(||MemoryError::NotFound(target.clone()))?.try_get(0).map_err(postgres_memory_error)?)?;
            let key=format!("memory_lifecycle:{target}");
            let source_ref=format!("memory:{target}");
            let history:Option<String>=tx.query_opt("SELECT value FROM memory_kv WHERE key=$1 FOR UPDATE", &[&key]).map_err(postgres_memory_error)?.map(|r|r.try_get(0)).transpose().map_err(postgres_memory_error)?;
            let source:Option<MemorySourceFence>=tx.query_opt("SELECT payload FROM memory_source_fences WHERE source_ref=$1 FOR UPDATE", &[&source_ref]).map_err(postgres_memory_error)?.map(|r|r.try_get::<_,serde_json::Value>(0)).transpose().map_err(postgres_memory_error)?.map(serde_json::from_value).transpose().map_err(json_memory_error)?;
            let (raw,receipt,pending)=command.validate_cut(&entry,history.as_deref(),source.as_ref())?;
            let payload=serde_json::to_value(&receipt.source).map_err(json_memory_error)?;
            let epoch=i64::try_from(receipt.source.delete_epoch).map_err(|_|MemoryError::Store("Memory delete epoch overflow".into()))?;
            tx.execute("INSERT INTO memory_kv(key,value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value", &[&key,&raw]).map_err(postgres_memory_error)?;
            tx.execute("INSERT INTO memory_source_fences(source_ref,scope_key,delete_epoch,payload) VALUES($1,$2,$3,$4) ON CONFLICT(source_ref) DO UPDATE SET delete_epoch=EXCLUDED.delete_epoch,payload=EXCLUDED.payload", &[&source_ref,&receipt.source.scope_key,&epoch,&payload]).map_err(postgres_memory_error)?;
            if let Some(pending)=pending {
                let payload=serde_json::to_value(&pending).map_err(json_memory_error)?;
                tx.execute("INSERT INTO memory_cleanup_pending(mutation_id,source_ref,delete_epoch,payload) VALUES($1,$2,$3,$4)", &[&command.mutation_id,&source_ref,&epoch,&payload]).map_err(postgres_memory_error)?;
            }
            let payload=serde_json::to_value(&receipt).map_err(json_memory_error)?;
            tx.execute("INSERT INTO memory_lifecycle_receipts(mutation_id,payload) VALUES($1,$2)", &[&command.mutation_id,&payload]).map_err(postgres_memory_error)?;
            tx.commit().map_err(postgres_memory_error)?;
            Ok(receipt)
        }).await
    }

    async fn pending_memory_cleanup(
        &self,
        after_mutation: Option<&str>,
        limit: usize,
    ) -> MemoryResult<Vec<MemoryCleanupPending>> {
        if limit == 0 || limit > 256 {
            return Err(MemoryError::Store(
                "invalid Memory cleanup page size".into(),
            ));
        }
        let store = self.clone();
        let after = after_mutation.map(str::to_owned);
        run_memory_blocking(move || {
            let mut connection=store.executor.checkout_online_read().map_err(storage_memory_error)?;
            connection.query("SELECT payload,mutation_id,source_ref,delete_epoch FROM memory_cleanup_pending WHERE NOT confirmed AND ($1::TEXT IS NULL OR mutation_id>$1) ORDER BY mutation_id LIMIT $2", &[&after,&(limit as i64)]).map_err(postgres_memory_error)?.into_iter().map(|r| {
                let pending: MemoryCleanupPending = serde_json::from_value(r.try_get(0).map_err(postgres_memory_error)?).map_err(json_memory_error)?;
                pending.validate()?;
                if pending.mutation_id != r.try_get::<_, String>(1).map_err(postgres_memory_error)? || pending.source.source_ref != r.try_get::<_, String>(2).map_err(postgres_memory_error)? || i64::try_from(pending.source.delete_epoch).ok() != Some(r.try_get::<_, i64>(3).map_err(postgres_memory_error)?) { return Err(MemoryError::Store("Memory pending row identity mismatch".into())); }
                Ok(pending)
            }).collect()
        }).await
    }

    async fn confirm_memory_cleanup(
        &self,
        expected: &MemoryCleanupPending,
        knowledge: Option<&memory::knowledge::MemorySourceQuarantineReceipt>,
        auxiliary: bool,
    ) -> MemoryResult<()> {
        let store = self.clone();
        let expected = expected.clone();
        let knowledge = knowledge.cloned();
        run_memory_blocking(move || {
            let mut connection=store.executor.checkout_critical().map_err(storage_memory_error)?;
            let mut tx=connection.transaction().map_err(postgres_memory_error)?;
            let row=tx.query_opt("SELECT payload FROM memory_cleanup_pending WHERE mutation_id=$1 FOR UPDATE", &[&expected.mutation_id]).map_err(postgres_memory_error)?.ok_or_else(||MemoryError::NotFound(expected.mutation_id.clone()))?;
            let mut pending:MemoryCleanupPending=serde_json::from_value(row.try_get(0).map_err(postgres_memory_error)?).map_err(json_memory_error)?;
            if pending.source!=expected.source || pending.memory_id!=expected.memory_id {return Err(MemoryError::Store("foreign Memory cleanup confirmation".into()));}
            pending.confirm_knowledge(knowledge.as_ref())?; pending.auxiliary_confirmed |= auxiliary;
            if auxiliary {
                let id=expected.memory_id.to_string();let reference=expected.source.source_ref.clone();let scope=expected.source.scope_key.clone();
                tx.execute("DELETE FROM memory_symbol_references WHERE memory_id=$1", &[&id]).map_err(postgres_memory_error)?;
                tx.execute("DELETE FROM memory_entities WHERE scope_key=$1 AND jsonb_array_length(payload->'source_ids')>0 AND (payload->'source_ids') <@ to_jsonb($2::text[])", &[&scope,&vec![id.clone(),reference.clone()]]).map_err(postgres_memory_error)?;
                tx.execute("UPDATE memory_entities SET payload=jsonb_set(payload,'{source_ids}',(payload->'source_ids')-$2::text-$3::text) WHERE scope_key=$1 AND ((payload->'source_ids') ? $2 OR (payload->'source_ids') ? $3)", &[&scope,&id,&reference]).map_err(postgres_memory_error)?;
                tx.execute("DELETE FROM memory_triples WHERE scope_key=$1 AND payload->>'source'=ANY($2::text[])", &[&scope,&vec![id,reference]]).map_err(postgres_memory_error)?;
            }
            let complete=pending.knowledge_receipt_digest.is_some() && pending.auxiliary_confirmed;
            let payload=serde_json::to_value(&pending).map_err(json_memory_error)?;
            tx.execute("UPDATE memory_cleanup_pending SET confirmed=$2,payload=$3 WHERE mutation_id=$1", &[&expected.mutation_id,&complete,&payload]).map_err(postgres_memory_error)?;
            tx.commit().map_err(postgres_memory_error)?;Ok(())
        }).await
    }

    async fn search_fts(&self, query: &str, limit: usize) -> MemoryResult<Vec<MemoryEntry>> {
        let store = self.clone();
        let query = query.to_string();
        run_memory_blocking(move || store.search_memory(&query, None, None, None, limit)).await
    }
    async fn search_fts_scoped(
        &self,
        query: &str,
        scope: &MemoryScope,
        limit: usize,
    ) -> MemoryResult<Vec<MemoryEntry>> {
        let store = self.clone();
        let query = query.to_string();
        let scope = scope.scope_key();
        run_memory_blocking(move || store.search_memory(&query, Some(scope), None, None, limit))
            .await
    }
    async fn discover_page(
        &self,
        query: MemoryDiscoveryQuery,
    ) -> MemoryResult<MemoryDiscoveryPage> {
        let store = self.clone();
        run_memory_blocking(move || {
            let scopes = query.scopes.iter().map(MemoryScope::scope_key).collect::<std::collections::BTreeSet<_>>().into_iter().collect::<Vec<_>>();
            let mut connection = store.executor.checkout_online_read().map_err(storage_memory_error)?;
            let mut transaction = connection.transaction().map_err(postgres_memory_error)?;
            transaction.batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY").map_err(postgres_memory_error)?;
            let revisions = transaction.query("SELECT s.scope_key, coalesce(r.revision,0)::BIGINT FROM unnest($1::TEXT[]) AS s(scope_key) LEFT JOIN memory_discovery_revisions r USING(scope_key) ORDER BY s.scope_key", &[&scopes])
                .map_err(postgres_memory_error)?.into_iter().map(|row| (row.get::<_,String>(0), row.get::<_,i64>(1))).collect::<std::collections::BTreeMap<_,_>>();
            if query.expected_revisions.as_ref().is_some_and(|expected| expected != &revisions) {
                return Err(MemoryError::Store("memory discovery source changed; start a new search".into()));
            }
            let limit = query.limit.clamp(1,128); let sql_limit = (limit+1) as i64;
            let sql_skip = i64::try_from(query.skip).map_err(|_| MemoryError::Store("memory cursor skip is out of range".into()))?;
            let mut rows = transaction.query(
                "SELECT e.payload, k.key, k.value FROM memory_entries e LEFT JOIN memory_kv k ON k.key='memory_lifecycle:'||e.id
                 WHERE e.scope_key=ANY($1) AND ($2::TEXT IS NULL OR e.id > $2)
                   AND ($3='' OR to_tsvector('simple',e.title||' '||e.content||' '||coalesce(e.payload->>'tags','')) @@ websearch_to_tsquery('simple',$3)
                        OR e.title ILIKE '%'||$3||'%' OR e.content ILIKE '%'||$3||'%')
                 ORDER BY e.id ASC LIMIT $4 OFFSET $5", &[&scopes,&query.after_id,&query.query,&sql_limit,&sql_skip]).map_err(postgres_memory_error)?;
            let has_more = rows.len() > limit; rows.truncate(limit);
            let mut entries = Vec::with_capacity(rows.len()); let mut lifecycle = Vec::new();
            for row in rows {
                entries.push(serde_json::from_value::<MemoryEntry>(row.get(0)).map_err(json_memory_error)?);
                if let (Some(key),Some(value)) = (row.get::<_,Option<String>>(1),row.get::<_,Option<String>>(2)) { lifecycle.push(MemoryKeyValue {key,value}); }
            }
            let next_id = has_more.then(|| entries.last().expect("nonempty page").id.to_string());
            transaction.commit().map_err(postgres_memory_error)?;
            Ok(MemoryDiscoveryPage {entries,lifecycle,revisions,next_id})
        }).await
    }

    async fn search_fts_advanced(
        &self,
        query: &str,
        options: FtsSearchOptions,
        limit: usize,
    ) -> MemoryResult<FtsSearchResult> {
        let store = self.clone();
        let query = query.to_string();
        run_memory_blocking(move || {
            let total_matches = store.count_memory(&query, options.category, options.layer)?;
            let entries =
                store.search_memory(&query, None, options.category, options.layer, limit)?;
            let snippets = if options.with_snippets {
                entries
                    .iter()
                    .map(|entry| Some(entry.content.chars().take(160).collect()))
                    .collect()
            } else {
                vec![None; entries.len()]
            };
            let keywords = if options.with_keywords {
                matched_keywords(&query, &entries)
            } else {
                Vec::new()
            };
            Ok(FtsSearchResult {
                total_matches,
                entries,
                snippets,
                keywords,
            })
        })
        .await
    }
    async fn search_vector(
        &self,
        _embedding: &[f32],
        _limit: usize,
    ) -> MemoryResult<Vec<MemoryEntry>> {
        Err(MemoryError::CapabilityUnavailable {
            capability: "vector_search".to_string(),
            details: "PostgreSQL pgvector index is not configured for this deployment".to_string(),
        })
    }
    async fn search_by_layer(&self, layer: MemoryLayer) -> MemoryResult<Vec<MemoryEntry>> {
        let store = self.clone();
        run_memory_blocking(move || {
            store.entries(
                "SELECT payload FROM memory_entries WHERE layer=$1 ORDER BY created_at DESC",
                &[&enum_label(&layer)?],
            )
        })
        .await
    }
    async fn search_by_category(&self, category: MemoryCategory) -> MemoryResult<Vec<MemoryEntry>> {
        let store = self.clone();
        run_memory_blocking(move || {
            store.entries(
                "SELECT payload FROM memory_entries WHERE category=$1 ORDER BY created_at DESC",
                &[&enum_label(&category)?],
            )
        })
        .await
    }
    async fn lookup_authority_candidates(
        &self,
        query: AuthorityLookup,
    ) -> MemoryResult<Vec<MemoryEntry>> {
        let store = self.clone();
        run_memory_blocking(move || {
            let scope = query.scope.scope_key();
            let limit = limit_i64(query.limit.clamp(1, 256))?;
            let visibility = serde_json::to_value(&query.visibility)
                .map_err(|error| MemoryError::Store(error.to_string()))?;
            let private = matches!(query.visibility, memory::AgentVisibility::Private);
            store.entries(
                "SELECT payload FROM memory_entries
                  WHERE scope_key=$1 AND authority_fingerprint=$2
                    AND payload->'visibility'=$4::jsonb
                    AND (NOT $5::boolean OR ($6::text IS NOT NULL AND source_agent=$6))
                  ORDER BY updated_at DESC,id ASC LIMIT $3",
                &[
                    &scope,
                    &query.fingerprint,
                    &limit,
                    &visibility,
                    &private,
                    &query.source_agent,
                ],
            )
        })
        .await
    }
    async fn lookup_tagged_candidates(
        &self,
        query: TaggedLookup,
    ) -> MemoryResult<Vec<MemoryEntry>> {
        let store = self.clone();
        run_memory_blocking(move || {
            let scope = query.scope.scope_key();
            let limit = limit_i64(query.limit.clamp(1, 512))?;
            store.entries(
                "SELECT payload FROM memory_entries
                  WHERE scope_key=$1
                    AND ($2::text IS NULL OR source_agent=$2)
                    AND (payload -> 'tags') ?| $3::text[]
                  ORDER BY updated_at DESC,id ASC LIMIT $4",
                &[&scope, &query.source_agent, &query.tags_any, &limit],
            )
        })
        .await
    }
    async fn lookup_fact_candidates(
        &self,
        scope: &MemoryScope,
        category: MemoryCategory,
        limit: usize,
    ) -> MemoryResult<Vec<MemoryEntry>> {
        let store = self.clone();
        let scope = scope.scope_key();
        run_memory_blocking(move || {
            let category = enum_label(&category)?;
            let limit = limit_i64(limit.clamp(1, 1024))?;
            store.entries(
                "SELECT payload FROM memory_entries
                  WHERE scope_key=$1 AND category=$2
                  ORDER BY updated_at DESC,id ASC LIMIT $3",
                &[&scope, &category, &limit],
            )
        })
        .await
    }
    async fn search_semantic_checkpoints(
        &self,
        scope: &MemoryScope,
        query: &str,
        limit: usize,
    ) -> MemoryResult<Vec<MemoryEntry>> {
        let store = self.clone();
        let scope = scope.scope_key();
        let query = query.trim().to_string();
        run_memory_blocking(move || {
            let limit = limit_i64(limit.clamp(1, 256))?;
            store.entries(
                "SELECT payload FROM memory_entries
                  WHERE (scope_key=$1 OR scope_key='global')
                    AND payload->'tags' ? 'semantic-checkpoint'
                    AND ($2='' OR title ILIKE '%' || $2 || '%'
                               OR content ILIKE '%' || $2 || '%')
                  ORDER BY updated_at DESC,id ASC LIMIT $3",
                &[&scope, &query, &limit],
            )
        })
        .await
    }
    async fn scan_entries_page(
        &self,
        cursor: MemoryScanCursor,
        limit: usize,
    ) -> MemoryResult<MemoryScanPage> {
        let store = self.clone();
        run_memory_blocking(move || {
            let capped = limit.clamp(1, 1000);
            let limit = limit_i64(capped)?;
            let entries = if let (Some(updated_at), Some(id)) =
                (cursor.updated_at.as_deref(), cursor.id.as_deref())
            {
                store.entries(
                    "SELECT payload FROM memory_entries
                      WHERE updated_at < $1 OR (updated_at=$1 AND id>$2)
                      ORDER BY updated_at DESC,id ASC LIMIT $3",
                    &[&updated_at, &id, &limit],
                )?
            } else {
                store.entries(
                    "SELECT payload FROM memory_entries
                      ORDER BY updated_at DESC,id ASC LIMIT $1",
                    &[&limit],
                )?
            };
            let next = (entries.len() == capped)
                .then(|| entries.last())
                .flatten()
                .map(|last| MemoryScanCursor {
                    updated_at: Some(last.updated_at.to_rfc3339()),
                    id: Some(last.id.to_string()),
                });
            Ok(MemoryScanPage { entries, next })
        })
        .await
    }
    async fn aggregate(&self, stale_threshold: f32) -> MemoryResult<MemoryStoreAggregate> {
        let store = self.clone();
        run_memory_blocking(move || {
            let mut connection = store
                .executor
                .checkout_background()
                .map_err(storage_memory_error)?;
            let rows = connection
                .query(
                    "SELECT m.layer,
                            COUNT(*)::BIGINT AS retained_count,
                            SUM(CASE WHEN COALESCE(lifecycle.state, '') IN ('Archived', 'Superseded')
                                     THEN 0 ELSE 1 END)::BIGINT AS active_count,
                            SUM(CASE WHEN COALESCE(lifecycle.state, '') IN ('Archived', 'Superseded')
                                     THEN 1 ELSE 0 END)::BIGINT AS archived_count,
                            SUM(CASE WHEN m.layer IN ('L0', 'L1', 'L2')
                                     THEN 1 ELSE 0 END)::BIGINT AS orientation_like,
                            SUM(CASE WHEN (m.payload->>'confidence')::DOUBLE PRECISION < 0.35
                                     THEN 1 ELSE 0 END)::BIGINT AS conflicted,
                            SUM(CASE WHEN (m.payload->>'staleness')::DOUBLE PRECISION >= $1
                                     THEN 1 ELSE 0 END)::BIGINT AS stale,
                            SUM(CASE WHEN jsonb_array_length(COALESCE(m.payload->'tags', '[]'::jsonb)) > 0
                                           OR jsonb_array_length(COALESCE(m.payload->'relations', '[]'::jsonb)) > 0
                                     THEN 1 ELSE 0 END)::BIGINT AS linked
                     FROM memory_entries m
                     LEFT JOIN (
                         SELECT SUBSTRING(key FROM 18) AS memory_id,
                                value::jsonb -> -1 ->> 'to' AS state
                         FROM memory_kv WHERE key LIKE 'memory_lifecycle:%'
                     ) lifecycle ON lifecycle.memory_id = m.id
                     GROUP BY m.layer ORDER BY m.layer",
                    &[&f64::from(stale_threshold)],
                )
                .map_err(postgres_memory_error)?;
            let mut aggregate = MemoryStoreAggregate::default();
            for row in rows {
                let layer_label: String = row.try_get(0).map_err(postgres_memory_error)?;
                let layer = serde_json::from_value::<MemoryLayer>(serde_json::Value::String(
                    layer_label,
                ))
                .map_err(json_memory_error)?;
                let layer = MemoryLayerAggregate {
                    layer,
                    retained_count: nonnegative_u64(&row, 1)?,
                    active_count: nonnegative_u64(&row, 2)?,
                    archived_count: nonnegative_u64(&row, 3)?,
                };
                aggregate.total_entries += layer.retained_count;
                aggregate.active_entries += layer.active_count;
                aggregate.orientation_like += nonnegative_u64(&row, 4)?;
                aggregate.conflicted += nonnegative_u64(&row, 5)?;
                aggregate.stale += nonnegative_u64(&row, 6)?;
                aggregate.linked += nonnegative_u64(&row, 7)?;
                aggregate.layers.push(layer);
            }
            aggregate.evidence_backed = aggregate.total_entries;
            Ok(aggregate)
        })
        .await
    }
    async fn get_meta(&self, id: &MemoryId) -> MemoryResult<Option<MemoryMeta>> {
        let store = self.clone();
        let id = id.to_string();
        run_memory_blocking(move || {
            let mut connection = store
                .executor
                .checkout_online_read()
                .map_err(storage_memory_error)?;
            connection
                .query_opt("SELECT payload FROM memory_entries WHERE id=$1", &[&id])
                .map_err(postgres_memory_error)?
                .map(|row| {
                    let entry = entry_from_json(row.try_get(0).map_err(postgres_memory_error)?)?;
                    Ok(memory_meta(&entry))
                })
                .transpose()
        })
        .await
    }
    async fn list_metas(&self, layer: Option<MemoryLayer>) -> MemoryResult<Vec<MemoryMeta>> {
        let store = self.clone();
        run_memory_blocking(move || {
            let entries = match layer {
                Some(layer) => store.entries(
                    "SELECT payload FROM memory_entries WHERE layer=$1 ORDER BY created_at DESC",
                    &[&enum_label(&layer)?],
                )?,
                None => store.entries(
                    "SELECT payload FROM memory_entries ORDER BY created_at DESC",
                    &[],
                )?,
            };
            Ok(entries.iter().map(memory_meta).collect())
        })
        .await
    }
    async fn list_all(&self) -> MemoryResult<Vec<MemoryEntry>> {
        let store = self.clone();
        run_memory_blocking(move || {
            store.entries(
                "SELECT payload FROM memory_entries ORDER BY created_at DESC",
                &[],
            )
        })
        .await
    }
    async fn save_entities(&self, scope_key: &str, entities: &[Entity]) -> MemoryResult<()> {
        let store = self.clone();
        let scope_key = scope_key.to_string();
        let entities = entities.to_vec();
        run_memory_blocking(move || {
            store.replace_json_rows(&scope_key, "memory_entities", &entities, |value| {
                value
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
        })
        .await
    }
    async fn load_entities(&self, scope_key: &str) -> MemoryResult<Vec<Entity>> {
        let store = self.clone();
        let scope_key = scope_key.to_string();
        run_memory_blocking(move || store.load_json_rows(&scope_key, "memory_entities")).await
    }
    async fn save_triples(&self, scope_key: &str, triples: &[Triple]) -> MemoryResult<()> {
        let store = self.clone();
        let scope_key = scope_key.to_string();
        let triples = triples.to_vec();
        run_memory_blocking(move || store.replace_triples(&scope_key, &triples)).await
    }
    async fn save_knowledge_graph(
        &self,
        scope_key: &str,
        entities: &[Entity],
        triples: &[Triple],
    ) -> MemoryResult<()> {
        let store = self.clone();
        let scope_key = scope_key.to_string();
        let entities = entities.to_vec();
        let triples = triples.to_vec();
        run_memory_blocking(move || store.replace_knowledge_graph(&scope_key, &entities, &triples))
            .await
    }
    async fn load_triples(&self, scope_key: &str) -> MemoryResult<Vec<Triple>> {
        let store = self.clone();
        let scope_key = scope_key.to_string();
        run_memory_blocking(move || store.load_json_rows(&scope_key, "memory_triples")).await
    }
    async fn load_verbatim_by_id(&self, id: &str) -> MemoryResult<Option<VerbatimEntry>> {
        let store = self.clone();
        let id = id.to_string();
        run_memory_blocking(move || {
            let mut connection = store
                .executor
                .checkout_online_read()
                .map_err(storage_memory_error)?;
            connection
                .query_opt(
                    "SELECT id,content,source,layer,timestamp FROM memory_verbatim WHERE id=$1",
                    &[&id],
                )
                .map_err(postgres_memory_error)?
                .map(|row| row_to_verbatim(&row))
                .transpose()
        })
        .await
    }
    async fn search_verbatim_by_content(&self, query: &str) -> MemoryResult<Vec<VerbatimEntry>> {
        let store = self.clone();
        let query = query.to_string();
        run_memory_blocking(move || {
            let mut connection = store.executor.checkout_online_read().map_err(storage_memory_error)?;
            connection.query("SELECT id,content,source,layer,timestamp FROM memory_verbatim WHERE content LIKE $1 ORDER BY timestamp DESC", &[&query]).map_err(postgres_memory_error)?.iter().map(row_to_verbatim).collect()
        }).await
    }
    async fn list_verbatim_entries(&self) -> MemoryResult<Vec<VerbatimEntry>> {
        let store = self.clone();
        run_memory_blocking(move || {
            let mut connection = store.executor.checkout_online_read().map_err(storage_memory_error)?;
            connection.query("SELECT id,content,source,layer,timestamp FROM memory_verbatim ORDER BY timestamp,id", &[]).map_err(postgres_memory_error)?.iter().map(row_to_verbatim).collect()
        }).await
    }
    async fn insert_symbol(&self, symbol: &CodeSymbol) -> MemoryResult<()> {
        let store = self.clone();
        let symbol = symbol.clone();
        run_memory_blocking(move || store.upsert_symbol(&symbol)).await
    }
    async fn search_symbols(&self, query: &str, limit: usize) -> MemoryResult<Vec<CodeSymbol>> {
        let store = self.clone();
        let query = query.to_string();
        run_memory_blocking(move || store.symbols("SELECT id,name,kind,file_path,line,signature,doc FROM memory_code_symbols WHERE to_tsvector('simple',name || ' ' || signature || ' ' || coalesce(doc,'')) @@ websearch_to_tsquery('simple',$1) OR name ILIKE '%' || $1 || '%' ORDER BY name ASC,id ASC LIMIT $2", &[&query,&limit_i64(limit)?])).await
    }
    async fn insert_edge(&self, edge: &SymbolEdge) -> MemoryResult<()> {
        let store = self.clone();
        let edge = edge.clone();
        run_memory_blocking(move || {
            let mut connection = store.executor.checkout_critical().map_err(storage_memory_error)?;
            connection.execute("INSERT INTO memory_code_edges(source_id,target_id,edge_type,file_path) VALUES($1,$2,$3,$4) ON CONFLICT (source_id,target_id,edge_type,file_path) DO NOTHING", &[&edge.source_id,&edge.target_id,&edge.edge_type.as_str(),&edge.file_path]).map_err(postgres_memory_error)?;
            Ok(())
        }).await
    }
    async fn get_callers(&self, id: &str) -> MemoryResult<Vec<CodeSymbol>> {
        let store = self.clone();
        let id = id.to_string();
        run_memory_blocking(move || store.symbols("SELECT s.id,s.name,s.kind,s.file_path,s.line,s.signature,s.doc FROM memory_code_symbols s JOIN memory_code_edges e ON s.id=e.source_id WHERE e.target_id=$1 AND e.edge_type='calls' ORDER BY s.name,s.id", &[&id])).await
    }
    async fn get_callees(&self, id: &str) -> MemoryResult<Vec<CodeSymbol>> {
        let store = self.clone();
        let id = id.to_string();
        run_memory_blocking(move || store.symbols("SELECT s.id,s.name,s.kind,s.file_path,s.line,s.signature,s.doc FROM memory_code_symbols s JOIN memory_code_edges e ON s.id=e.target_id WHERE e.source_id=$1 AND e.edge_type='calls' ORDER BY s.name,s.id", &[&id])).await
    }
    async fn list_all_symbols(&self) -> MemoryResult<Vec<CodeSymbol>> {
        let store = self.clone();
        run_memory_blocking(move || {
            store.symbols(
            "SELECT id,name,kind,file_path,line,signature,doc FROM memory_code_symbols ORDER BY id",
            &[],
        )
        })
        .await
    }
    async fn list_all_edges(&self) -> MemoryResult<Vec<SymbolEdge>> {
        let store = self.clone();
        run_memory_blocking(move || {
            let mut connection = store.executor.checkout_online_read().map_err(storage_memory_error)?;
            connection.query("SELECT source_id,target_id,edge_type,file_path FROM memory_code_edges ORDER BY source_id,target_id,edge_type,file_path", &[]).map_err(postgres_memory_error)?.iter().map(row_to_edge).collect()
        }).await
    }
    async fn link_symbol_to_memory(
        &self,
        symbol_id: &str,
        memory_id: &MemoryId,
        turn_index: Option<i32>,
        reference_type: &str,
        timestamp: i64,
    ) -> MemoryResult<()> {
        let store = self.clone();
        let symbol_id = symbol_id.to_string();
        let memory_id = memory_id.to_string();
        let reference_type = reference_type.to_string();
        run_memory_blocking(move || {
            let mut connection = store.executor.checkout_critical().map_err(storage_memory_error)?;
            connection.execute("INSERT INTO memory_symbol_references(symbol_id,memory_id,turn_index,reference_type,timestamp) VALUES($1,$2,$3,$4,$5) ON CONFLICT (symbol_id, memory_id, COALESCE(turn_index, -1), COALESCE(reference_type, '')) DO NOTHING", &[&symbol_id,&memory_id,&turn_index,&reference_type,&timestamp]).map_err(postgres_memory_error)?;
            Ok(())
        }).await
    }
    async fn find_memories_by_symbol(&self, symbol_name: &str) -> MemoryResult<Vec<MemoryId>> {
        let store = self.clone();
        let symbol_name = symbol_name.to_string();
        run_memory_blocking(move || {
            let pattern = format!("%{}%", symbol_name.to_ascii_lowercase());
            let mut connection = store.executor.checkout_online_read().map_err(storage_memory_error)?;
            connection.query("SELECT memory_id FROM memory_symbol_references WHERE lower(symbol_id) LIKE $1 OR symbol_id=$2 GROUP BY memory_id ORDER BY max(timestamp) DESC,memory_id", &[&pattern,&symbol_name]).map_err(postgres_memory_error)?.iter().map(|row| uuid::Uuid::parse_str(&row.try_get::<_,String>(0).map_err(postgres_memory_error)?).map_err(|error| MemoryError::Store(format!("invalid symbol reference memory id: {error}")))).collect()
        }).await
    }
    async fn list_symbol_memory_references(&self) -> MemoryResult<Vec<SymbolMemoryReference>> {
        let store = self.clone();
        run_memory_blocking(move || {
            let mut connection = store.executor.checkout_online_read().map_err(storage_memory_error)?;
            connection.query("SELECT symbol_id,memory_id,turn_index,reference_type,timestamp FROM memory_symbol_references ORDER BY timestamp,symbol_id,memory_id,reference_type", &[]).map_err(postgres_memory_error)?.iter().map(row_to_symbol_reference).collect()
        }).await
    }
    async fn kv_put(&self, key: &str, value: &str) -> MemoryResult<()> {
        let store = self.clone();
        let key = key.to_string();
        let value = value.to_string();
        run_memory_blocking(move || {
            let mut connection = store.executor.checkout_critical().map_err(storage_memory_error)?;
            let mut tx=connection.transaction().map_err(postgres_memory_error)?;
            guard_legacy_lifecycle_kv(&mut tx,&key)?;
            tx.execute("INSERT INTO memory_kv(key,value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value", &[&key,&value]).map_err(postgres_memory_error)?;
            tx.commit().map_err(postgres_memory_error)?;
            Ok(())
        }).await
    }
    async fn kv_delete(&self, key: &str) -> MemoryResult<()> {
        let store = self.clone();
        let key = key.to_string();
        run_memory_blocking(move || {
            let mut connection = store
                .executor
                .checkout_critical()
                .map_err(storage_memory_error)?;
            let mut tx = connection.transaction().map_err(postgres_memory_error)?;
            guard_legacy_lifecycle_kv(&mut tx, &key)?;
            tx.execute("DELETE FROM memory_kv WHERE key=$1", &[&key])
                .map_err(postgres_memory_error)?;
            tx.commit().map_err(postgres_memory_error)?;
            Ok(())
        })
        .await
    }
    async fn kv_get(&self, key: &str) -> MemoryResult<Option<String>> {
        let store = self.clone();
        let key = key.to_string();
        run_memory_blocking(move || {
            let mut connection = store
                .executor
                .checkout_online_read()
                .map_err(storage_memory_error)?;
            connection
                .query_opt("SELECT value FROM memory_kv WHERE key=$1", &[&key])
                .map_err(postgres_memory_error)?
                .map(|row| row.try_get(0).map_err(postgres_memory_error))
                .transpose()
        })
        .await
    }
    async fn kv_get_many(&self, keys: &[String]) -> MemoryResult<Vec<MemoryKeyValue>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let store = self.clone();
        let keys = keys.to_vec();
        run_memory_blocking(move || {
            let mut connection = store
                .executor
                .checkout_online_read()
                .map_err(storage_memory_error)?;
            connection
                .query(
                    "SELECT key,value FROM memory_kv
                      WHERE key=ANY($1::text[]) ORDER BY key",
                    &[&keys],
                )
                .map_err(postgres_memory_error)?
                .iter()
                .map(|row| {
                    Ok(MemoryKeyValue {
                        key: row.try_get(0).map_err(postgres_memory_error)?,
                        value: row.try_get(1).map_err(postgres_memory_error)?,
                    })
                })
                .collect()
        })
        .await
    }
    async fn list_key_values(&self) -> MemoryResult<Vec<MemoryKeyValue>> {
        let store = self.clone();
        run_memory_blocking(move || {
            let mut connection = store
                .executor
                .checkout_online_read()
                .map_err(storage_memory_error)?;
            connection
                .query("SELECT key,value FROM memory_kv ORDER BY key", &[])
                .map_err(postgres_memory_error)?
                .iter()
                .map(|row| {
                    Ok(MemoryKeyValue {
                        key: row.try_get(0).map_err(postgres_memory_error)?,
                        value: row.try_get(1).map_err(postgres_memory_error)?,
                    })
                })
                .collect()
        })
        .await
    }
}

#[derive(Clone, Debug)]
pub struct PostgresKnowledgeStore {
    executor: PostgresExecutor,
}

impl PostgresKnowledgeStore {
    pub fn new(executor: PostgresExecutor) -> Result<Self, KnowledgeStoreError> {
        run_driver_sync(
            move || {
                executor
                    .apply_migrations(KNOWLEDGE_DOMAIN, KNOWLEDGE_MIGRATIONS)
                    .map_err(storage_knowledge_error)?;
                Ok(Self { executor })
            },
            || {
                KnowledgeStoreError::Backend(
                    "PostgreSQL knowledge initialization thread panicked".to_string(),
                )
            },
        )
    }
    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> Result<Self, KnowledgeStoreError> {
        run_driver_sync(
            move || {
                let executor =
                    PostgresExecutor::connect(config, resolver).map_err(storage_knowledge_error)?;
                executor
                    .apply_migrations(KNOWLEDGE_DOMAIN, KNOWLEDGE_MIGRATIONS)
                    .map_err(storage_knowledge_error)?;
                Ok(Self { executor })
            },
            || {
                KnowledgeStoreError::Backend(
                    "PostgreSQL knowledge connection thread panicked".to_string(),
                )
            },
        )
    }
    #[must_use]
    pub fn executor(&self) -> &PostgresExecutor {
        &self.executor
    }
}

impl KnowledgeStore for PostgresKnowledgeStore {
    fn quarantine_source(
        &self,
        source_ref: &str,
        retirement: Option<&memory::store::VerifiedMemoryRetirement>,
    ) -> Result<memory::knowledge::KnowledgeSourceQuarantine, KnowledgeStoreError> {
        memory::knowledge::validate_quarantine_source(source_ref, retirement)?;
        let store = self.clone();
        let source_ref = source_ref.to_owned();
        let retirement = retirement.map(|proof| proof.source().clone());
        run_driver_sync(
            move || {
                let mut connection = store
                    .executor
                    .checkout_critical()
                    .map_err(storage_knowledge_error)?;
                let mut tx = connection.transaction().map_err(postgres_knowledge_error)?;
                if let Some(expected) = retirement.as_ref() {
                    // Same original Memory fence, checked inside this native Knowledge
                    // transaction. No observation or boolean can stand for its epoch.
                    let row = tx.query_opt("SELECT payload,scope_key,delete_epoch FROM memory_source_fences WHERE source_ref=$1 FOR SHARE", &[&source_ref]).map_err(postgres_knowledge_error)?.ok_or(KnowledgeStoreError::MemorySourceUnknown)?;
                    let current: MemorySourceFence =
                        serde_json::from_value(row.try_get(0).map_err(postgres_knowledge_error)?)
                            .map_err(json_knowledge_error)?;
                    let scope: String = row.try_get(1).map_err(postgres_knowledge_error)?;
                    let epoch: i64 = row.try_get(2).map_err(postgres_knowledge_error)?;
                    if scope != current.scope_key
                        || u64::try_from(epoch).ok() != Some(current.delete_epoch)
                    {
                        return Err(KnowledgeStoreError::StaleRead);
                    }
                    current
                        .verifies_retirement(expected)
                        .map_err(|_| KnowledgeStoreError::StaleRead)?;
                }
                // Original maintenance lock order also closes the absent-receipt / new
                // ingestion gap. Ingestion takes ROW EXCLUSIVE before reading receipts.
                tx.batch_execute("LOCK TABLE knowledge_corpus, knowledge_pack, knowledge_conflict IN SHARE ROW EXCLUSIVE MODE").map_err(postgres_knowledge_error)?;
                if let Some(expected) = retirement.as_ref() {
                    let epoch = i64::try_from(expected.delete_epoch)
                        .map_err(|_| KnowledgeStoreError::StaleRead)?;
                    if let Some(row) = tx.query_opt("SELECT CASE WHEN octet_length(payload::text)<=8388608 THEN payload END,receipt_digest FROM knowledge_memory_retirement_receipts WHERE source_ref=$1 AND scope_key=$2 AND delete_epoch=$3", &[&source_ref,&expected.scope_key,&epoch]).map_err(postgres_knowledge_error)? {
                    let receipt: memory::knowledge::MemorySourceQuarantineReceipt = serde_json::from_value(row.try_get::<_, Option<serde_json::Value>>(0).map_err(postgres_knowledge_error)?.ok_or(KnowledgeStoreError::ReadLimit)?).map_err(json_knowledge_error)?;
                    if receipt.source != *expected || receipt.digest()? != row.try_get::<_, String>(1).map_err(postgres_knowledge_error)? { return Err(KnowledgeStoreError::StaleRead); }
                    tx.commit().map_err(postgres_knowledge_error)?;
                    return Ok(memory::knowledge::KnowledgeSourceQuarantine { pack_ids: receipt.pack_ids.clone(), memory_receipt: Some(receipt) });
                }
                }
                let frame = bounded_knowledge_source_snapshot(&mut tx, &source_ref)?;
                let (packs, conflicts) = memory::knowledge::prepare_source_quarantine(frame);
                let pack_ids: Vec<String> = packs.iter().map(|pack| pack.pack_id.clone()).collect();
                let receipt =
                    retirement.map(|source| memory::knowledge::MemorySourceQuarantineReceipt {
                        source,
                        pack_ids: pack_ids.clone(),
                    });
                let receipt_digest = receipt
                    .as_ref()
                    .map(|receipt| receipt.digest())
                    .transpose()?;
                apply_knowledge_maintenance(&mut tx, &packs, &conflicts)?;
                if let Some(receipt) = receipt.as_ref() {
                    let epoch = i64::try_from(receipt.source.delete_epoch)
                        .map_err(|_| KnowledgeStoreError::StaleRead)?;
                    tx.execute("INSERT INTO knowledge_memory_retirement_receipts(source_ref,scope_key,delete_epoch,receipt_digest,payload) VALUES($1,$2,$3,$4,$5)", &[&source_ref,&receipt.source.scope_key,&epoch,&receipt_digest.as_ref().ok_or(KnowledgeStoreError::StaleRead)?,&serde_json::to_value(receipt).map_err(json_knowledge_error)?]).map_err(postgres_knowledge_error)?;
                }
                tx.commit().map_err(postgres_knowledge_error)?;
                Ok(memory::knowledge::KnowledgeSourceQuarantine {
                    pack_ids,
                    memory_receipt: receipt,
                })
            },
            || KnowledgeStoreError::Backend("PostgreSQL source quarantine thread panicked".into()),
        )
    }
    fn save_receipt(
        &self,
        receipt: &KnowledgeIngestionReceipt,
    ) -> Result<KnowledgeIngestionReceipt, KnowledgeStoreError> {
        let store = self.clone();
        let receipt = receipt.clone();
        run_driver_sync(
            move || {
                let mut connection = store
                    .executor
                    .checkout_critical()
                    .map_err(storage_knowledge_error)?;
                let mut tx = connection.transaction().map_err(postgres_knowledge_error)?;
                tx.batch_execute("LOCK TABLE knowledge_corpus, knowledge_pack, knowledge_conflict IN ROW EXCLUSIVE MODE").map_err(postgres_knowledge_error)?;
                let (key, digest) = memory::knowledge::knowledge_ingestion_identity(&receipt)?;
                let inserted = tx.execute("INSERT INTO knowledge_ingestion_receipt(request_key,input_digest,payload) VALUES($1,$2,$3) ON CONFLICT(request_key) DO NOTHING", &[&key,&digest,&serde_json::to_value(&receipt).map_err(json_knowledge_error)?]).map_err(postgres_knowledge_error)?;
                if inserted == 0 {
                    let row = tx.query_one("SELECT input_digest, payload FROM knowledge_ingestion_receipt WHERE request_key=$1", &[&key]).map_err(postgres_knowledge_error)?;
                    let original_digest: String =
                        row.try_get(0).map_err(postgres_knowledge_error)?;
                    if original_digest != digest {
                        return Err(KnowledgeStoreError::Backend(
                            "knowledge ingestion identity conflict".into(),
                        ));
                    }
                    let payload: serde_json::Value =
                        row.try_get(1).map_err(postgres_knowledge_error)?;
                    let original = serde_json::from_value(payload).map_err(json_knowledge_error)?;
                    tx.commit().map_err(postgres_knowledge_error)?;
                    return Ok(original);
                }
                if tx.query_opt("SELECT 1 FROM knowledge_memory_retirement_receipts WHERE source_ref=$1 LIMIT 1", &[&receipt.corpus.source_ref.id]).map_err(postgres_knowledge_error)?.is_some() {
                    return Err(KnowledgeStoreError::StaleRead);
                }
                upsert_knowledge_json(
                    &mut tx,
                    "knowledge_corpus",
                    "corpus_id",
                    &receipt.corpus.corpus_id,
                    &receipt.corpus.namespace.key(),
                    None,
                    &receipt.corpus.updated_at.to_rfc3339(),
                    &receipt.corpus,
                )?;
                upsert_knowledge_json(
                    &mut tx,
                    "knowledge_pack",
                    "pack_id",
                    &receipt.pack.pack_id,
                    &receipt.pack.namespace.key(),
                    Some(knowledge_label(&receipt.pack.state)?),
                    &receipt.pack.updated_at.to_rfc3339(),
                    &receipt.pack,
                )?;
                tx.execute("INSERT INTO knowledge_canon(canon_id,pack_id,updated_at,payload) VALUES($1,$2,$3,$4) ON CONFLICT(canon_id) DO UPDATE SET pack_id=EXCLUDED.pack_id,updated_at=EXCLUDED.updated_at,payload=EXCLUDED.payload", &[&receipt.canon.canon_id,&receipt.canon.pack_id,&receipt.canon.updated_at.to_rfc3339(),&serde_json::to_value(&receipt.canon).map_err(json_knowledge_error)?]).map_err(postgres_knowledge_error)?;
                for conflict in &receipt.conflicts {
                    tx.execute("INSERT INTO knowledge_conflict(conflict_id,pack_id,state,detected_at,payload) VALUES($1,$2,$3,$4,$5) ON CONFLICT(conflict_id) DO UPDATE SET pack_id=EXCLUDED.pack_id,state=EXCLUDED.state,detected_at=EXCLUDED.detected_at,payload=EXCLUDED.payload", &[&conflict.conflict_id,&conflict.pack_id,&knowledge_label(&conflict.state)?,&conflict.detected_at.to_rfc3339(),&serde_json::to_value(conflict).map_err(json_knowledge_error)?]).map_err(postgres_knowledge_error)?;
                }
                for chunk in &receipt.chunks {
                    tx.execute("INSERT INTO knowledge_chunk(chunk_id,corpus_id,ordinal,title,text,payload) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(chunk_id) DO UPDATE SET corpus_id=EXCLUDED.corpus_id,ordinal=EXCLUDED.ordinal,title=EXCLUDED.title,text=EXCLUDED.text,payload=EXCLUDED.payload", &[&chunk.chunk_id,&chunk.corpus_id,&i64::try_from(chunk.ordinal).map_err(|_|KnowledgeStoreError::Backend("knowledge chunk ordinal overflow".to_string()))?,&chunk.title,&chunk.text,&serde_json::to_value(chunk).map_err(json_knowledge_error)?]).map_err(postgres_knowledge_error)?;
                }
                tx.commit().map_err(postgres_knowledge_error)?;
                Ok(receipt)
            },
            || {
                KnowledgeStoreError::Backend(
                    "PostgreSQL knowledge write thread panicked".to_string(),
                )
            },
        )
    }

    fn record_usage(&self, signal: &KnowledgeUsageSignal) -> Result<(), KnowledgeStoreError> {
        let store = self.clone();
        let signal = signal.clone();
        run_driver_sync(
            move || {
                let mut connection = store
                    .executor
                    .checkout_background()
                    .map_err(storage_knowledge_error)?;
                let changed = connection.execute("INSERT INTO knowledge_usage(signal_id,pack_id,session_id,occurred_at,payload) VALUES($1,$2,$3,$4,$5) ON CONFLICT(signal_id) DO UPDATE SET payload=knowledge_usage.payload WHERE knowledge_usage.payload=EXCLUDED.payload", &[&signal.signal_id,&signal.pack_id,&signal.session_id,&signal.occurred_at.to_rfc3339(),&serde_json::to_value(&signal).map_err(json_knowledge_error)?]).map_err(postgres_knowledge_error)?;
                if changed != 1 {
                    return Err(KnowledgeStoreError::Backend(
                        "knowledge usage identity conflict".into(),
                    ));
                }
                Ok(())
            },
            || {
                KnowledgeStoreError::Backend(
                    "PostgreSQL knowledge usage thread panicked".to_string(),
                )
            },
        )
    }
    fn read_candidate_page(
        &self,
        scope: &memory::knowledge::KnowledgeCandidateScope,
        cursor: Option<&memory::knowledge::KnowledgeCandidateCursor>,
        limit: usize,
        budget_bytes: usize,
    ) -> Result<memory::knowledge::KnowledgeCandidatePage, KnowledgeStoreError> {
        use memory::knowledge::{
            KnowledgeCandidateCursor, KnowledgeCandidatePage, KnowledgeCandidateScope,
            KnowledgeSourceRevision, KNOWLEDGE_CANDIDATE_PAGE_SIZE, KNOWLEDGE_READ_MAX_BYTES,
        };
        let store = self.clone();
        let scope = scope.clone();
        let cursor = cursor.cloned();
        let limit = limit.clamp(1, KNOWLEDGE_CANDIDATE_PAGE_SIZE);
        let budget_bytes = budget_bytes.min(KNOWLEDGE_READ_MAX_BYTES);
        run_driver_sync(
            move || {
                let mut connection = store
                    .executor
                    .checkout_online_read()
                    .map_err(storage_knowledge_error)?;
                let mut tx = connection.transaction().map_err(postgres_knowledge_error)?;
                tx.batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                    .map_err(postgres_knowledge_error)?;
                let keys = scope.revision_keys();
                let mut counters = keys
                    .iter()
                    .map(|key| (key.clone(), 0u64))
                    .collect::<std::collections::BTreeMap<_, _>>();
                for row in tx.query("SELECT scope_key,revision FROM knowledge_candidate_revision WHERE scope_key=ANY($1::text[]) ORDER BY scope_key", &[&keys]).map_err(postgres_knowledge_error)? {
                let key: String = row.try_get(0).map_err(postgres_knowledge_error)?;
                let value: i64 = row.try_get(1).map_err(postgres_knowledge_error)?;
                counters.insert(key, u64::try_from(value).map_err(|_| KnowledgeStoreError::StaleRead)?);
            }
                let store_id: String = tx
                    .query_one(
                        "SELECT store_id FROM knowledge_candidate_owner WHERE singleton",
                        &[],
                    )
                    .map_err(postgres_knowledge_error)?
                    .try_get(0)
                    .map_err(postgres_knowledge_error)?;
                let revision = KnowledgeSourceRevision { store_id, counters };
                if let Some(cursor) = &cursor {
                    cursor.validate(&scope, &revision)?;
                }
                let (namespace, project) = match &scope {
                    KnowledgeCandidateScope::Namespace(namespace) => (Some(namespace.key()), None),
                    KnowledgeCandidateScope::Activation { project_id } => {
                        (None, project_id.clone())
                    }
                };
                let after = cursor.as_ref().map(|cursor| cursor.after_pack_id.clone());
                let fetch = i64::try_from(limit + 1).map_err(|_| KnowledgeStoreError::ReadLimit)?;
                // Only bounded IDs and lengths cross the driver boundary here.
                // Exact namespace/project filtering precedes keyset limit and size
                // reservation; foreign payload cannot consume this page's budget.
                let metadata = tx.query("WITH page AS (SELECT pack_id,payload FROM knowledge_pack WHERE (($1::text IS NOT NULL AND namespace_key=$1) OR ($1::text IS NULL AND ((NOT starts_with(namespace_key,'project:') AND NOT starts_with(namespace_key,'project_group:')) OR namespace_key='project:'||$2::text OR namespace_key='project_group:'||$2::text))) AND ($3::text IS NULL OR pack_id>$3) ORDER BY pack_id LIMIT $4) SELECT page.pack_id,octet_length(page.payload::text)::bigint,canon.canon_id,COALESCE(octet_length(canon.payload::text),0)::bigint,page.payload->>'canon_pack_ref' FROM page LEFT JOIN knowledge_canon canon ON canon.canon_id=page.payload->>'canon_pack_ref' AND canon.pack_id=page.pack_id ORDER BY page.pack_id", &[&namespace,&project,&after,&fetch]).map_err(postgres_knowledge_error)?;
                let has_more = metadata.len() > limit;
                let mut ids = Vec::<String>::new();
                let mut canon_ids = Vec::<String>::new();
                let mut bytes = 0usize;
                for row in metadata.into_iter().take(limit) {
                    let id: String = row.try_get(0).map_err(postgres_knowledge_error)?;
                    let pack_bytes: i64 = row.try_get(1).map_err(postgres_knowledge_error)?;
                    let canon: Option<String> = row.try_get(2).map_err(postgres_knowledge_error)?;
                    let canon_bytes: i64 = row.try_get(3).map_err(postgres_knowledge_error)?;
                    let referenced: Option<String> =
                        row.try_get(4).map_err(postgres_knowledge_error)?;
                    if referenced.is_some() && canon.is_none() {
                        return Err(KnowledgeStoreError::Backend(
                            "Knowledge candidate has no canonical source".into(),
                        ));
                    }
                    for length in [pack_bytes, canon_bytes] {
                        bytes = bytes
                            .checked_add(
                                usize::try_from(length)
                                    .map_err(|_| KnowledgeStoreError::ReadLimit)?,
                            )
                            .ok_or(KnowledgeStoreError::ReadLimit)?;
                    }
                    if bytes > budget_bytes {
                        return Err(KnowledgeStoreError::ReadLimit);
                    }
                    ids.push(id);
                    if let Some(canon) = canon {
                        canon_ids.push(canon);
                    }
                }
                let packs = tx.query("SELECT payload FROM knowledge_pack WHERE pack_id=ANY($1::text[]) ORDER BY pack_id", &[&ids]).map_err(postgres_knowledge_error)?.iter().map(|row| {
                let value: serde_json::Value = row.try_get(0).map_err(postgres_knowledge_error)?;
                serde_json::from_value(value).map_err(json_knowledge_error)
            }).collect::<Result<Vec<KnowledgePack>,_>>()?;
                let source_ids = packs
                    .iter()
                    .flat_map(|p| p.source_corpus_refs.iter().cloned())
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                if source_ids.len() > memory::knowledge::KNOWLEDGE_READ_MAX_ROWS {
                    return Err(KnowledgeStoreError::ReadLimit);
                }
                let namespaces = packs.iter().map(|p| p.namespace.key()).collect::<Vec<_>>();
                // Only selected namespace metadata participates; reserve its exact
                // serialized source strings before transferring them from PG.
                let lengths=tx.query("SELECT corpus_id,COALESCE(octet_length((payload->'namespace')::text),0)::bigint+COALESCE(octet_length(payload->'source_ref'->>'id'),0)::bigint+octet_length(corpus_id)::bigint FROM knowledge_corpus WHERE corpus_id=ANY($1::text[]) AND namespace_key=ANY($2::text[]) ORDER BY corpus_id", &[&source_ids,&namespaces]).map_err(postgres_knowledge_error)?;
                for row in lengths {
                    let length: i64 = row.try_get(1).map_err(postgres_knowledge_error)?;
                    bytes = bytes
                        .checked_add(
                            usize::try_from(length).map_err(|_| KnowledgeStoreError::ReadLimit)?,
                        )
                        .ok_or(KnowledgeStoreError::ReadLimit)?;
                    if bytes > budget_bytes {
                        return Err(KnowledgeStoreError::ReadLimit);
                    }
                }
                let sources=tx.query("SELECT corpus_id,payload->'namespace',payload->'source_ref'->>'id' FROM knowledge_corpus WHERE corpus_id=ANY($1::text[]) AND namespace_key=ANY($2::text[]) ORDER BY corpus_id", &[&source_ids,&namespaces]).map_err(postgres_knowledge_error)?.into_iter().map(|row| {
                    Ok(memory::knowledge::KnowledgeCandidateSource {corpus_id:row.try_get(0).map_err(postgres_knowledge_error)?,namespace:serde_json::from_value(row.try_get(1).map_err(postgres_knowledge_error)?).map_err(json_knowledge_error)?,source_ref:row.try_get(2).map_err(postgres_knowledge_error)?})
                }).collect::<Result<Vec<_>,KnowledgeStoreError>>()?;
                let canon = tx.query("SELECT payload FROM knowledge_canon WHERE canon_id=ANY($1::text[]) ORDER BY canon_id", &[&canon_ids]).map_err(postgres_knowledge_error)?.iter().map(|row| {
                let value: serde_json::Value = row.try_get(0).map_err(postgres_knowledge_error)?;
                serde_json::from_value(value).map_err(json_knowledge_error)
            }).collect::<Result<Vec<harness_contract::knowledge::KnowledgeCanonPack>,_>>()?;
                let next = if has_more {
                    Some(KnowledgeCandidateCursor {
                        scope,
                        revision: revision.clone(),
                        after_pack_id: ids.last().ok_or(KnowledgeStoreError::StaleRead)?.clone(),
                    })
                } else {
                    None
                };
                tx.commit().map_err(postgres_knowledge_error)?;
                Ok(KnowledgeCandidatePage {
                    sources,
                    packs,
                    canon,
                    revision,
                    materialized_bytes: bytes,
                    next,
                })
            },
            || {
                KnowledgeStoreError::Backend(
                    "PostgreSQL knowledge candidate read thread panicked".into(),
                )
            },
        )
    }

    fn maintenance_inputs(&self) -> Result<KnowledgeSnapshot, KnowledgeStoreError> {
        let store = self.clone();
        run_driver_sync(
            move || {
                let mut connection = store
                    .executor
                    .checkout_online_read()
                    .map_err(storage_knowledge_error)?;
                let mut tx = connection.transaction().map_err(postgres_knowledge_error)?;
                tx.batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                    .map_err(postgres_knowledge_error)?;
                let snapshot = bounded_knowledge_snapshot(&mut tx, false)?;
                tx.commit().map_err(postgres_knowledge_error)?;
                Ok(snapshot)
            },
            || {
                KnowledgeStoreError::Backend(
                    "PostgreSQL knowledge maintenance read thread panicked".into(),
                )
            },
        )
    }
    fn projection(&self) -> Result<serde_json::Value, KnowledgeStoreError> {
        let store = self.clone();
        run_driver_sync(
            move || {
                let mut connection = store
                    .executor
                    .checkout_online_read()
                    .map_err(storage_knowledge_error)?;
                let mut tx = connection.transaction().map_err(postgres_knowledge_error)?;
                tx.batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                    .map_err(postgres_knowledge_error)?;
                let snapshot = bounded_knowledge_snapshot(&mut tx, false)?;
                let count: i64 = tx
                    .query_one("SELECT count(*) FROM knowledge_usage", &[])
                    .map_err(postgres_knowledge_error)?
                    .try_get(0)
                    .map_err(postgres_knowledge_error)?;
                let count = usize::try_from(count).map_err(|_| KnowledgeStoreError::ReadLimit)?;
                let projection =
                    memory::knowledge::knowledge_projection_from_metadata(snapshot, count)?;
                tx.commit().map_err(postgres_knowledge_error)?;
                Ok(projection)
            },
            || {
                KnowledgeStoreError::Backend(
                    "PostgreSQL knowledge projection read thread panicked".into(),
                )
            },
        )
    }

    fn snapshot(&self) -> Result<KnowledgeSnapshot, KnowledgeStoreError> {
        let store = self.clone();
        run_driver_sync(
            move || {
                let mut connection = store
                    .executor
                    .checkout_online_read()
                    .map_err(storage_knowledge_error)?;
                let mut tx = connection.transaction().map_err(postgres_knowledge_error)?;
                tx.batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                    .map_err(postgres_knowledge_error)?;
                let snapshot = bounded_knowledge_snapshot(&mut tx, true)?;
                tx.commit().map_err(postgres_knowledge_error)?;
                Ok(snapshot)
            },
            || KnowledgeStoreError::Backend("PostgreSQL knowledge read thread panicked".into()),
        )
    }

    fn commit_maintenance(
        &self,
        expected: &KnowledgeSnapshot,
        packs: &[KnowledgePack],
        conflicts: &[KnowledgeConflictRecord],
    ) -> Result<(), KnowledgeStoreError> {
        let store = self.clone();
        let expected = expected.clone();
        let packs = packs.to_vec();
        let conflicts = conflicts.to_vec();
        run_driver_sync(
            move || {
                let mut connection = store
                    .executor
                    .checkout_critical()
                    .map_err(storage_knowledge_error)?;
                let mut tx = connection.transaction().map_err(postgres_knowledge_error)?;
                // Original writers acquire ROW EXCLUSIVE on these tables. Taking
                // the conflicting locks before reading closes the CAS/write gap,
                // including inserts of previously absent corpus/pack/conflict IDs.
                tx.batch_execute("LOCK TABLE knowledge_corpus, knowledge_pack, knowledge_conflict IN SHARE ROW EXCLUSIVE MODE").map_err(postgres_knowledge_error)?;
                let current = bounded_knowledge_snapshot(&mut tx, false)?;
                if !memory::knowledge::same_knowledge_maintenance_inputs(&expected, &current)? {
                    return Err(KnowledgeStoreError::StaleRead);
                }
                apply_knowledge_maintenance(&mut tx, &packs, &conflicts)?;
                tx.commit().map_err(postgres_knowledge_error)?;
                Ok(())
            },
            || {
                KnowledgeStoreError::Backend(
                    "PostgreSQL knowledge maintenance thread panicked".into(),
                )
            },
        )
    }
}

fn guard_legacy_lifecycle_kv(client: &mut impl PostgresClient, key: &str) -> MemoryResult<()> {
    if let Some(id) = key.strip_prefix("memory_lifecycle:") {
        client
            .query_opt(
                "SELECT id FROM memory_entries WHERE id=$1 FOR UPDATE",
                &[&id],
            )
            .map_err(postgres_memory_error)?;
        if client
            .query_opt(
                "SELECT source_ref FROM memory_source_fences WHERE source_ref=$1",
                &[&format!("memory:{id}")],
            )
            .map_err(postgres_memory_error)?
            .is_some()
        {
            return Err(MemoryError::Store(
                "owned Memory lifecycle requires typed transaction".into(),
            ));
        }
    }
    Ok(())
}

fn write_entry(
    client: &mut impl PostgresClient,
    entry: &MemoryEntry,
    upsert: bool,
) -> MemoryResult<()> {
    let id = entry.id.to_string();
    // Same-target row lock orders lifecycle retirement against existing-body
    // writers. The source tombstone remains even when a body is later erased.
    client
        .query_opt(
            "SELECT id FROM memory_entries WHERE id=$1 FOR UPDATE",
            &[&id],
        )
        .map_err(postgres_memory_error)?;
    if let Some(row) = client
        .query_opt(
            "SELECT payload FROM memory_source_fences WHERE source_ref=$1",
            &[&format!("memory:{id}")],
        )
        .map_err(postgres_memory_error)?
    {
        let fence: MemorySourceFence =
            serde_json::from_value(row.try_get(0).map_err(postgres_memory_error)?)
                .map_err(json_memory_error)?;
        if fence.retired || fence.scope_key != entry.scope.scope_key() {
            return Err(MemoryError::Store(
                "retired or foreign Memory source write".into(),
            ));
        }
    }
    let history: Option<String> = client
        .query_opt(
            "SELECT value FROM memory_kv WHERE key=$1",
            &[&format!("memory_lifecycle:{id}")],
        )
        .map_err(postgres_memory_error)?
        .map(|row| row.try_get(0))
        .transpose()
        .map_err(postgres_memory_error)?;
    if memory::store::lifecycle_retired(entry.id, history.as_deref())? {
        return Err(MemoryError::Store(
            "legacy retired Memory source write".into(),
        ));
    }
    let layer = enum_label(&entry.layer)?;
    let category = enum_label(&entry.category)?;
    let priority = enum_label(&entry.priority)?;
    let source = enum_label(&entry.source)?;
    let scope = entry.scope.scope_key();
    let created_at = entry.created_at.to_rfc3339();
    let updated_at = entry.updated_at.to_rfc3339();
    let authority_fingerprint = same_memory_key(entry);
    let payload = serde_json::to_value(entry).map_err(json_memory_error)?;
    let sql = if upsert {
        "INSERT INTO memory_entries(
            id,layer,category,priority,source,title,content,scope_key,session_id,
            source_agent,created_at,updated_at,payload,authority_fingerprint
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
         ON CONFLICT(id) DO UPDATE SET layer=EXCLUDED.layer,category=EXCLUDED.category,
            priority=EXCLUDED.priority,source=EXCLUDED.source,title=EXCLUDED.title,
            content=EXCLUDED.content,scope_key=EXCLUDED.scope_key,
            session_id=EXCLUDED.session_id,source_agent=EXCLUDED.source_agent,
            created_at=EXCLUDED.created_at,updated_at=EXCLUDED.updated_at,payload=EXCLUDED.payload,
            authority_fingerprint=EXCLUDED.authority_fingerprint"
    } else {
        "UPDATE memory_entries SET layer=$2,category=$3,priority=$4,source=$5,title=$6,
            content=$7,scope_key=$8,session_id=$9,source_agent=$10,created_at=$11,
            updated_at=$12,payload=$13,authority_fingerprint=$14 WHERE id=$1"
    };
    let affected = client
        .execute(
            sql,
            &[
                &id,
                &layer,
                &category,
                &priority,
                &source,
                &entry.title,
                &entry.content,
                &scope,
                &entry.session_id,
                &entry.source_agent,
                &created_at,
                &updated_at,
                &payload,
                &authority_fingerprint,
            ],
        )
        .map_err(postgres_memory_error)?;
    if affected != 1 {
        return Err(MemoryError::NotFound(id));
    }
    if let Some(verbatim) = memory::store::entry_verbatim(entry) {
        client.execute("INSERT INTO memory_verbatim(id,content,source,layer,timestamp) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET content=EXCLUDED.content,source=EXCLUDED.source,layer=EXCLUDED.layer,timestamp=EXCLUDED.timestamp", &[&verbatim.id,&verbatim.content,&verbatim.source,&verbatim.layer,&verbatim.timestamp]).map_err(postgres_memory_error)?;
    } else {
        client
            .execute(
                "DELETE FROM memory_verbatim WHERE id=$1",
                &[&entry.id.to_string()],
            )
            .map_err(postgres_memory_error)?;
    }
    Ok(())
}

fn enum_label<T: Serialize>(value: &T) -> MemoryResult<String> {
    match serde_json::to_value(value).map_err(json_memory_error)? {
        serde_json::Value::String(label) => Ok(label),
        _ => Err(MemoryError::Store(
            "memory enum did not serialize as a string".to_string(),
        )),
    }
}

fn nonnegative_u64(row: &Row, index: usize) -> MemoryResult<u64> {
    let value = row
        .try_get::<_, i64>(index)
        .map_err(postgres_memory_error)?;
    u64::try_from(value).map_err(|_| {
        MemoryError::Store(format!(
            "memory aggregate column {index} returned a negative value"
        ))
    })
}

fn row_to_maintenance_candidate(row: &Row) -> MemoryResult<MaintenanceCandidate> {
    let mut candidate = serde_json::from_value::<MaintenanceCandidate>(
        row.try_get(0).map_err(postgres_memory_error)?,
    )
    .map_err(json_memory_error)?;
    let status = row.try_get::<_, String>(1).map_err(postgres_memory_error)?;
    candidate.status = MaintenanceCandidateStatus::parse(&status).ok_or_else(|| {
        MemoryError::Store(format!("unknown maintenance candidate status `{status}`"))
    })?;
    let created_at = row.try_get::<_, String>(2).map_err(postgres_memory_error)?;
    candidate.created_at = chrono::DateTime::parse_from_rfc3339(&created_at)
        .map_err(|error| MemoryError::Store(format!("invalid maintenance created_at: {error}")))?
        .with_timezone(&chrono::Utc);
    let updated_at = row.try_get::<_, String>(3).map_err(postgres_memory_error)?;
    candidate.updated_at = chrono::DateTime::parse_from_rfc3339(&updated_at)
        .map_err(|error| MemoryError::Store(format!("invalid maintenance updated_at: {error}")))?
        .with_timezone(&chrono::Utc);
    Ok(candidate)
}

fn knowledge_label<T: Serialize>(value: &T) -> Result<String, KnowledgeStoreError> {
    match serde_json::to_value(value).map_err(json_knowledge_error)? {
        serde_json::Value::String(label) => Ok(label),
        _ => Err(KnowledgeStoreError::Backend(
            "knowledge enum did not serialize as a string".to_string(),
        )),
    }
}

fn entry_from_json(payload: serde_json::Value) -> MemoryResult<MemoryEntry> {
    serde_json::from_value(payload).map_err(json_memory_error)
}

fn row_to_entry(row: &Row) -> MemoryResult<MemoryEntry> {
    entry_from_json(row.try_get(0).map_err(postgres_memory_error)?)
}

fn row_to_symbol(row: &Row) -> MemoryResult<CodeSymbol> {
    let kind: String = row.try_get(2).map_err(postgres_memory_error)?;
    let line: i64 = row.try_get(4).map_err(postgres_memory_error)?;
    Ok(CodeSymbol {
        id: row.try_get(0).map_err(postgres_memory_error)?,
        name: row.try_get(1).map_err(postgres_memory_error)?,
        kind: SymbolKind::from_str(&kind).ok_or_else(|| {
            MemoryError::Store(format!("unknown PostgreSQL code symbol kind `{kind}`"))
        })?,
        file_path: row.try_get(3).map_err(postgres_memory_error)?,
        line: usize::try_from(line)
            .map_err(|_| MemoryError::Store("code symbol line overflow".to_string()))?,
        signature: row.try_get(5).map_err(postgres_memory_error)?,
        doc: row.try_get(6).map_err(postgres_memory_error)?,
    })
}

fn row_to_edge(row: &Row) -> MemoryResult<SymbolEdge> {
    let edge_type: String = row.try_get(2).map_err(postgres_memory_error)?;
    let edge_type = match edge_type.as_str() {
        "calls" => SymbolEdgeType::Calls,
        "imports" => SymbolEdgeType::Imports,
        "extends" => SymbolEdgeType::Extends,
        "implements" => SymbolEdgeType::Implements,
        _ => {
            return Err(MemoryError::Store(format!(
                "unknown PostgreSQL code edge type `{edge_type}`"
            )))
        }
    };
    Ok(SymbolEdge {
        source_id: row.try_get(0).map_err(postgres_memory_error)?,
        target_id: row.try_get(1).map_err(postgres_memory_error)?,
        edge_type,
        file_path: row.try_get(3).map_err(postgres_memory_error)?,
    })
}

fn row_to_verbatim(row: &Row) -> MemoryResult<VerbatimEntry> {
    Ok(VerbatimEntry {
        id: row.try_get(0).map_err(postgres_memory_error)?,
        content: row.try_get(1).map_err(postgres_memory_error)?,
        source: row.try_get(2).map_err(postgres_memory_error)?,
        layer: row.try_get(3).map_err(postgres_memory_error)?,
        timestamp: row.try_get(4).map_err(postgres_memory_error)?,
    })
}

fn row_to_symbol_reference(row: &Row) -> MemoryResult<SymbolMemoryReference> {
    let memory_id: String = row.try_get(1).map_err(postgres_memory_error)?;
    Ok(SymbolMemoryReference {
        symbol_id: row.try_get(0).map_err(postgres_memory_error)?,
        memory_id: uuid::Uuid::parse_str(&memory_id).map_err(|error| {
            MemoryError::Store(format!("invalid symbol reference memory id: {error}"))
        })?,
        turn_index: row.try_get(2).map_err(postgres_memory_error)?,
        reference_type: row.try_get(3).map_err(postgres_memory_error)?,
        timestamp: row.try_get(4).map_err(postgres_memory_error)?,
    })
}

fn memory_meta(entry: &MemoryEntry) -> MemoryMeta {
    MemoryMeta {
        id: entry.id,
        layer: entry.layer,
        category: entry.category,
        priority: entry.priority,
        title: entry.title.clone(),
        tags: entry.tags.clone(),
        confidence: entry.confidence,
        access_count: entry.access_count,
        staleness: entry.staleness,
        created_at: entry.created_at,
        updated_at: entry.updated_at,
        scope: Some(entry.scope.scope_key()),
    }
}

fn matched_keywords(query: &str, entries: &[MemoryEntry]) -> Vec<(String, i64)> {
    let mut counts = std::collections::BTreeMap::<String, i64>::new();
    for raw in query.split_whitespace() {
        let keyword = raw
            .trim_matches(|character: char| !character.is_alphanumeric())
            .to_lowercase();
        if keyword.is_empty() {
            continue;
        }
        let count = entries
            .iter()
            .map(|entry| {
                format!("{} {}", entry.title, entry.content)
                    .to_lowercase()
                    .matches(&keyword)
                    .count() as i64
            })
            .sum();
        counts.insert(keyword, count);
    }
    let mut keywords = counts.into_iter().collect::<Vec<_>>();
    keywords.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    keywords.truncate(20);
    keywords
}

fn limit_i64(limit: usize) -> MemoryResult<i64> {
    i64::try_from(limit).map_err(|_| MemoryError::Store("query limit overflow".to_string()))
}

fn storage_memory_error(error: storage::StorageError) -> MemoryError {
    MemoryError::Store(error.to_string())
}

fn run_driver_sync<T, E>(
    operation: impl FnOnce() -> Result<T, E> + Send,
    panic_error: impl FnOnce() -> E,
) -> Result<T, E>
where
    T: Send,
    E: Send,
{
    if tokio::runtime::Handle::try_current().is_err() {
        return operation();
    }
    std::thread::scope(|scope| match scope.spawn(operation).join() {
        Ok(result) => result,
        Err(_) => Err(panic_error()),
    })
}

async fn run_memory_blocking<T>(
    operation: impl FnOnce() -> MemoryResult<T> + Send + 'static,
) -> MemoryResult<T>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| MemoryError::Store(format!("PostgreSQL memory worker failed: {error}")))?
}

fn postgres_memory_error(error: postgres::Error) -> MemoryError {
    // Preserve safe structural rejection evidence, without echoing SQL values,
    // payloads or driver detail that can contain user content.
    if let Some(database) = error.as_db_error() {
        let constraint = database
            .constraint()
            .filter(|name| {
                name.len() <= 128 && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            })
            .unwrap_or("none");
        MemoryError::Store(format!(
            "PostgreSQL Memory refusal sqlstate={} constraint={constraint}",
            database.code().code()
        ))
    } else {
        MemoryError::Store(error.to_string())
    }
}

fn json_memory_error(error: serde_json::Error) -> MemoryError {
    MemoryError::Store(error.to_string())
}

fn storage_knowledge_error(error: storage::StorageError) -> KnowledgeStoreError {
    KnowledgeStoreError::Backend(error.to_string())
}

fn postgres_knowledge_error(error: postgres::Error) -> KnowledgeStoreError {
    KnowledgeStoreError::Backend(error.to_string())
}

fn json_knowledge_error(error: serde_json::Error) -> KnowledgeStoreError {
    KnowledgeStoreError::Backend(error.to_string())
}

#[allow(clippy::too_many_arguments)]
fn upsert_knowledge_json<T: Serialize>(
    transaction: &mut PostgresTransaction<'_>,
    table: &str,
    _key_column: &str,
    id: &str,
    namespace: &str,
    state: Option<String>,
    updated_at: &str,
    value: &T,
) -> Result<(), KnowledgeStoreError> {
    let payload = serde_json::to_value(value).map_err(json_knowledge_error)?;
    match table {
        "knowledge_corpus" => transaction.execute(
            "INSERT INTO knowledge_corpus(corpus_id,namespace_key,updated_at,payload)
                 VALUES($1,$2,$3,$4) ON CONFLICT(corpus_id) DO UPDATE SET
                    namespace_key=EXCLUDED.namespace_key,updated_at=EXCLUDED.updated_at,
                    payload=EXCLUDED.payload",
            &[&id, &namespace, &updated_at, &payload],
        ),
        "knowledge_pack" => {
            let state = state.ok_or_else(|| {
                KnowledgeStoreError::Backend("knowledge pack state is required".to_string())
            })?;
            transaction.execute(
                "INSERT INTO knowledge_pack(pack_id,namespace_key,state,updated_at,payload)
                 VALUES($1,$2,$3,$4,$5) ON CONFLICT(pack_id) DO UPDATE SET
                    namespace_key=EXCLUDED.namespace_key,state=EXCLUDED.state,
                    updated_at=EXCLUDED.updated_at,payload=EXCLUDED.payload",
                &[&id, &namespace, &state, &updated_at, &payload],
            )
        }
        _ => {
            return Err(KnowledgeStoreError::Backend(format!(
                "unsupported knowledge table `{table}`"
            )))
        }
    }
    .map_err(postgres_knowledge_error)?;
    Ok(())
}

fn check_knowledge_frame_budget(row: &Row) -> Result<(), KnowledgeStoreError> {
    let count: i64 = row.try_get(0).map_err(postgres_knowledge_error)?;
    let bytes: i64 = row.try_get(1).map_err(postgres_knowledge_error)?;
    if count < 0
        || bytes < 0
        || count as u64 > memory::knowledge::KNOWLEDGE_READ_MAX_ROWS as u64
        || bytes as u64 > memory::knowledge::KNOWLEDGE_READ_MAX_BYTES as u64
    {
        return Err(KnowledgeStoreError::ReadLimit);
    }
    Ok(())
}

fn apply_knowledge_maintenance(
    connection: &mut PostgresTransaction<'_>,
    packs: &[harness_contract::knowledge::KnowledgePack],
    conflicts: &[harness_contract::knowledge::KnowledgeConflictRecord],
) -> Result<(), KnowledgeStoreError> {
    for pack in packs
        .iter()
        .filter(|pack| memory::knowledge::knowledge_state_is_actionable(pack.state))
    {
        // Include the old source set: removing a retired reference from a
        // replacement payload is not permission to reactivate its original pack.
        if connection.query_opt("SELECT 1 FROM knowledge_corpus c JOIN knowledge_memory_retirement_receipts r ON r.source_ref=c.payload->'source_ref'->>'id' WHERE c.corpus_id=ANY($1::text[]) OR EXISTS(SELECT 1 FROM knowledge_pack p WHERE p.pack_id=$2 AND p.payload->'source_corpus_refs' ? c.corpus_id) LIMIT 1", &[&pack.source_corpus_refs,&pack.pack_id]).map_err(postgres_knowledge_error)?.is_some() {
            return Err(KnowledgeStoreError::StaleRead);
        }
    }
    for pack in packs {
        upsert_knowledge_json(
            connection,
            "knowledge_pack",
            "pack_id",
            &pack.pack_id,
            &pack.namespace.key(),
            Some(knowledge_label(&pack.state)?),
            &pack.updated_at.to_rfc3339(),
            pack,
        )?;
    }
    for conflict in conflicts {
        connection.execute("INSERT INTO knowledge_conflict(conflict_id,pack_id,state,detected_at,payload) VALUES($1,$2,$3,$4,$5) ON CONFLICT(conflict_id) DO UPDATE SET pack_id=EXCLUDED.pack_id,state=EXCLUDED.state,detected_at=EXCLUDED.detected_at,payload=EXCLUDED.payload", &[&conflict.conflict_id,&conflict.pack_id,&knowledge_label(&conflict.state)?,&conflict.detected_at.to_rfc3339(),&serde_json::to_value(conflict).map_err(json_knowledge_error)?]).map_err(postgres_knowledge_error)?;
    }
    Ok(())
}

/// Reserve only the exact source dependency frame before transferring JSON.
/// Caller holds the original maintenance write transaction, so these repeated
/// selections cannot observe different source cuts or phantom dependents.
fn bounded_knowledge_source_snapshot(
    connection: &mut impl PostgresClient,
    source_ref: &str,
) -> Result<KnowledgeSnapshot, KnowledgeStoreError> {
    const FRAME: &str = "WITH source_corpus AS (SELECT corpus_id,payload FROM knowledge_corpus WHERE payload->'source_ref'->>'id'=$1), source_packs AS (SELECT p.pack_id,p.payload FROM knowledge_pack p WHERE EXISTS(SELECT 1 FROM source_corpus c WHERE p.payload->'source_corpus_refs' ? c.corpus_id)), frame AS (SELECT 'corpus'::text AS kind,payload FROM source_corpus UNION ALL SELECT 'pack',payload FROM source_packs UNION ALL SELECT 'conflict',payload FROM knowledge_conflict WHERE pack_id IN (SELECT pack_id FROM source_packs))";
    let row = connection.query_one(&format!("{FRAME} SELECT count(*),COALESCE(sum(octet_length(payload::text)),0)::bigint FROM frame"), &[&source_ref]).map_err(postgres_knowledge_error)?;
    check_knowledge_frame_budget(&row)?;
    let rows = connection.query(&format!("{FRAME} SELECT kind,payload FROM frame ORDER BY kind,payload->>'pack_id',payload->>'conflict_id',payload->>'corpus_id'"), &[&source_ref]).map_err(postgres_knowledge_error)?;
    let mut snapshot = KnowledgeSnapshot {
        corpus: Vec::new(),
        packs: Vec::new(),
        canon: Vec::new(),
        conflicts: Vec::new(),
        chunks: Vec::new(),
        usage: Vec::new(),
    };
    for row in rows {
        let kind: &str = row.try_get(0).map_err(postgres_knowledge_error)?;
        let value: serde_json::Value = row.try_get(1).map_err(postgres_knowledge_error)?;
        match kind {
            "corpus" => snapshot
                .corpus
                .push(serde_json::from_value(value).map_err(json_knowledge_error)?),
            "pack" => snapshot
                .packs
                .push(serde_json::from_value(value).map_err(json_knowledge_error)?),
            "conflict" => snapshot
                .conflicts
                .push(serde_json::from_value(value).map_err(json_knowledge_error)?),
            _ => return Err(KnowledgeStoreError::StaleRead),
        }
    }
    Ok(snapshot)
}

fn bounded_knowledge_snapshot(
    connection: &mut impl PostgresClient,
    include_history: bool,
) -> Result<KnowledgeSnapshot, KnowledgeStoreError> {
    // Count/size only: no JSON payload crosses the driver boundary until the
    // entire consistent read frame is reserved. Overflow is never truncation.
    let tables = if include_history {
        "SELECT payload FROM knowledge_corpus UNION ALL SELECT payload FROM knowledge_pack UNION ALL SELECT payload FROM knowledge_canon UNION ALL SELECT payload FROM knowledge_conflict UNION ALL SELECT payload FROM knowledge_chunk UNION ALL SELECT payload FROM knowledge_usage"
    } else {
        "SELECT payload FROM knowledge_corpus UNION ALL SELECT payload FROM knowledge_pack UNION ALL SELECT payload FROM knowledge_canon UNION ALL SELECT payload FROM knowledge_conflict"
    };
    let sql = format!("SELECT count(*), COALESCE(sum(octet_length(payload::text)),0)::bigint FROM ({tables}) AS frame");
    let row = connection
        .query_one(&sql, &[])
        .map_err(postgres_knowledge_error)?;
    check_knowledge_frame_budget(&row)?;
    Ok(KnowledgeSnapshot {
        corpus: knowledge_rows(connection, "knowledge_corpus", "corpus_id")?,
        packs: knowledge_rows(connection, "knowledge_pack", "pack_id")?,
        canon: knowledge_rows(connection, "knowledge_canon", "canon_id")?,
        conflicts: knowledge_rows(connection, "knowledge_conflict", "conflict_id")?,
        chunks: if include_history {
            knowledge_rows(connection, "knowledge_chunk", "chunk_id")?
        } else {
            Vec::new()
        },
        usage: if include_history {
            knowledge_rows(connection, "knowledge_usage", "signal_id")?
        } else {
            Vec::new()
        },
    })
}

fn knowledge_rows<T: DeserializeOwned>(
    connection: &mut impl PostgresClient,
    table: &str,
    _key_column: &str,
) -> Result<Vec<T>, KnowledgeStoreError> {
    let sql = match table {
        "knowledge_corpus" => "SELECT payload FROM knowledge_corpus ORDER BY corpus_id",
        "knowledge_pack" => "SELECT payload FROM knowledge_pack ORDER BY pack_id",
        "knowledge_canon" => "SELECT payload FROM knowledge_canon ORDER BY canon_id",
        "knowledge_conflict" => "SELECT payload FROM knowledge_conflict ORDER BY conflict_id",
        "knowledge_chunk" => "SELECT payload FROM knowledge_chunk ORDER BY chunk_id",
        "knowledge_usage" => "SELECT payload FROM knowledge_usage ORDER BY signal_id",
        _ => {
            return Err(KnowledgeStoreError::Backend(format!(
                "unsupported knowledge table `{table}`"
            )))
        }
    };
    connection
        .query(sql, &[])
        .map_err(postgres_knowledge_error)?
        .iter()
        .map(|row| {
            let payload: serde_json::Value = row.try_get(0).map_err(postgres_knowledge_error)?;
            serde_json::from_value(payload).map_err(json_knowledge_error)
        })
        .collect()
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

impl MemorySourceFenceReader for PostgresMemoryStore {
    fn read_source_fence(&self, source_ref: &str) -> MemoryResult<Option<MemorySourceFence>> {
        let id = source_ref
            .strip_prefix("memory:")
            .and_then(|id| uuid::Uuid::parse_str(id).ok())
            .ok_or_else(|| MemoryError::Store("invalid Memory source identity".into()))?;
        if format!("memory:{id}") != source_ref {
            return Err(MemoryError::Store(
                "noncanonical Memory source identity".into(),
            ));
        }
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_memory_error)?;
        // Single snapshot for the persisted fence, original body and legacy history.
        let row=connection.query_opt("SELECT f.payload,e.scope_key,k.value,f.scope_key,f.delete_epoch FROM (SELECT $1::TEXT AS source_ref) s LEFT JOIN memory_source_fences f ON f.source_ref=s.source_ref LEFT JOIN memory_entries e ON e.id=$2 LEFT JOIN memory_kv k ON k.key='memory_lifecycle:'||e.id", &[&source_ref,&id.to_string()]).map_err(postgres_memory_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        if let Some(payload) = row
            .try_get::<_, Option<serde_json::Value>>(0)
            .map_err(postgres_memory_error)?
        {
            let fence: MemorySourceFence =
                serde_json::from_value(payload).map_err(json_memory_error)?;
            let scope: String = row.try_get(3).map_err(postgres_memory_error)?;
            let epoch: i64 = row.try_get(4).map_err(postgres_memory_error)?;
            if fence.source_ref != source_ref
                || fence.scope_key != scope
                || i64::try_from(fence.delete_epoch).ok() != Some(epoch)
            {
                return Err(MemoryError::Store(
                    "Memory source fence identity mismatch".into(),
                ));
            }
            if let Some(body_scope) = row
                .try_get::<_, Option<String>>(1)
                .map_err(postgres_memory_error)?
            {
                if body_scope != scope {
                    return Err(MemoryError::Store(
                        "Memory source/body scope mismatch".into(),
                    ));
                }
            }
            if !fence.retired
                && row
                    .try_get::<_, Option<String>>(1)
                    .map_err(postgres_memory_error)?
                    .is_none()
            {
                return Err(MemoryError::Store("live Memory source body missing".into()));
            }
            return Ok(Some(fence));
        }
        let Some(scope_key) = row
            .try_get::<_, Option<String>>(1)
            .map_err(postgres_memory_error)?
        else {
            return Ok(None);
        };
        let history: Option<String> = row.try_get(2).map_err(postgres_memory_error)?;
        Ok(Some(MemorySourceFence {
            source_ref: source_ref.into(),
            scope_key,
            delete_epoch: 0,
            retired: memory::store::lifecycle_retired(id, history.as_deref())?,
        }))
    }
}
