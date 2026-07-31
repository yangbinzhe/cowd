//! PostgreSQL owners for Cowd Memory and Knowledge.
//!
//! Both adapters receive the host-owned bounded [`storage::PostgresExecutor`].
//! They intentionally do not accept a path or a database URL and never call a
//! SQLite adapter as a fallback.

use harness_contract::knowledge::{KnowledgeConflictRecord, KnowledgePack, KnowledgeUsageSignal};
use memory::{
    code_indexer::{CodeSymbol, SymbolEdge, SymbolEdgeType, SymbolKind},
    entity::{Entity, Triple},
    knowledge::{
        KnowledgeIngestionReceipt, KnowledgeSnapshot, KnowledgeStore, KnowledgeStoreError,
    },
    project_scope::MemoryScope,
    store::{
        FtsSearchOptions, FtsSearchResult, LegacyScopeMigrationReport, MemoryKeyValue, MemoryStore,
        MemoryStoreCapabilities, Result as MemoryResult, SymbolMemoryReference, VerbatimEntry,
    },
    MemoryCategory, MemoryEntry, MemoryError, MemoryId, MemoryLayer, MemoryMeta,
};
use postgres::{types::ToSql, Row};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use storage::{
    PostgresClient, PostgresConnectionConfig, PostgresExecutor, PostgresMigrationSpec,
    PostgresTransaction, SecretRefResolver,
};

const MEMORY_DOMAIN: &str = "memory";
const KNOWLEDGE_DOMAIN: &str = "knowledge";
const MEMORY_SNAPSHOT_VERSION: u32 = 1;
const KNOWLEDGE_SNAPSHOT_VERSION: u32 = 1;

/// Complete portable Memory truth used only during a quiesced backend cutover.
/// FTS/ANN physical indexes are excluded because they are rebuilt from these
/// durable rows; embedded vectors inside `MemoryEntry` remain included.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryMigrationSnapshot {
    pub schema_version: u32,
    pub entries: Vec<MemoryEntry>,
    pub legacy_scope_reports: Vec<LegacyScopeMigrationReport>,
    pub entities: Vec<Entity>,
    pub triples: Vec<Triple>,
    pub verbatim: Vec<VerbatimEntry>,
    pub symbols: Vec<CodeSymbol>,
    pub edges: Vec<SymbolEdge>,
    pub symbol_memory_references: Vec<SymbolMemoryReference>,
    pub key_values: Vec<MemoryKeyValue>,
}

impl MemoryMigrationSnapshot {
    pub fn canonical_digest(&self) -> MemoryResult<String> {
        let bytes = serde_json::to_vec(self).map_err(json_memory_error)?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
            && self.legacy_scope_reports.is_empty()
            && self.entities.is_empty()
            && self.triples.is_empty()
            && self.verbatim.is_empty()
            && self.symbols.is_empty()
            && self.edges.is_empty()
            && self.symbol_memory_references.is_empty()
            && self.key_values.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryMigrationManifest {
    pub domain: String,
    pub schema_version: u32,
    pub source_digest: String,
    pub target_digest: String,
    pub entry_count: usize,
    pub entity_count: usize,
    pub triple_count: usize,
    pub verbatim_count: usize,
    pub symbol_count: usize,
    pub edge_count: usize,
    pub symbol_reference_count: usize,
    pub key_value_count: usize,
}

/// Portable Knowledge state. The wrapper carries a cutover schema marker while
/// the domain-owned `KnowledgeSnapshot` remains the canonical DTO collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeMigrationSnapshot {
    pub schema_version: u32,
    pub state: KnowledgeSnapshot,
}

impl KnowledgeMigrationSnapshot {
    pub fn canonical_digest(&self) -> Result<String, KnowledgeStoreError> {
        let bytes = serde_json::to_vec(self).map_err(json_knowledge_error)?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }

    fn is_empty(&self) -> bool {
        self.state.corpus.is_empty()
            && self.state.packs.is_empty()
            && self.state.canon.is_empty()
            && self.state.conflicts.is_empty()
            && self.state.chunks.is_empty()
            && self.state.usage.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeMigrationManifest {
    pub domain: String,
    pub schema_version: u32,
    pub source_digest: String,
    pub target_digest: String,
    pub corpus_count: usize,
    pub pack_count: usize,
    pub canon_count: usize,
    pub conflict_count: usize,
    pub chunk_count: usize,
    pub usage_count: usize,
}

/// Export all Memory durable truth through the backend-neutral port and sort it
/// canonically so SQLite and PostgreSQL produce identical digests.
pub async fn export_memory_snapshot(
    source: &dyn MemoryStore,
) -> MemoryResult<MemoryMigrationSnapshot> {
    let mut snapshot = MemoryMigrationSnapshot {
        schema_version: MEMORY_SNAPSHOT_VERSION,
        entries: source.list_all().await?,
        legacy_scope_reports: source.legacy_scope_migration_reports().await?,
        entities: source.load_entities().await?,
        triples: source.load_triples().await?,
        verbatim: source.list_verbatim_entries().await?,
        symbols: source.list_all_symbols().await?,
        edges: source.list_all_edges().await?,
        symbol_memory_references: source.list_symbol_memory_references().await?,
        key_values: source.list_key_values().await?,
    };
    sort_memory_snapshot(&mut snapshot);
    Ok(snapshot)
}

pub fn export_knowledge_snapshot(
    source: &dyn KnowledgeStore,
) -> Result<KnowledgeMigrationSnapshot, KnowledgeStoreError> {
    let mut state = source.snapshot()?;
    sort_knowledge_snapshot(&mut state);
    Ok(KnowledgeMigrationSnapshot {
        schema_version: KNOWLEDGE_SNAPSHOT_VERSION,
        state,
    })
}

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
}];

#[derive(Clone, Debug)]
pub struct PostgresMemoryStore {
    executor: PostgresExecutor,
}

impl PostgresMemoryStore {
    pub fn new(executor: PostgresExecutor) -> MemoryResult<Self> {
        run_driver_sync(
            move || {
                executor
                    .apply_migrations(MEMORY_DOMAIN, MEMORY_MIGRATIONS)
                    .map_err(storage_memory_error)?;
                Ok(Self { executor })
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
                Ok(Self { executor })
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
            .checkout_runtime()
            .map_err(storage_memory_error)?;
        connection
            .query(sql, params)
            .map_err(postgres_memory_error)?
            .iter()
            .map(row_to_entry)
            .collect()
    }

    fn symbols(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> MemoryResult<Vec<CodeSymbol>> {
        let mut connection = self
            .executor
            .checkout_runtime()
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
            .checkout_runtime()
            .map_err(storage_memory_error)?;
        write_entry(&mut connection, entry, true)
    }

    fn update_entry(&self, entry: &MemoryEntry) -> MemoryResult<()> {
        let mut connection = self
            .executor
            .checkout_runtime()
            .map_err(storage_memory_error)?;
        write_entry(&mut connection, entry, false)
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
            .checkout_runtime()
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
        table: &str,
        values: &[T],
        id: impl Fn(&serde_json::Value) -> Option<String>,
    ) -> MemoryResult<()> {
        let (delete_sql, insert_sql) = match table {
            "memory_entities" => (
                "DELETE FROM memory_entities",
                "INSERT INTO memory_entities(id,payload) VALUES($1,$2)",
            ),
            _ => {
                return Err(MemoryError::Store(format!(
                    "unsupported memory JSON table `{table}`"
                )))
            }
        };
        let mut connection = self
            .executor
            .checkout_runtime()
            .map_err(storage_memory_error)?;
        let mut transaction = connection.transaction().map_err(postgres_memory_error)?;
        transaction
            .execute(delete_sql, &[])
            .map_err(postgres_memory_error)?;
        for value in values {
            let payload = serde_json::to_value(value).map_err(json_memory_error)?;
            let id = id(&payload)
                .ok_or_else(|| MemoryError::Store("durable JSON row has no id".to_string()))?;
            transaction
                .execute(insert_sql, &[&id, &payload])
                .map_err(postgres_memory_error)?;
        }
        transaction.commit().map_err(postgres_memory_error)?;
        Ok(())
    }

    fn load_json_rows<T: DeserializeOwned>(&self, table: &str) -> MemoryResult<Vec<T>> {
        let sql = match table {
            "memory_entities" => "SELECT payload FROM memory_entities ORDER BY id",
            "memory_triples" => "SELECT payload FROM memory_triples ORDER BY id",
            _ => {
                return Err(MemoryError::Store(format!(
                    "unsupported memory JSON table `{table}`"
                )))
            }
        };
        let mut connection = self
            .executor
            .checkout_runtime()
            .map_err(storage_memory_error)?;
        connection
            .query(sql, &[])
            .map_err(postgres_memory_error)?
            .iter()
            .map(|row| {
                let payload: serde_json::Value = row.try_get(0).map_err(postgres_memory_error)?;
                serde_json::from_value(payload).map_err(json_memory_error)
            })
            .collect()
    }

    fn replace_triples(&self, triples: &[Triple]) -> MemoryResult<()> {
        let mut connection = self
            .executor
            .checkout_runtime()
            .map_err(storage_memory_error)?;
        let mut transaction = connection.transaction().map_err(postgres_memory_error)?;
        transaction
            .execute("DELETE FROM memory_triples", &[])
            .map_err(postgres_memory_error)?;
        for triple in triples {
            let payload = serde_json::to_value(triple).map_err(json_memory_error)?;
            transaction
                .execute(
                    "INSERT INTO memory_triples(id,subject_key,object_key,payload) VALUES($1,$2,$3,$4)",
                    &[&triple.id, &triple.subject_id, &triple.object_id, &payload],
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
            .checkout_runtime()
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

    pub fn migration_snapshot(&self) -> MemoryResult<MemoryMigrationSnapshot> {
        let mut connection = self
            .executor
            .checkout_runtime()
            .map_err(storage_memory_error)?;
        let entries = connection
            .query("SELECT payload FROM memory_entries ORDER BY id", &[])
            .map_err(postgres_memory_error)?
            .iter()
            .map(row_to_entry)
            .collect::<MemoryResult<Vec<_>>>()?;
        let legacy_scope_reports = connection
            .query(
                "SELECT memory_id,raw_scope,held_scope,reason,migrated_at
                 FROM memory_scope_migration_reports ORDER BY migrated_at,memory_id",
                &[],
            )
            .map_err(postgres_memory_error)?
            .iter()
            .map(row_to_legacy_scope_report)
            .collect::<MemoryResult<Vec<_>>>()?;
        let entities = json_rows_from_client(&mut connection, "memory_entities")?;
        let triples = json_rows_from_client(&mut connection, "memory_triples")?;
        let verbatim = connection
            .query(
                "SELECT id,content,source,layer,timestamp FROM memory_verbatim ORDER BY timestamp,id",
                &[],
            )
            .map_err(postgres_memory_error)?
            .iter()
            .map(row_to_verbatim)
            .collect::<MemoryResult<Vec<_>>>()?;
        let symbols = connection
            .query(
                "SELECT id,name,kind,file_path,line,signature,doc FROM memory_code_symbols ORDER BY id",
                &[],
            )
            .map_err(postgres_memory_error)?
            .iter()
            .map(row_to_symbol)
            .collect::<MemoryResult<Vec<_>>>()?;
        let edges = connection
            .query(
                "SELECT source_id,target_id,edge_type,file_path FROM memory_code_edges
                 ORDER BY source_id,target_id,edge_type,file_path,edge_id",
                &[],
            )
            .map_err(postgres_memory_error)?
            .iter()
            .map(row_to_edge)
            .collect::<MemoryResult<Vec<_>>>()?;
        let symbol_memory_references = connection
            .query(
                "SELECT symbol_id,memory_id,turn_index,reference_type,timestamp
                 FROM memory_symbol_references
                 ORDER BY timestamp,symbol_id,memory_id,reference_type,reference_id",
                &[],
            )
            .map_err(postgres_memory_error)?
            .iter()
            .map(row_to_symbol_reference)
            .collect::<MemoryResult<Vec<_>>>()?;
        let key_values = connection
            .query("SELECT key,value FROM memory_kv ORDER BY key", &[])
            .map_err(postgres_memory_error)?
            .iter()
            .map(|row| {
                Ok(MemoryKeyValue {
                    key: row.try_get(0).map_err(postgres_memory_error)?,
                    value: row.try_get(1).map_err(postgres_memory_error)?,
                })
            })
            .collect::<MemoryResult<Vec<_>>>()?;
        let mut snapshot = MemoryMigrationSnapshot {
            schema_version: MEMORY_SNAPSHOT_VERSION,
            entries,
            legacy_scope_reports,
            entities,
            triples,
            verbatim,
            symbols,
            edges,
            symbol_memory_references,
            key_values,
        };
        sort_memory_snapshot(&mut snapshot);
        Ok(snapshot)
    }

    pub fn import_memory_snapshot(
        &self,
        source: &MemoryMigrationSnapshot,
    ) -> MemoryResult<MemoryMigrationManifest> {
        if source.schema_version != MEMORY_SNAPSHOT_VERSION {
            return Err(MemoryError::Store(format!(
                "unsupported memory snapshot version {}",
                source.schema_version
            )));
        }
        let source_digest = source.canonical_digest()?;
        let existing = self.migration_snapshot()?;
        let existing_digest = existing.canonical_digest()?;
        if !existing.is_empty() {
            if existing_digest == source_digest {
                return Ok(memory_manifest(
                    source,
                    source_digest.clone(),
                    existing_digest,
                ));
            }
            return Err(MemoryError::Store(
                "memory migration target is neither empty nor identical".to_string(),
            ));
        }

        let mut connection = self
            .executor
            .checkout_runtime()
            .map_err(storage_memory_error)?;
        let mut transaction = connection.transaction().map_err(postgres_memory_error)?;
        for table in [
            "memory_symbol_references",
            "memory_code_edges",
            "memory_code_symbols",
            "memory_verbatim",
            "memory_triples",
            "memory_entities",
            "memory_scope_migration_reports",
            "memory_kv",
            "memory_entries",
        ] {
            transaction
                .execute(&format!("DELETE FROM {table}"), &[])
                .map_err(postgres_memory_error)?;
        }
        for entry in &source.entries {
            write_entry(&mut transaction, entry, true)?;
        }
        for report in &source.legacy_scope_reports {
            transaction.execute(
                "INSERT INTO memory_scope_migration_reports(memory_id,raw_scope,held_scope,reason,migrated_at)
                 VALUES($1,$2,$3,$4,$5)",
                &[&report.memory_id,&report.raw_scope,&report.held_scope,&report.reason,&report.migrated_at],
            ).map_err(postgres_memory_error)?;
        }
        for entity in &source.entities {
            let payload = serde_json::to_value(entity).map_err(json_memory_error)?;
            transaction
                .execute(
                    "INSERT INTO memory_entities(id,payload) VALUES($1,$2)",
                    &[&entity.id, &payload],
                )
                .map_err(postgres_memory_error)?;
        }
        for triple in &source.triples {
            let payload = serde_json::to_value(triple).map_err(json_memory_error)?;
            transaction.execute("INSERT INTO memory_triples(id,subject_key,object_key,payload) VALUES($1,$2,$3,$4)", &[&triple.id,&triple.subject_id,&triple.object_id,&payload]).map_err(postgres_memory_error)?;
        }
        for value in &source.verbatim {
            transaction.execute("INSERT INTO memory_verbatim(id,content,source,layer,timestamp) VALUES($1,$2,$3,$4,$5)", &[&value.id,&value.content,&value.source,&value.layer,&value.timestamp]).map_err(postgres_memory_error)?;
        }
        for symbol in &source.symbols {
            write_symbol(&mut transaction, symbol)?;
        }
        for edge in &source.edges {
            transaction.execute("INSERT INTO memory_code_edges(source_id,target_id,edge_type,file_path) VALUES($1,$2,$3,$4)", &[&edge.source_id,&edge.target_id,&edge.edge_type.as_str(),&edge.file_path]).map_err(postgres_memory_error)?;
        }
        for reference in &source.symbol_memory_references {
            transaction.execute("INSERT INTO memory_symbol_references(symbol_id,memory_id,turn_index,reference_type,timestamp) VALUES($1,$2,$3,$4,$5)", &[&reference.symbol_id,&reference.memory_id.to_string(),&reference.turn_index,&reference.reference_type,&reference.timestamp]).map_err(postgres_memory_error)?;
        }
        for value in &source.key_values {
            transaction
                .execute(
                    "INSERT INTO memory_kv(key,value) VALUES($1,$2)",
                    &[&value.key, &value.value],
                )
                .map_err(postgres_memory_error)?;
        }
        transaction.commit().map_err(postgres_memory_error)?;

        let target = self.migration_snapshot()?;
        let target_digest = target.canonical_digest()?;
        if target_digest != source_digest {
            return Err(MemoryError::Store(format!(
                "memory migration digest mismatch: source={source_digest} target={target_digest}"
            )));
        }
        Ok(memory_manifest(source, source_digest, target_digest))
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
                .checkout_runtime()
                .map_err(storage_memory_error)?;
            let row = connection
                .query_opt("SELECT payload FROM memory_entries WHERE id=$1", &[&id])
                .map_err(postgres_memory_error)?;
            let Some(row) = row else {
                return Ok(None);
            };
            let entry = entry_from_json(row.try_get(0).map_err(postgres_memory_error)?)?;
            let mut accessed = entry.clone();
            accessed.access_count = accessed.access_count.saturating_add(1);
            accessed.last_accessed_at = Some(chrono::Utc::now());
            drop(connection);
            store.update_entry(&accessed)?;
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
                .checkout_runtime()
                .map_err(storage_memory_error)?;
            connection
                .execute("DELETE FROM memory_entries WHERE id=$1", &[&id])
                .map_err(postgres_memory_error)?;
            Ok(())
        })
        .await
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
    async fn get_meta(&self, id: &MemoryId) -> MemoryResult<Option<MemoryMeta>> {
        let store = self.clone();
        let id = id.to_string();
        run_memory_blocking(move || {
            let mut connection = store
                .executor
                .checkout_runtime()
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
    async fn legacy_scope_migration_reports(
        &self,
    ) -> MemoryResult<Vec<LegacyScopeMigrationReport>> {
        let store = self.clone();
        run_memory_blocking(move || {
            let mut connection = store.executor.checkout_runtime().map_err(storage_memory_error)?;
            connection.query("SELECT memory_id,raw_scope,held_scope,reason,migrated_at FROM memory_scope_migration_reports ORDER BY memory_id", &[]).map_err(postgres_memory_error)?.iter().map(row_to_legacy_scope_report).collect()
        }).await
    }
    async fn save_entities(&self, entities: &[Entity]) -> MemoryResult<()> {
        let store = self.clone();
        let entities = entities.to_vec();
        run_memory_blocking(move || {
            store.replace_json_rows("memory_entities", &entities, |value| {
                value
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
        })
        .await
    }
    async fn load_entities(&self) -> MemoryResult<Vec<Entity>> {
        let store = self.clone();
        run_memory_blocking(move || store.load_json_rows("memory_entities")).await
    }
    async fn save_triples(&self, triples: &[Triple]) -> MemoryResult<()> {
        let store = self.clone();
        let triples = triples.to_vec();
        run_memory_blocking(move || store.replace_triples(&triples)).await
    }
    async fn load_triples(&self) -> MemoryResult<Vec<Triple>> {
        let store = self.clone();
        run_memory_blocking(move || store.load_json_rows("memory_triples")).await
    }
    async fn save_verbatim(
        &self,
        id: &str,
        content: &str,
        source: &str,
        layer: i32,
        timestamp: &str,
    ) -> MemoryResult<()> {
        let store = self.clone();
        let id = id.to_string();
        let content = content.to_string();
        let source = source.to_string();
        let timestamp = timestamp.to_string();
        run_memory_blocking(move || {
            let mut connection = store.executor.checkout_runtime().map_err(storage_memory_error)?;
            connection.execute("INSERT INTO memory_verbatim(id,content,source,layer,timestamp) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET content=EXCLUDED.content,source=EXCLUDED.source,layer=EXCLUDED.layer,timestamp=EXCLUDED.timestamp", &[&id,&content,&source,&layer,&timestamp]).map_err(postgres_memory_error)?;
            Ok(())
        }).await
    }
    async fn load_verbatim_by_id(&self, id: &str) -> MemoryResult<Option<VerbatimEntry>> {
        let store = self.clone();
        let id = id.to_string();
        run_memory_blocking(move || {
            let mut connection = store
                .executor
                .checkout_runtime()
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
            let mut connection = store.executor.checkout_runtime().map_err(storage_memory_error)?;
            connection.query("SELECT id,content,source,layer,timestamp FROM memory_verbatim WHERE content LIKE $1 ORDER BY timestamp DESC", &[&query]).map_err(postgres_memory_error)?.iter().map(row_to_verbatim).collect()
        }).await
    }
    async fn list_verbatim_entries(&self) -> MemoryResult<Vec<VerbatimEntry>> {
        let store = self.clone();
        run_memory_blocking(move || {
            let mut connection = store.executor.checkout_runtime().map_err(storage_memory_error)?;
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
            let mut connection = store.executor.checkout_runtime().map_err(storage_memory_error)?;
            connection.execute("INSERT INTO memory_code_edges(source_id,target_id,edge_type,file_path) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", &[&edge.source_id,&edge.target_id,&edge.edge_type.as_str(),&edge.file_path]).map_err(postgres_memory_error)?;
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
            let mut connection = store.executor.checkout_runtime().map_err(storage_memory_error)?;
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
            let mut connection = store.executor.checkout_runtime().map_err(storage_memory_error)?;
            connection.execute("INSERT INTO memory_symbol_references(symbol_id,memory_id,turn_index,reference_type,timestamp) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING", &[&symbol_id,&memory_id,&turn_index,&reference_type,&timestamp]).map_err(postgres_memory_error)?;
            Ok(())
        }).await
    }
    async fn find_memories_by_symbol(&self, symbol_name: &str) -> MemoryResult<Vec<MemoryId>> {
        let store = self.clone();
        let symbol_name = symbol_name.to_string();
        run_memory_blocking(move || {
            let pattern = format!("%{}%", symbol_name.to_ascii_lowercase());
            let mut connection = store.executor.checkout_runtime().map_err(storage_memory_error)?;
            connection.query("SELECT memory_id FROM memory_symbol_references WHERE lower(symbol_id) LIKE $1 OR symbol_id=$2 GROUP BY memory_id ORDER BY max(timestamp) DESC,memory_id", &[&pattern,&symbol_name]).map_err(postgres_memory_error)?.iter().map(|row| uuid::Uuid::parse_str(&row.try_get::<_,String>(0).map_err(postgres_memory_error)?).map_err(|error| MemoryError::Store(format!("invalid symbol reference memory id: {error}")))).collect()
        }).await
    }
    async fn list_symbol_memory_references(&self) -> MemoryResult<Vec<SymbolMemoryReference>> {
        let store = self.clone();
        run_memory_blocking(move || {
            let mut connection = store.executor.checkout_runtime().map_err(storage_memory_error)?;
            connection.query("SELECT symbol_id,memory_id,turn_index,reference_type,timestamp FROM memory_symbol_references ORDER BY timestamp,symbol_id,memory_id,reference_type", &[]).map_err(postgres_memory_error)?.iter().map(row_to_symbol_reference).collect()
        }).await
    }
    async fn kv_put(&self, key: &str, value: &str) -> MemoryResult<()> {
        let store = self.clone();
        let key = key.to_string();
        let value = value.to_string();
        run_memory_blocking(move || {
            let mut connection = store.executor.checkout_runtime().map_err(storage_memory_error)?;
            connection.execute("INSERT INTO memory_kv(key,value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value", &[&key,&value]).map_err(postgres_memory_error)?;
            Ok(())
        }).await
    }
    async fn kv_get(&self, key: &str) -> MemoryResult<Option<String>> {
        let store = self.clone();
        let key = key.to_string();
        run_memory_blocking(move || {
            let mut connection = store
                .executor
                .checkout_runtime()
                .map_err(storage_memory_error)?;
            connection
                .query_opt("SELECT value FROM memory_kv WHERE key=$1", &[&key])
                .map_err(postgres_memory_error)?
                .map(|row| row.try_get(0).map_err(postgres_memory_error))
                .transpose()
        })
        .await
    }
    async fn list_key_values(&self) -> MemoryResult<Vec<MemoryKeyValue>> {
        let store = self.clone();
        run_memory_blocking(move || {
            let mut connection = store
                .executor
                .checkout_runtime()
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

    pub fn migration_snapshot(&self) -> Result<KnowledgeMigrationSnapshot, KnowledgeStoreError> {
        export_knowledge_snapshot(self)
    }

    pub fn import_knowledge_snapshot(
        &self,
        source: &KnowledgeMigrationSnapshot,
    ) -> Result<KnowledgeMigrationManifest, KnowledgeStoreError> {
        if source.schema_version != KNOWLEDGE_SNAPSHOT_VERSION {
            return Err(KnowledgeStoreError::Backend(format!(
                "unsupported knowledge snapshot version {}",
                source.schema_version
            )));
        }
        let source_digest = source.canonical_digest()?;
        let existing = self.migration_snapshot()?;
        let existing_digest = existing.canonical_digest()?;
        if !existing.is_empty() {
            if existing_digest == source_digest {
                return Ok(knowledge_manifest(
                    source,
                    source_digest.clone(),
                    existing_digest,
                ));
            }
            return Err(KnowledgeStoreError::Backend(
                "knowledge migration target is neither empty nor identical".to_string(),
            ));
        }

        let mut connection = self
            .executor
            .checkout_runtime()
            .map_err(storage_knowledge_error)?;
        let mut transaction = connection.transaction().map_err(postgres_knowledge_error)?;
        for table in [
            "knowledge_usage",
            "knowledge_chunk",
            "knowledge_conflict",
            "knowledge_canon",
            "knowledge_pack",
            "knowledge_corpus",
        ] {
            transaction
                .execute(&format!("DELETE FROM {table}"), &[])
                .map_err(postgres_knowledge_error)?;
        }
        for corpus in &source.state.corpus {
            upsert_knowledge_json(
                &mut transaction,
                "knowledge_corpus",
                "corpus_id",
                &corpus.corpus_id,
                &corpus.namespace.key(),
                None,
                &corpus.updated_at.to_rfc3339(),
                corpus,
            )?;
        }
        for pack in &source.state.packs {
            upsert_knowledge_json(
                &mut transaction,
                "knowledge_pack",
                "pack_id",
                &pack.pack_id,
                &pack.namespace.key(),
                Some(knowledge_label(&pack.state)?),
                &pack.updated_at.to_rfc3339(),
                pack,
            )?;
        }
        for canon in &source.state.canon {
            let payload = serde_json::to_value(canon).map_err(json_knowledge_error)?;
            transaction.execute("INSERT INTO knowledge_canon(canon_id,pack_id,updated_at,payload) VALUES($1,$2,$3,$4)", &[&canon.canon_id,&canon.pack_id,&canon.updated_at.to_rfc3339(),&payload]).map_err(postgres_knowledge_error)?;
        }
        for conflict in &source.state.conflicts {
            let payload = serde_json::to_value(conflict).map_err(json_knowledge_error)?;
            transaction.execute("INSERT INTO knowledge_conflict(conflict_id,pack_id,state,detected_at,payload) VALUES($1,$2,$3,$4,$5)", &[&conflict.conflict_id,&conflict.pack_id,&knowledge_label(&conflict.state)?,&conflict.detected_at.to_rfc3339(),&payload]).map_err(postgres_knowledge_error)?;
        }
        for chunk in &source.state.chunks {
            let ordinal = i64::try_from(chunk.ordinal).map_err(|_| {
                KnowledgeStoreError::Backend("knowledge chunk ordinal overflow".to_string())
            })?;
            let payload = serde_json::to_value(chunk).map_err(json_knowledge_error)?;
            transaction.execute("INSERT INTO knowledge_chunk(chunk_id,corpus_id,ordinal,title,text,payload) VALUES($1,$2,$3,$4,$5,$6)", &[&chunk.chunk_id,&chunk.corpus_id,&ordinal,&chunk.title,&chunk.text,&payload]).map_err(postgres_knowledge_error)?;
        }
        for signal in &source.state.usage {
            let payload = serde_json::to_value(signal).map_err(json_knowledge_error)?;
            transaction.execute("INSERT INTO knowledge_usage(signal_id,pack_id,session_id,occurred_at,payload) VALUES($1,$2,$3,$4,$5)", &[&signal.signal_id,&signal.pack_id,&signal.session_id,&signal.occurred_at.to_rfc3339(),&payload]).map_err(postgres_knowledge_error)?;
        }
        transaction.commit().map_err(postgres_knowledge_error)?;

        let target = self.migration_snapshot()?;
        let target_digest = target.canonical_digest()?;
        if target_digest != source_digest {
            return Err(KnowledgeStoreError::Backend(format!(
                "knowledge migration digest mismatch: source={source_digest} target={target_digest}"
            )));
        }
        Ok(knowledge_manifest(source, source_digest, target_digest))
    }
}

impl KnowledgeStore for PostgresKnowledgeStore {
    fn save_receipt(&self, receipt: &KnowledgeIngestionReceipt) -> Result<(), KnowledgeStoreError> {
        let store = self.clone();
        let receipt = receipt.clone();
        run_driver_sync(
            move || {
                let mut connection = store
                    .executor
                    .checkout_runtime()
                    .map_err(storage_knowledge_error)?;
                let mut tx = connection.transaction().map_err(postgres_knowledge_error)?;
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
                Ok(())
            },
            || {
                KnowledgeStoreError::Backend(
                    "PostgreSQL knowledge write thread panicked".to_string(),
                )
            },
        )
    }

    fn save_pack(&self, pack: &KnowledgePack) -> Result<(), KnowledgeStoreError> {
        let store = self.clone();
        let pack = pack.clone();
        run_driver_sync(
            move || {
                let mut connection = store
                    .executor
                    .checkout_runtime()
                    .map_err(storage_knowledge_error)?;
                let mut tx = connection.transaction().map_err(postgres_knowledge_error)?;
                upsert_knowledge_json(
                    &mut tx,
                    "knowledge_pack",
                    "pack_id",
                    &pack.pack_id,
                    &pack.namespace.key(),
                    Some(knowledge_label(&pack.state)?),
                    &pack.updated_at.to_rfc3339(),
                    &pack,
                )?;
                tx.commit().map_err(postgres_knowledge_error)?;
                Ok(())
            },
            || {
                KnowledgeStoreError::Backend(
                    "PostgreSQL knowledge pack write thread panicked".to_string(),
                )
            },
        )
    }

    fn save_conflict(&self, conflict: &KnowledgeConflictRecord) -> Result<(), KnowledgeStoreError> {
        let store = self.clone();
        let conflict = conflict.clone();
        run_driver_sync(
            move || {
                let mut connection = store
                    .executor
                    .checkout_runtime()
                    .map_err(storage_knowledge_error)?;
                let mut tx = connection.transaction().map_err(postgres_knowledge_error)?;
                tx.execute(
                    "INSERT INTO knowledge_conflict(conflict_id,pack_id,state,detected_at,payload) VALUES($1,$2,$3,$4,$5) ON CONFLICT(conflict_id) DO UPDATE SET pack_id=EXCLUDED.pack_id,state=EXCLUDED.state,detected_at=EXCLUDED.detected_at,payload=EXCLUDED.payload",
                    &[
                        &conflict.conflict_id,
                        &conflict.pack_id,
                        &knowledge_label(&conflict.state)?,
                        &conflict.detected_at.to_rfc3339(),
                        &serde_json::to_value(&conflict).map_err(json_knowledge_error)?,
                    ],
                )
                .map_err(postgres_knowledge_error)?;
                tx.commit().map_err(postgres_knowledge_error)?;
                Ok(())
            },
            || {
                KnowledgeStoreError::Backend(
                    "PostgreSQL knowledge conflict write thread panicked".to_string(),
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
                    .checkout_runtime()
                    .map_err(storage_knowledge_error)?;
                connection.execute("INSERT INTO knowledge_usage(signal_id,pack_id,session_id,occurred_at,payload) VALUES($1,$2,$3,$4,$5) ON CONFLICT(signal_id) DO UPDATE SET pack_id=EXCLUDED.pack_id,session_id=EXCLUDED.session_id,occurred_at=EXCLUDED.occurred_at,payload=EXCLUDED.payload", &[&signal.signal_id,&signal.pack_id,&signal.session_id,&signal.occurred_at.to_rfc3339(),&serde_json::to_value(&signal).map_err(json_knowledge_error)?]).map_err(postgres_knowledge_error)?;
                Ok(())
            },
            || {
                KnowledgeStoreError::Backend(
                    "PostgreSQL knowledge usage thread panicked".to_string(),
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
                    .checkout_runtime()
                    .map_err(storage_knowledge_error)?;
                Ok(KnowledgeSnapshot {
                    corpus: knowledge_rows(&mut connection, "knowledge_corpus", "corpus_id")?,
                    packs: knowledge_rows(&mut connection, "knowledge_pack", "pack_id")?,
                    canon: knowledge_rows(&mut connection, "knowledge_canon", "canon_id")?,
                    conflicts: knowledge_rows(
                        &mut connection,
                        "knowledge_conflict",
                        "conflict_id",
                    )?,
                    chunks: knowledge_rows(&mut connection, "knowledge_chunk", "chunk_id")?,
                    usage: knowledge_rows(&mut connection, "knowledge_usage", "signal_id")?,
                })
            },
            || {
                KnowledgeStoreError::Backend(
                    "PostgreSQL knowledge read thread panicked".to_string(),
                )
            },
        )
    }
}

/// Perform a quiesced copy and prove the source did not change while the
/// target was populated. A mismatch returns no activation manifest.
pub async fn copy_quiesced_memory_store(
    source: &dyn MemoryStore,
    target: &PostgresMemoryStore,
) -> MemoryResult<MemoryMigrationManifest> {
    let before = export_memory_snapshot(source).await?;
    let before_digest = before.canonical_digest()?;
    let target = target.clone();
    let import_snapshot = before.clone();
    let manifest =
        run_memory_blocking(move || target.import_memory_snapshot(&import_snapshot)).await?;
    let after = export_memory_snapshot(source).await?;
    let after_digest = after.canonical_digest()?;
    if after_digest != before_digest {
        return Err(MemoryError::Store(format!(
            "memory source changed during quiesced copy: before={before_digest} after={after_digest}"
        )));
    }
    if manifest.source_digest != before_digest || manifest.target_digest != before_digest {
        return Err(MemoryError::Store(
            "memory cutover manifest digest invariant failed".to_string(),
        ));
    }
    Ok(manifest)
}

pub fn copy_quiesced_knowledge_store(
    source: &dyn KnowledgeStore,
    target: &PostgresKnowledgeStore,
) -> Result<KnowledgeMigrationManifest, KnowledgeStoreError> {
    let before = export_knowledge_snapshot(source)?;
    let before_digest = before.canonical_digest()?;
    let target = target.clone();
    let import_snapshot = before.clone();
    let manifest = run_driver_sync(
        move || target.import_knowledge_snapshot(&import_snapshot),
        || {
            KnowledgeStoreError::Backend(
                "PostgreSQL knowledge migration thread panicked".to_string(),
            )
        },
    )?;
    let after = export_knowledge_snapshot(source)?;
    let after_digest = after.canonical_digest()?;
    if after_digest != before_digest {
        return Err(KnowledgeStoreError::Backend(format!(
            "knowledge source changed during quiesced copy: before={before_digest} after={after_digest}"
        )));
    }
    if manifest.source_digest != before_digest || manifest.target_digest != before_digest {
        return Err(KnowledgeStoreError::Backend(
            "knowledge cutover manifest digest invariant failed".to_string(),
        ));
    }
    Ok(manifest)
}

fn sort_memory_snapshot(snapshot: &mut MemoryMigrationSnapshot) {
    snapshot.entries.sort_by_key(|entry| entry.id);
    snapshot.legacy_scope_reports.sort_by(|left, right| {
        (left.migrated_at.as_str(), left.memory_id.as_str())
            .cmp(&(right.migrated_at.as_str(), right.memory_id.as_str()))
    });
    snapshot
        .entities
        .sort_by(|left, right| left.id.cmp(&right.id));
    snapshot
        .triples
        .sort_by(|left, right| left.id.cmp(&right.id));
    snapshot.verbatim.sort_by(|left, right| {
        (left.timestamp.as_str(), left.id.as_str())
            .cmp(&(right.timestamp.as_str(), right.id.as_str()))
    });
    snapshot
        .symbols
        .sort_by(|left, right| left.id.cmp(&right.id));
    snapshot.edges.sort_by(|left, right| {
        (
            left.source_id.as_str(),
            left.target_id.as_str(),
            left.edge_type.as_str(),
            left.file_path.as_str(),
        )
            .cmp(&(
                right.source_id.as_str(),
                right.target_id.as_str(),
                right.edge_type.as_str(),
                right.file_path.as_str(),
            ))
    });
    snapshot.symbol_memory_references.sort_by(|left, right| {
        (
            left.timestamp,
            left.symbol_id.as_str(),
            left.memory_id,
            left.turn_index,
            left.reference_type.as_deref(),
        )
            .cmp(&(
                right.timestamp,
                right.symbol_id.as_str(),
                right.memory_id,
                right.turn_index,
                right.reference_type.as_deref(),
            ))
    });
    snapshot
        .key_values
        .sort_by(|left, right| left.key.cmp(&right.key));
}

fn sort_knowledge_snapshot(snapshot: &mut KnowledgeSnapshot) {
    snapshot
        .corpus
        .sort_by(|left, right| left.corpus_id.cmp(&right.corpus_id));
    snapshot
        .packs
        .sort_by(|left, right| left.pack_id.cmp(&right.pack_id));
    snapshot
        .canon
        .sort_by(|left, right| left.canon_id.cmp(&right.canon_id));
    snapshot
        .conflicts
        .sort_by(|left, right| left.conflict_id.cmp(&right.conflict_id));
    snapshot
        .chunks
        .sort_by(|left, right| left.chunk_id.cmp(&right.chunk_id));
    snapshot
        .usage
        .sort_by(|left, right| left.signal_id.cmp(&right.signal_id));
}

fn memory_manifest(
    source: &MemoryMigrationSnapshot,
    source_digest: String,
    target_digest: String,
) -> MemoryMigrationManifest {
    MemoryMigrationManifest {
        domain: MEMORY_DOMAIN.to_string(),
        schema_version: source.schema_version,
        source_digest,
        target_digest,
        entry_count: source.entries.len(),
        entity_count: source.entities.len(),
        triple_count: source.triples.len(),
        verbatim_count: source.verbatim.len(),
        symbol_count: source.symbols.len(),
        edge_count: source.edges.len(),
        symbol_reference_count: source.symbol_memory_references.len(),
        key_value_count: source.key_values.len(),
    }
}

fn knowledge_manifest(
    source: &KnowledgeMigrationSnapshot,
    source_digest: String,
    target_digest: String,
) -> KnowledgeMigrationManifest {
    KnowledgeMigrationManifest {
        domain: KNOWLEDGE_DOMAIN.to_string(),
        schema_version: source.schema_version,
        source_digest,
        target_digest,
        corpus_count: source.state.corpus.len(),
        pack_count: source.state.packs.len(),
        canon_count: source.state.canon.len(),
        conflict_count: source.state.conflicts.len(),
        chunk_count: source.state.chunks.len(),
        usage_count: source.state.usage.len(),
    }
}

fn row_to_legacy_scope_report(row: &Row) -> MemoryResult<LegacyScopeMigrationReport> {
    Ok(LegacyScopeMigrationReport {
        memory_id: row.try_get(0).map_err(postgres_memory_error)?,
        raw_scope: row.try_get(1).map_err(postgres_memory_error)?,
        held_scope: row.try_get(2).map_err(postgres_memory_error)?,
        reason: row.try_get(3).map_err(postgres_memory_error)?,
        migrated_at: row.try_get(4).map_err(postgres_memory_error)?,
    })
}

fn json_rows_from_client<T: DeserializeOwned>(
    client: &mut impl PostgresClient,
    table: &str,
) -> MemoryResult<Vec<T>> {
    let sql = match table {
        "memory_entities" => "SELECT payload FROM memory_entities ORDER BY id",
        "memory_triples" => "SELECT payload FROM memory_triples ORDER BY id",
        _ => {
            return Err(MemoryError::Store(format!(
                "unsupported memory snapshot table `{table}`"
            )))
        }
    };
    client
        .query(sql, &[])
        .map_err(postgres_memory_error)?
        .iter()
        .map(|row| {
            let payload: serde_json::Value = row.try_get(0).map_err(postgres_memory_error)?;
            serde_json::from_value(payload).map_err(json_memory_error)
        })
        .collect()
}

fn write_symbol(client: &mut impl PostgresClient, symbol: &CodeSymbol) -> MemoryResult<()> {
    let line = i64::try_from(symbol.line)
        .map_err(|_| MemoryError::Store("code symbol line overflow".to_string()))?;
    client
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

fn write_entry(
    client: &mut impl PostgresClient,
    entry: &MemoryEntry,
    upsert: bool,
) -> MemoryResult<()> {
    let id = entry.id.to_string();
    let layer = enum_label(&entry.layer)?;
    let category = enum_label(&entry.category)?;
    let priority = enum_label(&entry.priority)?;
    let source = enum_label(&entry.source)?;
    let scope = entry.scope.scope_key();
    let created_at = entry.created_at.to_rfc3339();
    let updated_at = entry.updated_at.to_rfc3339();
    let payload = serde_json::to_value(entry).map_err(json_memory_error)?;
    let sql = if upsert {
        "INSERT INTO memory_entries(
            id,layer,category,priority,source,title,content,scope_key,session_id,
            source_agent,created_at,updated_at,payload
         ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
         ON CONFLICT(id) DO UPDATE SET layer=EXCLUDED.layer,category=EXCLUDED.category,
            priority=EXCLUDED.priority,source=EXCLUDED.source,title=EXCLUDED.title,
            content=EXCLUDED.content,scope_key=EXCLUDED.scope_key,
            session_id=EXCLUDED.session_id,source_agent=EXCLUDED.source_agent,
            created_at=EXCLUDED.created_at,updated_at=EXCLUDED.updated_at,payload=EXCLUDED.payload"
    } else {
        "UPDATE memory_entries SET layer=$2,category=$3,priority=$4,source=$5,title=$6,
            content=$7,scope_key=$8,session_id=$9,source_agent=$10,created_at=$11,
            updated_at=$12,payload=$13 WHERE id=$1"
    };
    client
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
            ],
        )
        .map_err(postgres_memory_error)?;
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
    MemoryError::Store(error.to_string())
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
mod tests {
    use std::sync::Arc;

    use harness_contract::knowledge::{
        KnowledgeActivationPolicy, KnowledgeGovernanceLevel, KnowledgeNamespace,
        KnowledgeUsageSignal,
    };
    use memory::{
        code_indexer::{CodeSymbol, SymbolEdge, SymbolEdgeType, SymbolKind},
        entity::{Entity, EntityType, Triple},
        knowledge::{DocumentContent, InMemoryKnowledgeStore, KnowledgeFabric, KnowledgeStore},
        project_scope::MemoryScope,
        store::{sqlite::SqliteStore, MemoryStore},
        types::AgentVisibility,
        MemoryCategory, MemoryEntry, MemoryLayer, MemorySource, Priority,
    };
    use storage::{PostgresConnectionConfig, StaticSecretRefResolver};

    use super::*;

    fn memory_entry(id: uuid::Uuid, marker: &str) -> MemoryEntry {
        let now = chrono::Utc::now();
        MemoryEntry {
            id,
            layer: MemoryLayer::L3,
            category: MemoryCategory::ProjectKnowledge,
            priority: Priority::High,
            source: MemorySource::Import,
            title: format!("durable {marker}"),
            content: format!("portable memory truth {marker}"),
            embedding: Some(vec![0.25, 0.75]),
            tags: vec!["migration".to_string()],
            relations: Vec::new(),
            confidence: 0.9,
            access_count: 0,
            staleness: 0.0,
            created_at: now,
            updated_at: now,
            last_accessed_at: None,
            scope: MemoryScope::Project("snapshot-test".to_string()),
            session_id: Some("session-snapshot".to_string()),
            source_agent: Some("test-agent".to_string()),
            visibility: AgentVisibility::Private,
        }
    }

    #[tokio::test]
    async fn sqlite_snapshot_covers_every_durable_memory_class() {
        let store = SqliteStore::open_in_memory().expect("sqlite memory store");
        let id = uuid::Uuid::new_v4();
        store.insert(&memory_entry(id, "snapshot")).await.unwrap();
        store
            .save_entities(&[Entity {
                id: "entity-1".to_string(),
                name: "Cowd".to_string(),
                entity_type: EntityType::Project,
                confidence: 1.0,
                frequency: 2,
                first_seen: chrono::Utc::now(),
                last_seen: chrono::Utc::now(),
                source_ids: vec![id.to_string()],
                source_type: "memory".to_string(),
            }])
            .await
            .unwrap();
        store
            .save_triples(&[Triple {
                id: "triple-1".to_string(),
                subject_id: "entity-1".to_string(),
                predicate: "uses".to_string(),
                object_id: "postgres".to_string(),
                valid_from: None,
                valid_to: None,
                source: Some("test".to_string()),
                confidence: 1.0,
                created_at: chrono::Utc::now(),
                source_agent: None,
            }])
            .await
            .unwrap();
        store
            .save_verbatim("verbatim-1", "raw", "test", 3, "2026-07-23T00:00:00Z")
            .await
            .unwrap();
        store
            .insert_symbol(&CodeSymbol {
                id: "symbol-1".to_string(),
                name: "snapshot".to_string(),
                kind: SymbolKind::Function,
                file_path: "src/lib.rs".to_string(),
                line: 7,
                signature: "fn snapshot()".to_string(),
                doc: Some("test".to_string()),
            })
            .await
            .unwrap();
        store
            .insert_edge(&SymbolEdge {
                source_id: "symbol-1".to_string(),
                target_id: "symbol-2".to_string(),
                edge_type: SymbolEdgeType::Calls,
                file_path: "src/lib.rs".to_string(),
            })
            .await
            .unwrap();
        store
            .link_symbol_to_memory("symbol-1", &id, Some(3), "mentioned", 42)
            .await
            .unwrap();
        store.kv_put("closet:test", "value").await.unwrap();

        let first = export_memory_snapshot(&store).await.unwrap();
        let second = export_memory_snapshot(&store).await.unwrap();
        assert_eq!(
            first.canonical_digest().unwrap(),
            second.canonical_digest().unwrap()
        );
        assert_eq!(first.entries.len(), 1);
        assert_eq!(first.entities.len(), 1);
        assert_eq!(first.triples.len(), 1);
        assert_eq!(first.verbatim.len(), 1);
        assert_eq!(first.symbols.len(), 1);
        assert_eq!(first.edges.len(), 1);
        assert_eq!(first.symbol_memory_references.len(), 1);
        assert_eq!(first.key_values.len(), 1);
        assert_eq!(
            first.entries[0].embedding.as_deref(),
            Some(&[0.25, 0.75][..])
        );
    }

    #[tokio::test]
    async fn vector_unavailable_is_not_reported_as_no_match() {
        let store = SqliteStore::open_in_memory().expect("sqlite memory store");
        assert!(!store.capabilities().vector_search);
        let error = store.search_vector(&[0.5], 1).await.unwrap_err();
        assert!(matches!(error, MemoryError::CapabilityUnavailable { .. }));
    }

    #[test]
    fn knowledge_snapshot_is_canonical_and_complete() {
        let store = Arc::new(InMemoryKnowledgeStore::new());
        let fabric = KnowledgeFabric::with_store(store.clone());
        let receipt = fabric.ingest_document(
            KnowledgeNamespace::Project("snapshot-test".to_string()),
            KnowledgeActivationPolicy::OnDemand,
            KnowledgeGovernanceLevel::Advisory,
            DocumentContent::new("Snapshot", "The durable knowledge body."),
        );
        store
            .record_usage(&KnowledgeUsageSignal {
                signal_id: "usage-1".to_string(),
                session_id: "session-1".to_string(),
                pack_id: receipt.pack.pack_id,
                action: "activated".to_string(),
                summary: "used in test".to_string(),
                score_delta_bp: 10,
                occurred_at: chrono::Utc::now(),
            })
            .unwrap();
        let first = export_knowledge_snapshot(store.as_ref()).unwrap();
        let second = export_knowledge_snapshot(store.as_ref()).unwrap();
        assert_eq!(
            first.canonical_digest().unwrap(),
            second.canonical_digest().unwrap()
        );
        assert_eq!(first.state.corpus.len(), 1);
        assert_eq!(first.state.packs.len(), 1);
        assert_eq!(first.state.canon.len(), 1);
        assert!(!first.state.chunks.is_empty());
        assert_eq!(first.state.usage.len(), 1);
    }

    #[tokio::test]
    #[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
    async fn real_postgres_memory_roundtrip() {
        let url =
            std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
        let marker = uuid::Uuid::new_v4().simple().to_string();
        let mut config = PostgresConnectionConfig::new(
            format!("memory-test-{marker}"),
            "memory-test-url",
            format!("cowd-memory-test-{marker}"),
        );
        config.max_connections = 4;
        let resolver = StaticSecretRefResolver::new([("memory-test-url".to_string(), url)]);
        let store =
            PostgresMemoryStore::connect(config.clone(), &resolver).expect("connect PostgreSQL");
        let source = SqliteStore::open_in_memory().expect("SQLite migration source");
        let id = uuid::Uuid::new_v4();
        let entry = memory_entry(id, &marker);
        source.insert(&entry).await.unwrap();
        source
            .kv_put(&format!("migration:{marker}"), "present")
            .await
            .unwrap();
        source
            .insert_symbol(&CodeSymbol {
                id: format!("symbol-{marker}"),
                name: marker.clone(),
                kind: SymbolKind::Function,
                file_path: "src/real_pg_test.rs".to_string(),
                line: 1,
                signature: format!("fn {marker}()"),
                doc: None,
            })
            .await
            .unwrap();
        source
            .link_symbol_to_memory(&format!("symbol-{marker}"), &id, None, "test", 1)
            .await
            .unwrap();

        let manifest = copy_quiesced_memory_store(&source, &store)
            .await
            .expect("quiesced Memory copy");
        assert_eq!(manifest.source_digest, manifest.target_digest);
        assert_eq!(manifest.entry_count, 1);
        assert_eq!(manifest.symbol_reference_count, 1);

        let reopened =
            PostgresMemoryStore::connect(config, &resolver).expect("reopen PostgreSQL owner");
        let loaded = reopened.get(&id).await.unwrap().expect("persisted entry");
        assert_eq!(loaded.id, id);
        assert!(reopened
            .search_fts(&marker, 10)
            .await
            .unwrap()
            .iter()
            .any(|item| item.id == id));
        assert!(reopened
            .search_fts_scoped(
                &marker,
                &MemoryScope::Project("snapshot-test".to_string()),
                10,
            )
            .await
            .unwrap()
            .iter()
            .any(|item| item.id == id));
        assert_eq!(
            reopened.find_memories_by_symbol(&marker).await.unwrap(),
            vec![id]
        );
        assert_eq!(
            reopened
                .kv_get(&format!("migration:{marker}"))
                .await
                .unwrap()
                .as_deref(),
            Some("present")
        );

        let knowledge_source = Arc::new(InMemoryKnowledgeStore::new());
        let fabric = KnowledgeFabric::with_store(knowledge_source.clone());
        let receipt = fabric.ingest_document(
            KnowledgeNamespace::Project(format!("project-{marker}")),
            KnowledgeActivationPolicy::OnDemand,
            KnowledgeGovernanceLevel::Required,
            DocumentContent::new("Real PG", format!("knowledge {marker}")),
        );
        knowledge_source
            .record_usage(&KnowledgeUsageSignal {
                signal_id: format!("usage-{marker}"),
                session_id: format!("session-{marker}"),
                pack_id: receipt.pack.pack_id,
                action: "activated".to_string(),
                summary: "real PostgreSQL test".to_string(),
                score_delta_bp: 25,
                occurred_at: chrono::Utc::now(),
            })
            .unwrap();
        let knowledge_target = PostgresKnowledgeStore::new(reopened.executor().clone())
            .expect("PostgreSQL Knowledge owner");
        let knowledge_manifest =
            copy_quiesced_knowledge_store(knowledge_source.as_ref(), &knowledge_target)
                .expect("quiesced Knowledge copy");
        assert_eq!(
            knowledge_manifest.source_digest,
            knowledge_manifest.target_digest
        );
        let knowledge_reopened = PostgresKnowledgeStore::new(reopened.executor().clone())
            .expect("reopen Knowledge owner");
        let knowledge_snapshot = knowledge_reopened.snapshot().unwrap();
        assert_eq!(knowledge_snapshot.corpus.len(), 1);
        assert_eq!(knowledge_snapshot.usage.len(), 1);

        let mut concurrent_ids = Vec::new();
        let mut tasks = Vec::new();
        for index in 0..8 {
            let concurrent_id = uuid::Uuid::new_v4();
            concurrent_ids.push(concurrent_id);
            let concurrent_store = reopened.clone();
            let concurrent_marker = format!("{marker}-{index}");
            tasks.push(tokio::spawn(async move {
                concurrent_store
                    .insert(&memory_entry(concurrent_id, &concurrent_marker))
                    .await
            }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        for concurrent_id in concurrent_ids {
            assert!(reopened.get(&concurrent_id).await.unwrap().is_some());
            reopened.delete(&concurrent_id).await.unwrap();
        }
        assert!(reopened.executor().health().metrics.checkout_count > 8);
    }
}
