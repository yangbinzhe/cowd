//! `MemoryStore` trait – the unified storage abstraction.
//!
//! All selected durable backends implement this trait so higher-level layers
//! remain backend-agnostic. Rebuildable blob/vector accelerators are separate
//! capabilities rather than partial `MemoryStore` implementations.

use async_trait::async_trait;

use crate::{
    code_indexer::{CodeSymbol, SymbolEdge},
    entity::{Entity, Triple},
    error::MemoryError,
    project_scope::MemoryScope,
    types::{MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemoryMeta},
};

pub use verbatim::VerbatimEntry;

pub mod blob;
pub mod sqlite;
pub mod vector;
pub mod verbatim;

/// Unified result type used throughout the store module.
pub type Result<T> = std::result::Result<T, MemoryError>;

/// Advanced FTS search options.
#[derive(Debug, Clone, Default)]
pub struct FtsSearchOptions {
    /// Filter by category.
    pub category: Option<MemoryCategory>,
    /// Filter by layer.
    pub layer: Option<MemoryLayer>,
    /// Include highlighted snippets.
    pub with_snippets: bool,
    /// Extract matched keywords.
    pub with_keywords: bool,
}

/// FTS search result with optional snippets.
#[derive(Debug, Clone)]
pub struct FtsSearchResult {
    pub entries: Vec<MemoryEntry>,
    pub snippets: Vec<Option<String>>,
    pub total_matches: usize,
    pub keywords: Vec<(String, i64)>,
}

/// Runtime-readable capability verdict. A disabled accelerator must never be
/// represented as an empty successful search result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryStoreCapabilities {
    pub backend: &'static str,
    pub full_text_search: bool,
    pub lexical_fallback: bool,
    pub vector_search: bool,
    pub code_index: bool,
}

/// A durable report for a historical scope that was deliberately kept out of
/// normal recall until an operator classifies it.  This is a port DTO rather
/// than a SQLite implementation detail because it must survive a backend
/// cutover unchanged.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LegacyScopeMigrationReport {
    pub memory_id: String,
    pub raw_scope: Option<String>,
    pub held_scope: String,
    pub reason: String,
    pub migrated_at: String,
}

/// A durable code-to-memory recall association.  Listing these records is
/// required for a verifiable, quiesced backend migration; `find_*` alone is
/// insufficient because it loses turn, reference type, and ordering truth.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SymbolMemoryReference {
    pub symbol_id: String,
    pub memory_id: MemoryId,
    pub turn_index: Option<i32>,
    pub reference_type: Option<String>,
    pub timestamp: i64,
}

/// A durable auxiliary key/value record.  This is intentionally enumerable
/// only through the storage port so a migration cannot silently drop Closet,
/// Seed, or other auxiliary state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MemoryKeyValue {
    pub key: String,
    pub value: String,
}

/// Exact authority lookup derived from `memory_authority::same_memory_key`.
#[derive(Debug, Clone)]
pub struct AuthorityLookup {
    pub fingerprint: String,
    pub scope: MemoryScope,
    pub limit: usize,
}

/// Bounded governance lookup for subsystem-owned entries identified by one
/// of their durable tags.
#[derive(Debug, Clone)]
pub struct TaggedLookup {
    pub scope: MemoryScope,
    pub tags_any: Vec<String>,
    pub source_agent: Option<String>,
    pub limit: usize,
}

/// Stable keyset cursor for bounded maintenance scans.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MemoryScanCursor {
    pub updated_at: Option<String>,
    pub id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MemoryScanPage {
    pub entries: Vec<MemoryEntry>,
    pub next: Option<MemoryScanCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MemoryLayerAggregate {
    pub layer: MemoryLayer,
    pub retained_count: u64,
    pub active_count: u64,
    pub archived_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MemoryStoreAggregate {
    pub total_entries: u64,
    pub active_entries: u64,
    pub orientation_like: u64,
    pub conflicted: u64,
    pub stale: u64,
    pub evidence_backed: u64,
    pub linked: u64,
    pub layers: Vec<MemoryLayerAggregate>,
}

/// The primary storage trait that all backends must implement.
#[async_trait]
pub trait MemoryStore: Send + Sync {
    /// Return the selected store's durable/rebuildable capability verdict.
    fn capabilities(&self) -> MemoryStoreCapabilities;
    /// Persist a new memory entry and return its assigned ID.
    async fn insert(&self, entry: &MemoryEntry) -> Result<MemoryId>;

    /// Retrieve a memory entry by its ID.
    async fn get(&self, id: &MemoryId) -> Result<Option<MemoryEntry>>;

    /// Overwrite an existing entry (matched by `entry.id`).
    async fn update(&self, entry: &MemoryEntry) -> Result<()>;

    /// Permanently remove a memory entry.
    async fn delete(&self, id: &MemoryId) -> Result<()>;

    /// Full-text search across `title` and `content` fields.
    async fn search_fts(&self, query: &str, limit: usize) -> Result<Vec<MemoryEntry>>;

    /// Full-text search with scope filtering — only returns entries matching
    /// the requested scope, plus globally-scoped entries.
    async fn search_fts_scoped(
        &self,
        query: &str,
        scope: &MemoryScope,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>>;

    /// Advanced FTS search with filtering and snippets.
    async fn search_fts_advanced(
        &self,
        query: &str,
        options: FtsSearchOptions,
        limit: usize,
    ) -> Result<FtsSearchResult>;

    /// Approximate nearest-neighbour search using a pre-computed embedding.
    async fn search_vector(&self, embedding: &[f32], limit: usize) -> Result<Vec<MemoryEntry>>;

    /// Return all entries belonging to the given layer.
    async fn search_by_layer(&self, layer: MemoryLayer) -> Result<Vec<MemoryEntry>>;

    /// Return all entries matching the given semantic category.
    async fn search_by_category(&self, category: MemoryCategory) -> Result<Vec<MemoryEntry>>;

    /// Exact, indexed duplicate/conflict candidates in the same authority scope.
    async fn lookup_authority_candidates(&self, query: AuthorityLookup)
        -> Result<Vec<MemoryEntry>>;
    async fn lookup_tagged_candidates(&self, query: TaggedLookup) -> Result<Vec<MemoryEntry>>;

    /// Bounded facts of one category/scope used by semantic checkpoint review.
    async fn lookup_fact_candidates(
        &self,
        scope: &MemoryScope,
        category: MemoryCategory,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>>;

    /// Bounded, scoped semantic checkpoint recall without a full-store scan.
    async fn search_semantic_checkpoints(
        &self,
        scope: &MemoryScope,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>>;

    /// Keyset-paginated maintenance scan. Foreground turn paths must not call it.
    async fn scan_entries_page(
        &self,
        cursor: MemoryScanCursor,
        limit: usize,
    ) -> Result<MemoryScanPage>;

    /// Return lightweight counts used by health and status projections.
    /// Implementations must aggregate in the selected backend and must not
    /// materialize entry content, embeddings, tags, or relations.
    async fn aggregate(&self, stale_threshold: f32) -> Result<MemoryStoreAggregate>;

    /// Retrieve lightweight metadata for a single entry.
    async fn get_meta(&self, id: &MemoryId) -> Result<Option<MemoryMeta>>;

    /// List metadata for all entries, optionally filtered by layer.
    async fn list_metas(&self, layer: Option<MemoryLayer>) -> Result<Vec<MemoryMeta>>;

    /// List all entries across all layers (for temporal graph queries).
    async fn list_all(&self) -> Result<Vec<MemoryEntry>>;

    /// Batch auxiliary-state lookup used for lifecycle filtering.
    async fn kv_get_many(&self, keys: &[String]) -> Result<Vec<MemoryKeyValue>>;

    /// Return historical scope records that were deliberately held during a
    /// contract migration. Selected durable backends must implement this
    /// explicitly so cutover cannot silently omit held records.
    async fn legacy_scope_migration_reports(&self) -> Result<Vec<LegacyScopeMigrationReport>>;

    // -----------------------------------------------------------------------
    // Knowledge-graph persistence
    // -----------------------------------------------------------------------

    /// Save all entities to persistent storage.
    async fn save_entities(&self, entities: &[Entity]) -> Result<()>;

    /// Load all entities from persistent storage.
    async fn load_entities(&self) -> Result<Vec<Entity>>;

    /// Save all triples to persistent storage.
    async fn save_triples(&self, triples: &[Triple]) -> Result<()>;

    /// Load all triples from persistent storage.
    async fn load_triples(&self) -> Result<Vec<Triple>>;

    // -----------------------------------------------------------------------
    // Verbatim sink (zero-loss raw storage)
    // -----------------------------------------------------------------------

    /// Store a verbatim entry that never passes through compression.
    async fn save_verbatim(
        &self,
        id: &str,
        content: &str,
        source: &str,
        layer: i32,
        timestamp: &str,
    ) -> Result<()>;

    /// Retrieve a verbatim entry by its ID.
    async fn load_verbatim_by_id(&self, id: &str) -> Result<Option<VerbatimEntry>>;

    /// Search verbatim entries whose content matches a SQL LIKE pattern.
    ///
    /// The caller should include `%` wildcards (e.g. `"%keyword%"`).
    async fn search_verbatim_by_content(&self, query: &str) -> Result<Vec<VerbatimEntry>>;

    /// Enumerate every durable verbatim entry for a quiesced migration. This
    /// is intentionally separate from request-path content search.
    async fn list_verbatim_entries(&self) -> Result<Vec<VerbatimEntry>>;

    // -----------------------------------------------------------------------
    // Code symbol persistence (Phase 1: code indexer storage)
    // -----------------------------------------------------------------------

    /// Persist a code symbol extracted from source code.
    async fn insert_symbol(&self, sym: &CodeSymbol) -> Result<()>;

    /// Full-text search across indexed code symbols.
    async fn search_symbols(&self, query: &str, limit: usize) -> Result<Vec<CodeSymbol>>;

    /// Persist a code edge (call/import/extends/implements) between two symbols.
    async fn insert_edge(&self, edge: &SymbolEdge) -> Result<()>;

    /// Find all symbols that call the given symbol (callers / upstream).
    async fn get_callers(&self, symbol_id: &str) -> Result<Vec<CodeSymbol>>;

    /// Find all symbols called by the given symbol (callees / downstream).
    async fn get_callees(&self, symbol_id: &str) -> Result<Vec<CodeSymbol>>;

    /// List all code symbols in the index (no filtering).
    async fn list_all_symbols(&self) -> Result<Vec<CodeSymbol>>;

    /// List all code edges in the index (no filtering).
    async fn list_all_edges(&self) -> Result<Vec<SymbolEdge>>;

    // -----------------------------------------------------------------------
    // Symbol ↔ memory linking (Phase 2: L3 deep recall integration)
    // -----------------------------------------------------------------------

    /// Link a code symbol to a memory entry (conversation context).
    ///
    /// Each time a symbol is referenced during a conversation turn,
    /// this records the association so that `find_memories_by_symbol`
    /// can retrieve all conversations that mentioned a given symbol.
    async fn link_symbol_to_memory(
        &self,
        _symbol_id: &str,
        _memory_id: &MemoryId,
        _turn_index: Option<i32>,
        _reference_type: &str,
        _timestamp: i64,
    ) -> Result<()>;

    /// Find all memory entries that reference a given code symbol.
    ///
    /// Searches the `symbol_references` table for all memory IDs
    /// linked to `symbol_name` (matched case-insensitively).
    async fn find_memories_by_symbol(&self, symbol_name: &str) -> Result<Vec<MemoryId>>;

    /// Enumerate every durable symbol-to-memory association for a quiesced
    /// migration. This is not a normal recall API.
    async fn list_symbol_memory_references(&self) -> Result<Vec<SymbolMemoryReference>>;

    // -----------------------------------------------------------------------
    // Key-value store (for Closet, Seeds, and auxiliary persistence)
    // -----------------------------------------------------------------------

    /// Store a key-value pair. Replaces existing value if key exists.
    /// Used by Closet index, Seed registry, and other auxiliary data.
    async fn kv_put(&self, key: &str, value: &str) -> Result<()>;

    /// Retrieve a value by key. Returns None if key does not exist.
    async fn kv_get(&self, key: &str) -> Result<Option<String>>;

    /// Enumerate all durable auxiliary values for a quiesced migration. This
    /// is not intended for request-path cache access.
    async fn list_key_values(&self) -> Result<Vec<MemoryKeyValue>>;
}
