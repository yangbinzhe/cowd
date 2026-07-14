//! `MemoryStore` trait – the unified storage abstraction.
//!
//! All concrete backends (`SQLite`, blob FS, vector index) implement this trait
//! so that higher-level layers can remain backend-agnostic.

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
pub mod session;
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

/// The primary storage trait that all backends must implement.
#[async_trait]
pub trait MemoryStore: Send + Sync {
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
    ) -> Result<Vec<MemoryEntry>> {
        let _ = scope;
        self.search_fts(query, limit).await
    }

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

    /// Retrieve lightweight metadata for a single entry.
    async fn get_meta(&self, id: &MemoryId) -> Result<Option<MemoryMeta>>;

    /// List metadata for all entries, optionally filtered by layer.
    async fn list_metas(&self, layer: Option<MemoryLayer>) -> Result<Vec<MemoryMeta>>;

    /// List all entries across all layers (for temporal graph queries).
    async fn list_all(&self) -> Result<Vec<MemoryEntry>>;

    /// Return historical scope records that were deliberately held during a
    /// contract migration. Backends without durable scope migration support
    /// expose an empty report rather than pretending those records are
    /// recallable.
    async fn legacy_scope_migration_reports(
        &self,
    ) -> Result<Vec<sqlite::LegacyScopeMigrationReport>> {
        Ok(Vec::new())
    }

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

    // -----------------------------------------------------------------------
    // Code symbol persistence (Phase 1: code indexer storage)
    // -----------------------------------------------------------------------

    /// Persist a code symbol extracted from source code.
    async fn insert_symbol(&self, _sym: &CodeSymbol) -> Result<()> {
        Err(MemoryError::Store(
            "code symbol storage not supported by this backend".into(),
        ))
    }

    /// Full-text search across indexed code symbols.
    async fn search_symbols(&self, _query: &str, _limit: usize) -> Result<Vec<CodeSymbol>> {
        Err(MemoryError::Store(
            "code symbol search not supported by this backend".into(),
        ))
    }

    /// Persist a code edge (call/import/extends/implements) between two symbols.
    async fn insert_edge(&self, _edge: &SymbolEdge) -> Result<()> {
        Err(MemoryError::Store(
            "code edge storage not supported by this backend".into(),
        ))
    }

    /// Find all symbols that call the given symbol (callers / upstream).
    async fn get_callers(&self, _symbol_id: &str) -> Result<Vec<CodeSymbol>> {
        Err(MemoryError::Store(
            "code symbol callers not supported by this backend".into(),
        ))
    }

    /// Find all symbols called by the given symbol (callees / downstream).
    async fn get_callees(&self, _symbol_id: &str) -> Result<Vec<CodeSymbol>> {
        Err(MemoryError::Store(
            "code symbol callees not supported by this backend".into(),
        ))
    }

    /// List all code symbols in the index (no filtering).
    async fn list_all_symbols(&self) -> Result<Vec<CodeSymbol>> {
        Err(MemoryError::Store(
            "code symbol listing not supported by this backend".into(),
        ))
    }

    /// List all code edges in the index (no filtering).
    async fn list_all_edges(&self) -> Result<Vec<SymbolEdge>> {
        Err(MemoryError::Store(
            "code edge listing not supported by this backend".into(),
        ))
    }

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
    ) -> Result<()> {
        Err(MemoryError::Store(
            "symbol↔memory linking not supported by this backend".into(),
        ))
    }

    /// Find all memory entries that reference a given code symbol.
    ///
    /// Searches the `symbol_references` table for all memory IDs
    /// linked to `symbol_name` (matched case-insensitively).
    async fn find_memories_by_symbol(&self, _symbol_name: &str) -> Result<Vec<MemoryId>> {
        Err(MemoryError::Store(
            "symbol↔memory lookup not supported by this backend".into(),
        ))
    }

    // -----------------------------------------------------------------------
    // Key-value store (for Closet, Seeds, and auxiliary persistence)
    // -----------------------------------------------------------------------

    /// Store a key-value pair. Replaces existing value if key exists.
    /// Used by Closet index, Seed registry, and other auxiliary data.
    async fn kv_put(&self, _key: &str, _value: &str) -> Result<()> {
        Err(MemoryError::Store(
            "kv store not supported by this backend".into(),
        ))
    }

    /// Retrieve a value by key. Returns None if key does not exist.
    async fn kv_get(&self, _key: &str) -> Result<Option<String>> {
        Err(MemoryError::Store(
            "kv store not supported by this backend".into(),
        ))
    }
}
