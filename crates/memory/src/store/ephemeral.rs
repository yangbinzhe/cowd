//! Explicit non-durable Memory port for pure business-logic tests.
//!
//! Runtime composition never selects this adapter. PostgreSQL tests remain
//! responsible for transaction, restart, concurrency, and SQL semantics.

use std::{collections::BTreeMap, sync::RwLock};

use async_trait::async_trait;

use super::*;
use crate::{
    code_indexer::{CodeSymbol, SymbolEdge},
    entity::{Entity, Triple},
    error::MemoryError,
    memory_authority::same_memory_key,
    project_scope::MemoryScope,
    types::{MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemoryMeta},
};

#[derive(Debug, Default)]
struct State {
    entries: BTreeMap<String, MemoryEntry>,
    entities: BTreeMap<String, Entity>,
    triples: BTreeMap<String, Triple>,
    verbatim: BTreeMap<String, VerbatimEntry>,
    symbols: BTreeMap<String, CodeSymbol>,
    edges: Vec<SymbolEdge>,
    symbol_memory: Vec<SymbolMemoryReference>,
    kv: BTreeMap<String, String>,
}

#[derive(Debug, Default)]
pub struct EphemeralMemoryStore {
    state: RwLock<State>,
}

impl EphemeralMemoryStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn read(&self) -> Result<std::sync::RwLockReadGuard<'_, State>> {
        self.state
            .read()
            .map_err(|_| MemoryError::Store("ephemeral Memory read lock poisoned".into()))
    }

    fn write(&self) -> Result<std::sync::RwLockWriteGuard<'_, State>> {
        self.state
            .write()
            .map_err(|_| MemoryError::Store("ephemeral Memory write lock poisoned".into()))
    }
}

fn matches_text(entry: &MemoryEntry, query: &str) -> bool {
    let query = query.trim().to_lowercase();
    if query.is_empty() {
        return true;
    }
    let searchable = format!(
        "{} {} {}",
        entry.title.to_lowercase(),
        entry.content.to_lowercase(),
        entry.tags.join(" ").to_lowercase()
    );
    query
        .split_whitespace()
        .map(|term| term.trim_matches(|character: char| matches!(character, '"' | '*' | '\'')))
        .filter(|term| !term.is_empty())
        .all(|term| searchable.contains(term))
}

fn ordered(mut entries: Vec<MemoryEntry>, limit: usize) -> Vec<MemoryEntry> {
    entries.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    entries.truncate(limit);
    entries
}

fn metadata(entry: &MemoryEntry) -> MemoryMeta {
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

fn is_inactive(state: &State, id: &MemoryId) -> bool {
    state
        .kv
        .get(&format!("memory_lifecycle:{id}"))
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .and_then(|events| events.as_array().and_then(|events| events.last()).cloned())
        .and_then(|event| {
            event
                .get("to")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .is_some_and(|state| matches!(state.as_str(), "Archived" | "Superseded"))
}

#[async_trait]
impl MemoryStore for EphemeralMemoryStore {
    fn capabilities(&self) -> MemoryStoreCapabilities {
        MemoryStoreCapabilities {
            backend: "ephemeral-test",
            full_text_search: true,
            lexical_fallback: true,
            vector_search: true,
            code_index: true,
        }
    }

    async fn insert(&self, entry: &MemoryEntry) -> Result<MemoryId> {
        self.write()?
            .entries
            .insert(entry.id.to_string(), entry.clone());
        Ok(entry.id)
    }
    async fn get(&self, id: &MemoryId) -> Result<Option<MemoryEntry>> {
        Ok(self.read()?.entries.get(&id.to_string()).cloned())
    }
    async fn update(&self, entry: &MemoryEntry) -> Result<()> {
        let mut state = self.write()?;
        if !state.entries.contains_key(&entry.id.to_string()) {
            return Err(MemoryError::NotFound(entry.id.to_string()));
        }
        state.entries.insert(entry.id.to_string(), entry.clone());
        Ok(())
    }
    async fn delete(&self, id: &MemoryId) -> Result<()> {
        self.write()?.entries.remove(&id.to_string());
        Ok(())
    }

    async fn search_fts(&self, query: &str, limit: usize) -> Result<Vec<MemoryEntry>> {
        Ok(ordered(
            self.read()?
                .entries
                .values()
                .filter(|e| matches_text(e, query))
                .cloned()
                .collect(),
            limit,
        ))
    }
    async fn search_fts_scoped(
        &self,
        query: &str,
        scope: &MemoryScope,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>> {
        Ok(ordered(
            self.read()?
                .entries
                .values()
                .filter(|e| (e.scope.is_global() || &e.scope == scope) && matches_text(e, query))
                .cloned()
                .collect(),
            limit,
        ))
    }
    async fn search_fts_advanced(
        &self,
        query: &str,
        options: FtsSearchOptions,
        limit: usize,
    ) -> Result<FtsSearchResult> {
        let all = self
            .read()?
            .entries
            .values()
            .filter(|e| {
                matches_text(e, query)
                    && options.category.is_none_or(|v| e.category == v)
                    && options.layer.is_none_or(|v| e.layer == v)
            })
            .cloned()
            .collect::<Vec<_>>();
        let total_matches = all.len();
        let entries = ordered(all, limit);
        let snippets = if options.with_snippets {
            entries
                .iter()
                .map(|entry| Some(format!("{} — {}", entry.title, entry.content)))
                .collect()
        } else {
            vec![None; entries.len()]
        };
        let keywords = if options.with_keywords {
            query
                .trim_matches(|character: char| matches!(character, '"' | '*' | '\''))
                .split_whitespace()
                .map(|keyword| (keyword.to_string(), 1_i64))
                .collect()
        } else {
            Vec::new()
        };
        Ok(FtsSearchResult {
            snippets,
            entries,
            total_matches,
            keywords,
        })
    }
    async fn search_vector(&self, embedding: &[f32], limit: usize) -> Result<Vec<MemoryEntry>> {
        let mut values = self
            .read()?
            .entries
            .values()
            .filter_map(|entry| {
                entry.embedding.as_ref().map(|v| {
                    (
                        v.iter().zip(embedding).map(|(a, b)| a * b).sum::<f32>(),
                        entry.clone(),
                    )
                })
            })
            .collect::<Vec<_>>();
        values.sort_by(|a, b| b.0.total_cmp(&a.0));
        Ok(values.into_iter().take(limit).map(|(_, e)| e).collect())
    }
    async fn search_by_layer(&self, layer: MemoryLayer) -> Result<Vec<MemoryEntry>> {
        Ok(ordered(
            self.read()?
                .entries
                .values()
                .filter(|e| e.layer == layer)
                .cloned()
                .collect(),
            usize::MAX,
        ))
    }
    async fn search_by_category(&self, category: MemoryCategory) -> Result<Vec<MemoryEntry>> {
        Ok(ordered(
            self.read()?
                .entries
                .values()
                .filter(|e| e.category == category)
                .cloned()
                .collect(),
            usize::MAX,
        ))
    }
    async fn lookup_authority_candidates(
        &self,
        query: AuthorityLookup,
    ) -> Result<Vec<MemoryEntry>> {
        Ok(ordered(
            self.read()?
                .entries
                .values()
                .filter(|e| e.scope == query.scope && same_memory_key(e) == query.fingerprint)
                .cloned()
                .collect(),
            query.limit.clamp(1, 256),
        ))
    }
    async fn lookup_tagged_candidates(&self, query: TaggedLookup) -> Result<Vec<MemoryEntry>> {
        Ok(ordered(
            self.read()?
                .entries
                .values()
                .filter(|e| {
                    e.scope == query.scope
                        && query
                            .source_agent
                            .as_ref()
                            .is_none_or(|a| e.source_agent.as_ref() == Some(a))
                        && e.tags.iter().any(|tag| query.tags_any.contains(tag))
                })
                .cloned()
                .collect(),
            query.limit.clamp(1, 512),
        ))
    }
    async fn lookup_fact_candidates(
        &self,
        scope: &MemoryScope,
        category: MemoryCategory,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>> {
        Ok(ordered(
            self.read()?
                .entries
                .values()
                .filter(|e| &e.scope == scope && e.category == category)
                .cloned()
                .collect(),
            limit.clamp(1, 1024),
        ))
    }
    async fn search_semantic_checkpoints(
        &self,
        scope: &MemoryScope,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>> {
        Ok(ordered(
            self.read()?
                .entries
                .values()
                .filter(|e| {
                    (e.scope.is_global() || &e.scope == scope)
                        && e.tags.iter().any(|tag| tag == "semantic-checkpoint")
                        && matches_text(e, query)
                })
                .cloned()
                .collect(),
            limit.clamp(1, 256),
        ))
    }
    async fn scan_entries_page(
        &self,
        cursor: MemoryScanCursor,
        limit: usize,
    ) -> Result<MemoryScanPage> {
        let mut entries = ordered(self.read()?.entries.values().cloned().collect(), usize::MAX);
        if let (Some(at), Some(id)) = (cursor.updated_at, cursor.id) {
            entries.retain(|e| {
                let t = e.updated_at.to_rfc3339();
                t < at || (t == at && e.id.to_string() > id)
            });
        }
        let limit = limit.clamp(1, 1000);
        entries.truncate(limit);
        let next = (entries.len() == limit)
            .then(|| entries.last())
            .flatten()
            .map(|e| MemoryScanCursor {
                updated_at: Some(e.updated_at.to_rfc3339()),
                id: Some(e.id.to_string()),
            });
        Ok(MemoryScanPage { entries, next })
    }
    async fn aggregate(&self, stale_threshold: f32) -> Result<MemoryStoreAggregate> {
        let state = self.read()?;
        let mut out = MemoryStoreAggregate::default();
        for layer in [
            MemoryLayer::L0,
            MemoryLayer::L1,
            MemoryLayer::L2,
            MemoryLayer::L3,
            MemoryLayer::L4,
        ] {
            let retained = state.entries.values().filter(|e| e.layer == layer).count() as u64;
            let archived = state
                .entries
                .values()
                .filter(|entry| entry.layer == layer && is_inactive(&state, &entry.id))
                .count() as u64;
            if retained > 0 {
                out.layers.push(MemoryLayerAggregate {
                    layer,
                    retained_count: retained,
                    active_count: retained.saturating_sub(archived),
                    archived_count: archived,
                });
            }
        }
        out.total_entries = state.entries.len() as u64;
        out.active_entries = state
            .entries
            .values()
            .filter(|entry| !is_inactive(&state, &entry.id))
            .count() as u64;
        out.evidence_backed = out.total_entries;
        out.orientation_like = state
            .entries
            .values()
            .filter(|e| matches!(e.layer, MemoryLayer::L0 | MemoryLayer::L1 | MemoryLayer::L2))
            .count() as u64;
        out.conflicted = state
            .entries
            .values()
            .filter(|e| e.confidence < 0.35)
            .count() as u64;
        out.stale = state
            .entries
            .values()
            .filter(|e| e.staleness >= stale_threshold)
            .count() as u64;
        out.linked = state
            .entries
            .values()
            .filter(|e| !e.tags.is_empty() || !e.relations.is_empty())
            .count() as u64;
        Ok(out)
    }
    async fn get_meta(&self, id: &MemoryId) -> Result<Option<MemoryMeta>> {
        Ok(self.read()?.entries.get(&id.to_string()).map(metadata))
    }
    async fn list_metas(&self, layer: Option<MemoryLayer>) -> Result<Vec<MemoryMeta>> {
        Ok(self
            .read()?
            .entries
            .values()
            .filter(|e| layer.is_none_or(|v| e.layer == v))
            .map(metadata)
            .collect())
    }
    async fn list_all(&self) -> Result<Vec<MemoryEntry>> {
        Ok(ordered(
            self.read()?.entries.values().cloned().collect(),
            usize::MAX,
        ))
    }
    async fn kv_get_many(&self, keys: &[String]) -> Result<Vec<MemoryKeyValue>> {
        let s = self.read()?;
        Ok(keys
            .iter()
            .filter_map(|k| {
                s.kv.get(k).map(|v| MemoryKeyValue {
                    key: k.clone(),
                    value: v.clone(),
                })
            })
            .collect())
    }

    async fn save_entities(&self, values: &[Entity]) -> Result<()> {
        let mut s = self.write()?;
        for v in values {
            s.entities.insert(v.id.clone(), v.clone());
        }
        Ok(())
    }
    async fn load_entities(&self) -> Result<Vec<Entity>> {
        Ok(self.read()?.entities.values().cloned().collect())
    }
    async fn save_triples(&self, values: &[Triple]) -> Result<()> {
        let mut s = self.write()?;
        for v in values {
            s.triples.insert(v.id.clone(), v.clone());
        }
        Ok(())
    }
    async fn load_triples(&self) -> Result<Vec<Triple>> {
        Ok(self.read()?.triples.values().cloned().collect())
    }
    async fn save_verbatim(
        &self,
        id: &str,
        content: &str,
        source: &str,
        layer: i32,
        timestamp: &str,
    ) -> Result<()> {
        self.write()?.verbatim.insert(
            id.into(),
            VerbatimEntry {
                id: id.into(),
                content: content.into(),
                source: source.into(),
                layer,
                timestamp: timestamp.into(),
            },
        );
        Ok(())
    }
    async fn load_verbatim_by_id(&self, id: &str) -> Result<Option<VerbatimEntry>> {
        Ok(self.read()?.verbatim.get(id).cloned())
    }
    async fn search_verbatim_by_content(&self, q: &str) -> Result<Vec<VerbatimEntry>> {
        let q = q.trim_matches('%').to_lowercase();
        Ok(self
            .read()?
            .verbatim
            .values()
            .filter(|e| e.content.to_lowercase().contains(&q))
            .cloned()
            .collect())
    }
    async fn list_verbatim_entries(&self) -> Result<Vec<VerbatimEntry>> {
        Ok(self.read()?.verbatim.values().cloned().collect())
    }

    async fn insert_symbol(&self, v: &CodeSymbol) -> Result<()> {
        self.write()?.symbols.insert(v.id.clone(), v.clone());
        Ok(())
    }
    async fn search_symbols(&self, q: &str, limit: usize) -> Result<Vec<CodeSymbol>> {
        let q = q.to_lowercase();
        Ok(self
            .read()?
            .symbols
            .values()
            .filter(|s| {
                s.name.to_lowercase().contains(&q) || s.signature.to_lowercase().contains(&q)
            })
            .take(limit)
            .cloned()
            .collect())
    }
    async fn insert_edge(&self, v: &SymbolEdge) -> Result<()> {
        let mut s = self.write()?;
        if !s.edges.contains(v) {
            s.edges.push(v.clone());
        }
        Ok(())
    }
    async fn get_callers(&self, id: &str) -> Result<Vec<CodeSymbol>> {
        let s = self.read()?;
        Ok(s.edges
            .iter()
            .filter(|e| e.target_id == id)
            .filter_map(|e| s.symbols.get(&e.source_id).cloned())
            .collect())
    }
    async fn get_callees(&self, id: &str) -> Result<Vec<CodeSymbol>> {
        let s = self.read()?;
        Ok(s.edges
            .iter()
            .filter(|e| e.source_id == id)
            .filter_map(|e| s.symbols.get(&e.target_id).cloned())
            .collect())
    }
    async fn list_all_symbols(&self) -> Result<Vec<CodeSymbol>> {
        Ok(self.read()?.symbols.values().cloned().collect())
    }
    async fn list_all_edges(&self) -> Result<Vec<SymbolEdge>> {
        Ok(self.read()?.edges.clone())
    }
    async fn link_symbol_to_memory(
        &self,
        symbol_id: &str,
        memory_id: &MemoryId,
        turn_index: Option<i32>,
        reference_type: &str,
        timestamp: i64,
    ) -> Result<()> {
        let v = SymbolMemoryReference {
            symbol_id: symbol_id.into(),
            memory_id: *memory_id,
            turn_index,
            reference_type: Some(reference_type.into()),
            timestamp,
        };
        let mut s = self.write()?;
        if !s.symbol_memory.contains(&v) {
            s.symbol_memory.push(v);
        }
        Ok(())
    }
    async fn find_memories_by_symbol(&self, name: &str) -> Result<Vec<MemoryId>> {
        let s = self.read()?;
        let name = name.to_ascii_lowercase();
        Ok(s.symbol_memory
            .iter()
            .filter(|reference| reference.symbol_id.to_ascii_lowercase().contains(&name))
            .map(|v| v.memory_id)
            .collect())
    }
    async fn list_symbol_memory_references(&self) -> Result<Vec<SymbolMemoryReference>> {
        Ok(self.read()?.symbol_memory.clone())
    }
    async fn kv_put(&self, k: &str, v: &str) -> Result<()> {
        self.write()?.kv.insert(k.into(), v.into());
        Ok(())
    }
    async fn kv_get(&self, k: &str) -> Result<Option<String>> {
        Ok(self.read()?.kv.get(k).cloned())
    }
    async fn list_key_values(&self) -> Result<Vec<MemoryKeyValue>> {
        Ok(self
            .read()?
            .kv
            .iter()
            .map(|(k, v)| MemoryKeyValue {
                key: k.clone(),
                value: v.clone(),
            })
            .collect())
    }
}
