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
    source_fences: BTreeMap<String, MemorySourceFence>,
    lifecycle_receipts: BTreeMap<String, MemoryLifecycleReceipt>,
    cleanup_pending: BTreeMap<String, MemoryCleanupPending>,
    discovery_revisions: BTreeMap<String, i64>,
    /// PLAN B-8: keyed by `(scope_key, id)`, matching the Postgres store's
    /// scope-partitioned tables. A flat id-keyed map made the in-memory store
    /// MERGE across scopes while the durable store REPLACED per scope, so a test
    /// double and production disagreed about what "save the graph for scope X"
    /// means.
    entities: BTreeMap<(String, String), Entity>,
    triples: BTreeMap<(String, String), Triple>,
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

fn discovery_content(entry: &MemoryEntry) -> serde_json::Value {
    let mut value = serde_json::to_value(entry).expect("serializable memory");
    if let Some(value) = value.as_object_mut() {
        value.remove("access_count");
        value.remove("last_accessed_at");
    }
    value
}
fn bump_discovery(state: &mut State, scope: &MemoryScope) {
    *state
        .discovery_revisions
        .entry(scope.scope_key())
        .or_default() += 1;
}
fn put_discovery_entry(state: &mut State, entry: &MemoryEntry) {
    if let Some(verbatim) = super::entry_verbatim(entry) {
        state.verbatim.insert(verbatim.id.clone(), verbatim);
    } else {
        state.verbatim.remove(&entry.id.to_string());
    }
    let previous = state.entries.insert(entry.id.to_string(), entry.clone());
    if previous
        .as_ref()
        .is_none_or(|old| discovery_content(old) != discovery_content(entry))
    {
        if let Some(old) = previous.filter(|old| old.scope != entry.scope) {
            bump_discovery(state, &old.scope);
        }
        bump_discovery(state, &entry.scope);
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

fn insert_original_entry(state: &mut State, entry: &MemoryEntry) -> Result<MemoryId> {
    if state
        .source_fences
        .get(&format!("memory:{}", entry.id))
        .is_some_and(|f| {
            f.source_ref != format!("memory:{}", entry.id)
                || f.retired
                || f.scope_key != entry.scope.scope_key()
        })
    {
        return Err(MemoryError::Store(
            "retired or foreign Memory source write".into(),
        ));
    }
    if lifecycle_retired(
        entry.id,
        state
            .kv
            .get(&format!("memory_lifecycle:{}", entry.id))
            .map(String::as_str),
    )? {
        return Err(MemoryError::Store(
            "legacy retired Memory source write".into(),
        ));
    }
    if let Some(current) = state.entries.get(&entry.id.to_string()) {
        if MemoryEntryRevision::of(current)? == MemoryEntryRevision::of(entry)? {
            return Ok(entry.id);
        }
        return Err(MemoryError::Store(
            "Memory create target already exists".into(),
        ));
    }
    put_discovery_entry(state, entry);
    Ok(entry.id)
}

fn extraction_binding(state: &State, id: MemoryId) -> Result<Option<MemoryExtractionOrigin>> {
    state
        .kv
        .get(&MemoryExtractionOrigin::binding_key(id))
        .map(|raw| serde_json::from_str(raw).map_err(MemoryError::from))
        .transpose()
}

// Reserve the borrowed durable bytes before parsing an origin carrying readsets.
// Both source cuts and extraction validation share this native budget contract.
fn bounded_extraction_binding(
    state: &State,
    id: MemoryId,
    budget_bytes: usize,
) -> Result<(Option<MemoryExtractionOrigin>, usize)> {
    let Some(raw) = state.kv.get(&MemoryExtractionOrigin::binding_key(id)) else {
        return Ok((None, 0));
    };
    if raw.len() > budget_bytes {
        return Err(MemoryError::SourceReadLimit);
    }
    Ok((Some(serde_json::from_str(raw)?), raw.len()))
}

fn validate_extraction_sources(state: &State, origin: &MemoryExtractionOrigin) -> Result<()> {
    let mut remaining = crate::knowledge::KNOWLEDGE_READ_MAX_BYTES;
    let mut frame = BTreeMap::new();
    for expected in origin.source_readset() {
        if !frame.contains_key(&expected.memory_id) {
            let entry = state
                .entries
                .get(&expected.memory_id.to_string())
                .ok_or(MemoryError::StaleSource)?;
            let (binding, origin_bytes) =
                bounded_extraction_binding(state, expected.memory_id, remaining)?;
            remaining = remaining
                .checked_sub(origin_bytes)
                .ok_or(MemoryError::SourceReadLimit)?;
            if binding
                .as_ref()
                .map(MemoryExtractionOrigin::retirement_ref)
                .transpose()?
                .is_some_and(|key| state.source_fences.contains_key(&key))
            {
                return Err(MemoryError::StaleSource);
            }
            let read = live_source_revision(
                &format!("memory:{}", expected.memory_id),
                entry,
                state
                    .kv
                    .get(&format!("memory_lifecycle:{}", expected.memory_id))
                    .map(String::as_str),
                state
                    .source_fences
                    .get(&format!("memory:{}", expected.memory_id)),
                remaining,
            )?;
            remaining = remaining
                .checked_sub(read.materialized_bytes)
                .ok_or(MemoryError::SourceReadLimit)?;
            frame.insert(expected.memory_id, read);
        }
        let read = &frame[&expected.memory_id];
        if read.fence.retired || read.revision.as_ref() != Some(expected) {
            return Err(MemoryError::StaleSource);
        }
    }
    Ok(())
}

fn validate_extraction_origin(
    state: &State,
    entry: &MemoryEntry,
    origin: Option<&MemoryExtractionOrigin>,
) -> Result<()> {
    let binding = extraction_binding(state, entry.id)?;
    if let Some(origin) = origin {
        origin.validate_entry(entry)?;
        if binding.as_ref().is_some_and(|bound| bound != origin)
            || state.source_fences.contains_key(&origin.retirement_ref()?)
        {
            return Err(MemoryError::StaleSource);
        }
    } else if binding.is_none()
        && entry
            .tags
            .iter()
            .any(|tag| tag.starts_with("extraction-batch:"))
    {
        return Err(MemoryError::StaleSource);
    }
    if let Some(bound) = binding {
        if state.source_fences.contains_key(&bound.retirement_ref()?) {
            return Err(MemoryError::StaleSource);
        }
    }
    Ok(())
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
        let mut state = self.write()?;
        validate_extraction_origin(&state, entry, None)?;
        insert_original_entry(&mut state, entry)
    }
    async fn write_extraction(
        &self,
        entry: &MemoryEntry,
        origin: &MemoryExtractionOrigin,
        expected: Option<&MemoryEntryRevision>,
    ) -> Result<MemoryId> {
        let mut state = self.write()?;
        validate_extraction_origin(&state, entry, Some(origin))?;
        validate_extraction_sources(&state, origin)?;
        if state.entries.contains_key(&entry.id.to_string())
            && extraction_binding(&state, entry.id)?.is_none()
        {
            return Err(MemoryError::StaleSource);
        }
        if let Some(expected) = expected {
            let current = state
                .entries
                .get(&entry.id.to_string())
                .ok_or(MemoryError::StaleSource)?;
            if extraction_binding(&state, entry.id)?.as_ref() != Some(origin) {
                return Err(MemoryError::StaleSource);
            }
            if lifecycle_retired(
                entry.id,
                state
                    .kv
                    .get(&format!("memory_lifecycle:{}", entry.id))
                    .map(String::as_str),
            )? {
                return Err(MemoryError::StaleSource);
            }
            let next = expected.validate_replace(current, entry)?;
            put_discovery_entry(&mut state, &next);
        } else {
            insert_original_entry(&mut state, entry)?;
        }
        state.kv.insert(
            MemoryExtractionOrigin::binding_key(entry.id),
            serde_json::to_string(origin)?,
        );
        Ok(entry.id)
    }
    async fn get(&self, id: &MemoryId) -> Result<Option<MemoryEntry>> {
        Ok(self.read()?.entries.get(&id.to_string()).cloned())
    }
    async fn update(&self, entry: &MemoryEntry, expected: &MemoryEntryRevision) -> Result<()> {
        let mut state = self.write()?;
        validate_extraction_origin(&state, entry, None)?;
        if state
            .source_fences
            .get(&format!("memory:{}", entry.id))
            .is_some_and(|f| {
                f.source_ref != format!("memory:{}", entry.id)
                    || f.retired
                    || f.scope_key != entry.scope.scope_key()
            })
        {
            return Err(MemoryError::Store(
                "retired or foreign Memory source write".into(),
            ));
        }
        if !state.entries.contains_key(&entry.id.to_string()) {
            return Err(MemoryError::NotFound(entry.id.to_string()));
        }
        if lifecycle_retired(
            entry.id,
            state
                .kv
                .get(&format!("memory_lifecycle:{}", entry.id))
                .map(String::as_str),
        )? {
            return Err(MemoryError::Store(
                "legacy retired Memory source write".into(),
            ));
        }
        let current = state
            .entries
            .get(&entry.id.to_string())
            .ok_or_else(|| MemoryError::NotFound(entry.id.to_string()))?;
        let entry = expected.validate_replace(current, entry)?;
        put_discovery_entry(&mut state, &entry);
        Ok(())
    }
    async fn delete(&self, id: &MemoryId) -> Result<()> {
        let mut state = self.write()?;
        if let Some(entry) = state.entries.remove(&id.to_string()) {
            bump_discovery(&mut state, &entry.scope);
        }
        state.verbatim.remove(&id.to_string());
        Ok(())
    }

    async fn commit_lifecycle(
        &self,
        command: MemoryLifecycleMutation,
    ) -> Result<MemoryLifecycleReceipt> {
        let mut state = self.write()?;
        if let Some(receipt) = state.lifecycle_receipts.get(&command.mutation_id) {
            command.validate_replay(receipt)?;
            return Ok(receipt.clone());
        }
        let entry = state
            .entries
            .get(&command.event.memory_id.to_string())
            .ok_or_else(|| MemoryError::NotFound(command.event.memory_id.to_string()))?
            .clone();
        let key = format!("memory_lifecycle:{}", entry.id);
        let source_ref = format!("memory:{}", entry.id);
        let (raw, receipt, pending) = command.validate_cut(
            &entry,
            state.kv.get(&key).map(String::as_str),
            state.source_fences.get(&source_ref),
        )?;
        if receipt.source.retired && command.revokes_origin() {
            if let Some(origin) = extraction_binding(&state, entry.id)? {
                let fence = origin.retirement_fence(&receipt.source)?;
                state
                    .source_fences
                    .entry(fence.source_ref.clone())
                    .or_insert(fence);
            }
        }
        state.kv.insert(key, raw);
        bump_discovery(&mut state, &entry.scope);
        state
            .source_fences
            .insert(source_ref, receipt.source.clone());
        if let Some(pending) = pending {
            state
                .cleanup_pending
                .insert(command.mutation_id.clone(), pending);
        }
        state
            .lifecycle_receipts
            .insert(command.mutation_id, receipt.clone());
        Ok(receipt)
    }

    async fn lifecycle_receipt(&self, mutation_id: &str) -> Result<Option<MemoryLifecycleReceipt>> {
        Ok(self.read()?.lifecycle_receipts.get(mutation_id).cloned())
    }

    async fn pending_memory_cleanup(
        &self,
        after_mutation: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MemoryCleanupPending>> {
        if limit == 0 || limit > 256 {
            return Err(MemoryError::Store(
                "invalid Memory cleanup page size".into(),
            ));
        }
        let state = self.read()?;
        let mut page = Vec::new();
        for pending in state
            .cleanup_pending
            .values()
            .filter(|p| after_mutation.is_none_or(|after| p.mutation_id.as_str() > after))
        {
            pending.validate()?;
            if pending.knowledge_receipt_digest.is_some() && pending.auxiliary_confirmed {
                continue;
            }
            page.push(pending.clone());
            if page.len() == limit {
                break;
            }
        }
        Ok(page)
    }

    async fn confirm_memory_cleanup(
        &self,
        expected: &MemoryCleanupPending,
        knowledge: Option<&crate::knowledge::MemorySourceQuarantineReceipt>,
        auxiliary: bool,
    ) -> Result<()> {
        let mut state = self.write()?;
        let pending = state
            .cleanup_pending
            .get_mut(&expected.mutation_id)
            .ok_or_else(|| MemoryError::NotFound(expected.mutation_id.clone()))?;
        if pending.source != expected.source || pending.memory_id != expected.memory_id {
            return Err(MemoryError::Store(
                "foreign Memory cleanup confirmation".into(),
            ));
        }
        pending.confirm_knowledge(knowledge)?;
        pending.auxiliary_confirmed |= auxiliary;
        if auxiliary {
            let id = expected.memory_id.to_string();
            let reference = expected.source.source_ref.as_str();
            state
                .symbol_memory
                .retain(|r| r.memory_id != expected.memory_id);
            state.entities.retain(|(scope, _), entity| {
                if scope != &expected.source.scope_key {
                    return true;
                }
                let matched = entity.source_ids.iter().any(|s| s == &id || s == reference);
                entity.source_ids.retain(|s| s != &id && s != reference);
                !matched || !entity.source_ids.is_empty()
            });
            state.triples.retain(|(scope, _), triple| {
                scope != &expected.source.scope_key
                    || !triple
                        .source
                        .as_deref()
                        .is_some_and(|s| s == id || s == reference)
            });
        }
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
    async fn discover_page(&self, query: MemoryDiscoveryQuery) -> Result<MemoryDiscoveryPage> {
        let state = self.read()?;
        let scopes = query
            .scopes
            .iter()
            .map(MemoryScope::scope_key)
            .collect::<std::collections::BTreeSet<_>>();
        let revisions = scopes
            .iter()
            .map(|scope| {
                (
                    scope.clone(),
                    *state.discovery_revisions.get(scope).unwrap_or(&0),
                )
            })
            .collect::<BTreeMap<_, _>>();
        if query
            .expected_revisions
            .as_ref()
            .is_some_and(|expected| expected != &revisions)
        {
            return Err(MemoryError::Store(
                "memory discovery source changed; start a new search".into(),
            ));
        }
        let limit = query.limit.clamp(1, 128);
        let mut entries = state
            .entries
            .values()
            .filter(|entry| {
                scopes.contains(&entry.scope.scope_key())
                    && query
                        .after_id
                        .as_ref()
                        .is_none_or(|after| entry.id.to_string() > *after)
                    && matches_text(entry, &query.query)
            })
            .skip(query.skip)
            .take(limit + 1)
            .cloned()
            .collect::<Vec<_>>();
        let has_more = entries.len() > limit;
        entries.truncate(limit);
        let next_id = has_more.then(|| entries.last().expect("nonempty page").id.to_string());
        let lifecycle = entries
            .iter()
            .filter_map(|entry| {
                let key = format!("memory_lifecycle:{}", entry.id);
                state.kv.get(&key).map(|value| MemoryKeyValue {
                    key,
                    value: value.clone(),
                })
            })
            .collect();
        Ok(MemoryDiscoveryPage {
            entries,
            lifecycle,
            revisions,
            next_id,
        })
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
                .filter(|e| {
                    e.scope == query.scope
                        && same_memory_key(e) == query.fingerprint
                        && e.visibility == query.visibility
                        && (!matches!(query.visibility, crate::types::AgentVisibility::Private)
                            || (query.source_agent.is_some()
                                && e.source_agent == query.source_agent))
                })
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

    async fn save_entities(&self, scope_key: &str, values: &[Entity]) -> Result<()> {
        let mut s = self.write()?;
        // Scope-keyed REPLACE, matching the Postgres store: saving the graph for a
        // scope replaces that scope's set rather than merging into every scope.
        s.entities.retain(|(scope, _), _| scope != scope_key);
        for v in values {
            s.entities
                .insert((scope_key.to_string(), v.id.clone()), v.clone());
        }
        Ok(())
    }
    async fn load_entities(&self, scope_key: &str) -> Result<Vec<Entity>> {
        Ok(self
            .read()?
            .entities
            .iter()
            .filter(|((scope, _), _)| scope == scope_key)
            .map(|(_, entity)| entity.clone())
            .collect())
    }
    async fn save_triples(&self, scope_key: &str, values: &[Triple]) -> Result<()> {
        let mut s = self.write()?;
        s.triples.retain(|(scope, _), _| scope != scope_key);
        for v in values {
            s.triples
                .insert((scope_key.to_string(), v.id.clone()), v.clone());
        }
        Ok(())
    }
    async fn load_triples(&self, scope_key: &str) -> Result<Vec<Triple>> {
        Ok(self
            .read()?
            .triples
            .iter()
            .filter(|((scope, _), _)| scope == scope_key)
            .map(|(_, triple)| triple.clone())
            .collect())
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
        if k.starts_with("memory_extraction_origin:") {
            return Err(MemoryError::StaleSource);
        }
        let mut state = self.write()?;
        if k.strip_prefix("memory_lifecycle:")
            .is_some_and(|id| state.source_fences.contains_key(&format!("memory:{id}")))
        {
            return Err(MemoryError::Store(
                "owned Memory lifecycle requires typed transaction".into(),
            ));
        }
        let previous = state.kv.insert(k.into(), v.into());
        if previous.as_deref() != Some(v) {
            if let Some(scope) = k
                .strip_prefix("memory_lifecycle:")
                .and_then(|id| state.entries.get(id))
                .map(|entry| entry.scope.clone())
            {
                bump_discovery(&mut state, &scope);
            }
        }
        Ok(())
    }
    async fn kv_get(&self, k: &str) -> Result<Option<String>> {
        Ok(self.read()?.kv.get(k).cloned())
    }
    async fn kv_delete(&self, key: &str) -> Result<()> {
        if key.starts_with("memory_extraction_origin:") {
            return Err(MemoryError::StaleSource);
        }
        let mut state = self.write()?;
        if key
            .strip_prefix("memory_lifecycle:")
            .is_some_and(|id| state.source_fences.contains_key(&format!("memory:{id}")))
        {
            return Err(MemoryError::Store(
                "owned Memory lifecycle requires typed transaction".into(),
            ));
        }
        if state.kv.remove(key).is_some() {
            if let Some(scope) = key
                .strip_prefix("memory_lifecycle:")
                .and_then(|id| state.entries.get(id))
                .map(|entry| entry.scope.clone())
            {
                bump_discovery(&mut state, &scope);
            }
        }
        Ok(())
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

#[cfg(test)]
mod scope_tests {
    use super::*;

    fn entity(id: &str) -> Entity {
        let now = chrono::Utc::now();
        Entity {
            id: id.to_string(),
            name: id.to_string(),
            entity_type: crate::entity::EntityType::Concept,
            confidence: 1.0,
            frequency: 1,
            first_seen: now,
            last_seen: now,
            source_ids: Vec::new(),
            source_type: "test".to_string(),
        }
    }

    /// PLAN B-8: the in-memory store must mean the same thing by "save the graph
    /// for scope X" as the durable store - a scope-keyed REPLACE, not a global
    /// merge. Before this, a test double and production disagreed.
    #[tokio::test]
    async fn saving_a_scope_replaces_only_that_scope() {
        let store = EphemeralMemoryStore::new();
        store
            .save_entities("scope-a", &[entity("a1"), entity("a2")])
            .await
            .expect("save a");
        store
            .save_entities("scope-b", &[entity("b1")])
            .await
            .expect("save b");

        let mut a = store
            .load_entities("scope-a")
            .await
            .expect("load a")
            .into_iter()
            .map(|entity| entity.id)
            .collect::<Vec<_>>();
        a.sort();
        assert_eq!(a, vec!["a1".to_string(), "a2".to_string()]);
        assert_eq!(
            store
                .load_entities("scope-b")
                .await
                .expect("load b")
                .into_iter()
                .map(|entity| entity.id)
                .collect::<Vec<_>>(),
            vec!["b1".to_string()],
            "scope-b must be untouched by scope-a"
        );

        // Re-saving scope-a REPLACES it, and never leaks into scope-b.
        store
            .save_entities("scope-a", &[entity("a3")])
            .await
            .expect("replace a");
        assert_eq!(
            store
                .load_entities("scope-a")
                .await
                .expect("load a")
                .into_iter()
                .map(|entity| entity.id)
                .collect::<Vec<_>>(),
            vec!["a3".to_string()],
            "saving a scope replaces that scope's set"
        );
        assert_eq!(
            store.load_entities("scope-b").await.expect("load b").len(),
            1
        );
    }
}

/// Keeps the original Memory read guard alive across the dependent in-memory
/// Knowledge commit. It owns no writable projection or duplicated source state.
pub(crate) struct MemorySourceRevisionGuard<'a> {
    _state: std::sync::RwLockReadGuard<'a, State>,
    pub read: MemorySourceRevisionRead,
}
impl EphemeralMemoryStore {
    pub(crate) fn source_revision_guard(
        &self,
        source_ref: &str,
        budget_bytes: usize,
    ) -> Result<Option<MemorySourceRevisionGuard<'_>>> {
        let state = self.read()?;
        let id = source_ref
            .strip_prefix("memory:")
            .and_then(|id| uuid::Uuid::parse_str(id).ok())
            .filter(|id| format!("memory:{id}") == source_ref)
            .ok_or_else(|| MemoryError::Store("invalid Memory source identity".into()))?;
        if let Some(fence) = state
            .source_fences
            .get(source_ref)
            .filter(|fence| fence.retired)
        {
            let bytes = fence.read_bytes(budget_bytes)?;
            if fence.source_ref != source_ref
                || state
                    .entries
                    .get(&id.to_string())
                    .is_some_and(|entry| entry.scope.scope_key() != fence.scope_key)
            {
                return Err(MemoryError::StaleSource);
            }
            let read = MemorySourceRevisionRead {
                origin_retired: false,
                access: None,
                packet_content_digest: None,
                packet_atom: None,
                fence: fence.clone(),
                revision: None,
                materialized_bytes: bytes,
            };
            return Ok(Some(MemorySourceRevisionGuard {
                _state: state,
                read,
            }));
        }
        let Some(entry) = state.entries.get(&id.to_string()) else {
            if state.source_fences.contains_key(source_ref) {
                return Err(MemoryError::Store("live Memory source body missing".into()));
            }
            return Ok(None);
        };
        let (origin, origin_bytes) = bounded_extraction_binding(&state, id, budget_bytes)?;
        let origin_retired = origin
            .as_ref()
            .map(MemoryExtractionOrigin::retirement_ref)
            .transpose()?
            .is_some_and(|key| state.source_fences.contains_key(&key));
        let mut read = live_source_revision(
            source_ref,
            entry,
            state
                .kv
                .get(&format!("memory_lifecycle:{id}"))
                .map(String::as_str),
            state.source_fences.get(source_ref),
            budget_bytes
                .checked_sub(origin_bytes)
                .ok_or(MemoryError::SourceReadLimit)?,
        )?;
        read.materialized_bytes += origin_bytes;
        read.origin_retired = origin_retired;
        if origin_retired {
            read.revision = None;
            read.access = None;
        }
        Ok(Some(MemorySourceRevisionGuard {
            _state: state,
            read,
        }))
    }
}

impl MemorySourceFenceReader for EphemeralMemoryStore {
    fn read_source_revision(
        &self,
        source_ref: &str,
        budget_bytes: usize,
    ) -> Result<Option<MemorySourceRevisionRead>> {
        Ok(self
            .source_revision_guard(source_ref, budget_bytes)?
            .map(|guard| guard.read.clone()))
    }

    fn read_source_fence(&self, source_ref: &str) -> Result<Option<MemorySourceFence>> {
        let parsed = source_ref
            .strip_prefix("memory:")
            .and_then(|id| uuid::Uuid::parse_str(id).ok())
            .ok_or_else(|| MemoryError::Store("invalid Memory source identity".into()))?;
        if format!("memory:{parsed}") != source_ref {
            return Err(MemoryError::Store(
                "noncanonical Memory source identity".into(),
            ));
        }
        let state = self.read()?;
        let history = if state.source_fences.contains_key(source_ref) {
            None
        } else {
            state.kv.get(&format!("memory_lifecycle:{parsed}"))
        };
        reserved_source_bytes(
            &(
                &state
                    .entries
                    .get(&parsed.to_string())
                    .map(|entry| &entry.scope),
                &state.source_fences.get(source_ref),
                history,
            ),
            crate::knowledge::KNOWLEDGE_READ_MAX_BYTES,
        )?;
        if let Some(fence) = state.source_fences.get(source_ref) {
            if fence.source_ref != source_ref
                || source_ref
                    .strip_prefix("memory:")
                    .and_then(|id| state.entries.get(id))
                    .is_some_and(|entry| entry.scope.scope_key() != fence.scope_key)
            {
                return Err(MemoryError::Store(
                    "Memory source fence identity mismatch".into(),
                ));
            }
            if !fence.retired
                && source_ref
                    .strip_prefix("memory:")
                    .is_none_or(|id| !state.entries.contains_key(id))
            {
                return Err(MemoryError::Store("live Memory source body missing".into()));
            }
            return Ok(Some(fence.clone()));
        }
        let Some(id) = source_ref.strip_prefix("memory:") else {
            return Err(MemoryError::Store("not a Memory source reference".into()));
        };
        let Some(entry) = state.entries.get(id) else {
            return Ok(None);
        };
        let history = state.kv.get(&format!("memory_lifecycle:{id}"));
        if history.is_some_and(|raw| raw.len() > crate::knowledge::KNOWLEDGE_READ_MAX_BYTES) {
            return Err(MemoryError::SourceReadLimit);
        }
        Ok(Some(MemorySourceFence {
            source_ref: source_ref.into(),
            scope_key: entry.scope.scope_key(),
            delete_epoch: 0,
            retired: lifecycle_retired(entry.id, history.map(String::as_str))?,
        }))
    }
}

#[cfg(test)]
mod extraction_source_budget_tests {
    use super::*;
    #[test]
    fn origin_bytes_are_reserved_before_parse_and_share_the_source_budget() {
        let mut state = State::default();
        let id = uuid::Uuid::from_u128(1);
        let key = MemoryExtractionOrigin::binding_key(id);
        state.kv.insert(key.clone(), "not-json".repeat(128));
        assert!(
            matches!(
                bounded_extraction_binding(&state, id, 64),
                Err(MemoryError::SourceReadLimit)
            ),
            "oversize borrowed origin must be refused before deserialization"
        );
        let raw = r#"{"scope_key":"project:a","session_id":"s","agent_id":"a","batch_tag":"extraction-batch:00000000-0000-0000-0000-000000000001"}"#;
        state.kv.insert(key, raw.into());
        let (origin, used) = bounded_extraction_binding(&state, id, raw.len() + 8).unwrap();
        assert!(origin.is_some());
        assert_eq!(used, raw.len());
        assert!(
            matches!(
                bounded_extraction_binding(&state, id, 8),
                Err(MemoryError::SourceReadLimit)
            ),
            "another source must not parse an origin exceeding its remaining shared budget"
        );
    }
}
