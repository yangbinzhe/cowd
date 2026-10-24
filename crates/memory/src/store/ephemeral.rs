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

fn read_single_memory_source(
    state: &State,
    source_ref: &str,
    budget_bytes: usize,
    origin: Option<&MemoryExtractionOrigin>,
) -> Result<Option<MemorySourceRevisionRead>> {
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
            legacy_provenance_unknown: false,
            origin_retired: false,
            access: None,
            packet_content_digest: None,
            packet_atom: None,
            fence: fence.clone(),
            revision: None,
            materialized_bytes: bytes,
        };
        return Ok(Some(read));
    }
    let Some(entry) = state.entries.get(&id.to_string()) else {
        if state.source_fences.contains_key(source_ref) {
            return Err(MemoryError::Store("live Memory source body missing".into()));
        }
        return Ok(None);
    };
    let origin_bytes = 0;
    match origin {
        Some(origin) => origin.validate_entry(entry)?,
        None => {}
    }
    let origin_retired = origin
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
    read.legacy_provenance_unknown =
        origin.is_none() && entry.source == crate::MemorySource::AutoExtracted;
    read.origin_retired = origin_retired;
    if origin_retired {
        read.revision = None;
        read.access = None;
    }
    Ok(Some(read))
}

// One original State guard, one bounded frame. Original origin bindings are
// immutable; their captured ancestor cuts must never be replaced by fresh heads.
fn read_memory_source_frame(
    state: &State,
    roots: &[MemoryId],
    budget_bytes: usize,
) -> Result<(
    BTreeMap<MemoryId, Option<MemorySourceRevisionRead>>,
    bool,
    usize,
)> {
    let mut remaining = budget_bytes;
    let mut origins = BTreeMap::new();
    let mut todo = roots.to_vec();
    while let Some(id) = todo.pop() {
        if origins.contains_key(&id) {
            continue;
        }
        if origins.len() >= crate::knowledge::KNOWLEDGE_READ_MAX_ROWS {
            return Err(MemoryError::SourceReadLimit);
        }
        remaining = remaining
            .checked_sub(16)
            .ok_or(MemoryError::SourceReadLimit)?;
        // A known irreversible retirement is sufficient to exclude this source.
        // Its old ancestors must not be hydrated just to rediscover that fact.
        if state
            .source_fences
            .get(&format!("memory:{id}"))
            .is_some_and(|fence| fence.retired)
        {
            origins.insert(id, None);
            continue;
        }
        let (origin, bytes) = bounded_extraction_binding(state, id, remaining)?;
        remaining = remaining
            .checked_sub(bytes)
            .ok_or(MemoryError::SourceReadLimit)?;
        if let Some(origin) = &origin {
            todo.extend(origin.source_readset().iter().map(|cut| cut.memory_id));
        }
        origins.insert(id, origin);
    }
    let mut frame = BTreeMap::new();
    for (id, origin) in &origins {
        let read =
            read_single_memory_source(state, &format!("memory:{id}"), remaining, origin.as_ref())?;
        if let Some(read) = &read {
            remaining = remaining
                .checked_sub(read.materialized_bytes)
                .ok_or(MemoryError::SourceReadLimit)?;
        }
        frame.insert(*id, read);
    }
    let valid = super::mutation::source_frame_matches_original_lineage(&origins, &frame);
    let classifications = roots
        .iter()
        .filter(|id| frame.get(id).and_then(Option::as_ref).is_some())
        .map(|id| {
            super::source_root_eligibility(*id, &origins, &frame).map(|current| (*id, current))
        })
        .collect::<Result<Vec<_>>>()?;
    for (id, eligibility) in classifications {
        if eligibility != super::MemorySourceEligibility::Current {
            let read = frame
                .get_mut(&id)
                .and_then(Option::as_mut)
                .ok_or(MemoryError::StaleSource)?;
            read.origin_retired = eligibility == super::MemorySourceEligibility::Obsolete;
            read.legacy_provenance_unknown =
                eligibility == super::MemorySourceEligibility::LegacyUnknown;
            read.revision = None;
            read.access = None;
        }
    }
    Ok((frame, valid, budget_bytes - remaining))
}

fn validate_extraction_sources(state: &State, origin: &MemoryExtractionOrigin) -> Result<()> {
    let roots = origin
        .source_readset()
        .iter()
        .map(|cut| cut.memory_id)
        .collect::<Vec<_>>();
    let (frame, valid, _) =
        read_memory_source_frame(state, &roots, crate::knowledge::KNOWLEDGE_READ_MAX_BYTES)?;
    if !valid {
        return Err(MemoryError::StaleSource);
    }
    for expected in origin.source_readset() {
        let read = frame
            .get(&expected.memory_id)
            .and_then(Option::as_ref)
            .ok_or(MemoryError::StaleSource)?;
        if read.fence.retired || read.origin_retired || read.revision.as_ref() != Some(expected) {
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
            .any(|tag| tag.starts_with("extraction-batch:") || tag.starts_with("memory-turn:"))
    {
        return Err(MemoryError::StaleSource);
    }
    if let Some(bound) = binding {
        bound.validate_entry(entry)?;
        if state.source_fences.contains_key(&bound.retirement_ref()?) {
            return Err(MemoryError::StaleSource);
        }
    }
    Ok(())
}

fn prepare_auxiliary_replacement(
    state: &State,
    scope: &str,
    old_entities: &[Entity],
    old_triples: &[Triple],
    entities: &[Entity],
    triples: &[Triple],
) -> Result<(Vec<Entity>, Vec<Triple>)> {
    let bytes = super::reserved_source_bytes(
        &(old_entities, old_triples, entities, triples),
        crate::knowledge::KNOWLEDGE_READ_MAX_BYTES,
    )?;
    let old = super::auxiliary_source_cuts(old_entities, old_triples)?;
    let new = super::auxiliary_source_cuts(entities, triples)?;
    let roots = old
        .keys()
        .chain(new.keys())
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let (frame, _, _) = read_memory_source_frame(
        state,
        &roots.into_iter().collect::<Vec<_>>(),
        crate::knowledge::KNOWLEDGE_READ_MAX_BYTES - bytes,
    )?;
    let kept = super::filter_auxiliary_source_frame(scope, entities, triples, &frame)?;
    if super::auxiliary_source_cuts(&kept.0, &kept.1)? != new {
        return Err(MemoryError::StaleSource);
    }
    // Validate old scope/identity too. Only the specifically classified legacy
    // unknown may be held; foreign, absent and malformed sources remain errors.
    let active_old =
        super::filter_auxiliary_source_frame(scope, old_entities, old_triples, &frame)?;
    let held = old
        .keys()
        .filter(|id| {
            frame
                .get(id)
                .and_then(Option::as_ref)
                .is_some_and(|read| read.legacy_provenance_unknown)
        })
        .copied()
        .collect();
    let retained = super::retain_held_auxiliary(
        old_entities,
        old_triples,
        &active_old.0,
        entities,
        triples,
        &held,
    )?;
    super::validate_auxiliary_replacement(old_entities, old_triples, &retained.0, &retained.1)?;
    Ok(retained)
}

fn auxiliary_at_state(
    state: &State,
    scope_key: &str,
    entities: &[Entity],
    triples: &[Triple],
) -> Result<(Vec<Entity>, Vec<Triple>)> {
    let cuts = super::auxiliary_source_cuts(entities, triples)?;
    let bytes = super::reserved_source_bytes(
        &(entities, triples),
        crate::knowledge::KNOWLEDGE_READ_MAX_BYTES,
    )?;
    let (frame, _valid, _) = read_memory_source_frame(
        state,
        &cuts.keys().copied().collect::<Vec<_>>(),
        crate::knowledge::KNOWLEDGE_READ_MAX_BYTES - bytes,
    )?;
    super::filter_auxiliary_source_frame(scope_key, entities, triples, &frame)
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
        if command.erases_body() {
            state.entries.remove(&entry.id.to_string());
            state.verbatim.remove(&entry.id.to_string());
        }
        state
            .lifecycle_receipts
            .insert(command.mutation_id, receipt.clone());
        Ok(receipt)
    }

    async fn lifecycle_receipt(&self, mutation_id: &str) -> Result<Option<MemoryLifecycleReceipt>> {
        Ok(self.read()?.lifecycle_receipts.get(mutation_id).cloned())
    }

    async fn legacy_lifecycle_page(
        &self,
        after_entry_id: Option<&str>,
        limit: usize,
    ) -> Result<MemoryLegacyLifecyclePage> {
        if limit == 0 || limit > 256 {
            return Err(MemoryError::SourceReadLimit);
        }
        let state = self.read()?;
        let mut remaining = crate::knowledge::KNOWLEDGE_READ_MAX_BYTES;
        let mut page = MemoryLegacyLifecyclePage::default();
        let mut scanned = 0;
        for (id, entry) in state.entries.iter().filter(|(id, _)| {
            after_entry_id.is_none_or(|after| id.as_str() > after)
                && !state.source_fences.contains_key(&format!("memory:{id}"))
        }) {
            let Some(raw) = state.kv.get(&format!("memory_lifecycle:{id}")) else {
                continue;
            };
            if entry.id.to_string() != *id {
                return Err(MemoryError::StaleSource);
            }
            match reserved_source_bytes(&(entry, raw), remaining) {
                Ok(bytes) => {
                    remaining -= bytes;
                    page.entries.push((entry.clone(), raw.clone()));
                }
                Err(MemoryError::SourceReadLimit) if page.entries.is_empty() => {
                    page.unavailable_ids.push(id.clone());
                }
                Err(MemoryError::SourceReadLimit) => break,
                Err(error) => return Err(error),
            }
            page.after_id = Some(id.clone());
            scanned += 1;
            if scanned == limit {
                break;
            }
        }
        Ok(page)
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

    async fn memory_cleanup(&self, mutation_id: &str) -> Result<Option<MemoryCleanupPending>> {
        if mutation_id.is_empty() || mutation_id.len() > 256 {
            return Err(MemoryError::StaleSource);
        }
        let state = self.read()?;
        let Some(pending) = state.cleanup_pending.get(mutation_id) else {
            return Ok(None);
        };
        let receipt = state
            .lifecycle_receipts
            .get(mutation_id)
            .ok_or(MemoryError::StaleSource)?;
        reserved_source_bytes(
            &(pending, receipt),
            crate::knowledge::KNOWLEDGE_READ_MAX_BYTES,
        )?;
        pending.validate_receipt(receipt)?;
        if pending.mutation_id != mutation_id {
            return Err(MemoryError::StaleSource);
        }
        Ok(Some(pending.clone()))
    }

    async fn confirm_memory_cleanup(
        &self,
        expected: &MemoryCleanupPending,
        knowledge: Option<&crate::knowledge::MemorySourceQuarantineReceipt>,
        auxiliary: bool,
    ) -> Result<()> {
        let mut state = self.write()?;
        let mut pending = state
            .cleanup_pending
            .get(&expected.mutation_id)
            .ok_or_else(|| MemoryError::NotFound(expected.mutation_id.clone()))?
            .clone();
        pending.validate()?;
        if pending.source != expected.source || pending.memory_id != expected.memory_id {
            return Err(MemoryError::StaleSource);
        }
        state
            .source_fences
            .get(&expected.source.source_ref)
            .ok_or(MemoryError::StaleSource)?
            .verifies_retirement(&expected.source)?;
        pending.confirm_knowledge(knowledge)?;
        pending.auxiliary_confirmed |= auxiliary;
        if auxiliary {
            let entities = state
                .entities
                .iter()
                .filter(|((scope, _), _)| scope == &expected.source.scope_key)
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            let triples = state
                .triples
                .iter()
                .filter(|((scope, _), _)| scope == &expected.source.scope_key)
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            super::reserved_source_bytes(
                &(&entities, &triples),
                crate::knowledge::KNOWLEDGE_READ_MAX_BYTES,
            )?;
            let (kept_entities, kept_triples) = super::retire_auxiliary_support(
                &expected.source,
                &entities.into_iter().cloned().collect::<Vec<_>>(),
                &triples.into_iter().cloned().collect::<Vec<_>>(),
            )?;
            state
                .symbol_memory
                .retain(|row| row.memory_id != expected.memory_id);
            state
                .entities
                .retain(|(scope, _), _| scope != &expected.source.scope_key);
            state
                .triples
                .retain(|(scope, _), _| scope != &expected.source.scope_key);
            for value in kept_entities {
                state
                    .entities
                    .insert((expected.source.scope_key.clone(), value.id.clone()), value);
            }
            for value in kept_triples {
                state
                    .triples
                    .insert((expected.source.scope_key.clone(), value.id.clone()), value);
            }
        }
        state
            .cleanup_pending
            .insert(pending.mutation_id.clone(), pending);
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
        let mut state = self.write()?;
        let old = state
            .entities
            .iter()
            .filter(|((scope, _), _)| scope == scope_key)
            .map(|(_, v)| v)
            .collect::<Vec<_>>();
        super::reserved_source_bytes(&old, crate::knowledge::KNOWLEDGE_READ_MAX_BYTES)?;
        let old = old.into_iter().cloned().collect::<Vec<_>>();
        let (values, _) = prepare_auxiliary_replacement(&state, scope_key, &old, &[], values, &[])?;
        state.entities.retain(|(scope, _), _| scope != scope_key);
        for value in values {
            state
                .entities
                .insert((scope_key.to_owned(), value.id.clone()), value);
        }
        Ok(())
    }
    async fn load_entities(&self, scope_key: &str) -> Result<Vec<Entity>> {
        let state = self.read()?;
        let refs = state
            .entities
            .iter()
            .filter(|((scope, _), _)| scope == scope_key)
            .map(|(_, value)| value)
            .take(crate::knowledge::KNOWLEDGE_READ_MAX_ROWS + 1)
            .collect::<Vec<_>>();
        if refs.len() > crate::knowledge::KNOWLEDGE_READ_MAX_ROWS {
            return Err(MemoryError::SourceReadLimit);
        }
        super::reserved_source_bytes(&refs, crate::knowledge::KNOWLEDGE_READ_MAX_BYTES)?;
        let values = refs.into_iter().cloned().collect::<Vec<_>>();
        auxiliary_at_state(&state, scope_key, &values, &[]).map(|(entities, _)| entities)
    }
    async fn save_triples(&self, scope_key: &str, values: &[Triple]) -> Result<()> {
        let mut state = self.write()?;
        let old = state
            .triples
            .iter()
            .filter(|((scope, _), _)| scope == scope_key)
            .map(|(_, v)| v)
            .collect::<Vec<_>>();
        super::reserved_source_bytes(&old, crate::knowledge::KNOWLEDGE_READ_MAX_BYTES)?;
        let old = old.into_iter().cloned().collect::<Vec<_>>();
        let (_, values) = prepare_auxiliary_replacement(&state, scope_key, &[], &old, &[], values)?;
        state.triples.retain(|(scope, _), _| scope != scope_key);
        for value in values {
            state
                .triples
                .insert((scope_key.to_owned(), value.id.clone()), value);
        }
        Ok(())
    }
    async fn save_knowledge_graph(
        &self,
        scope_key: &str,
        entities: &[Entity],
        triples: &[Triple],
    ) -> Result<()> {
        if entities.is_empty() && triples.is_empty() {
            return Err(MemoryError::StaleSource);
        }
        let mut state = self.write()?;
        let old_entities = state
            .entities
            .iter()
            .filter(|((scope, _), _)| scope == scope_key)
            .map(|(_, v)| v)
            .collect::<Vec<_>>();
        let old_triples = state
            .triples
            .iter()
            .filter(|((scope, _), _)| scope == scope_key)
            .map(|(_, v)| v)
            .collect::<Vec<_>>();
        super::reserved_source_bytes(
            &(&old_entities, &old_triples),
            crate::knowledge::KNOWLEDGE_READ_MAX_BYTES,
        )?;
        let old_entities = old_entities.into_iter().cloned().collect::<Vec<_>>();
        let old_triples = old_triples.into_iter().cloned().collect::<Vec<_>>();
        let (entities, triples) = prepare_auxiliary_replacement(
            &state,
            scope_key,
            &old_entities,
            &old_triples,
            entities,
            triples,
        )?;
        state.entities.retain(|(scope, _), _| scope != scope_key);
        state.triples.retain(|(scope, _), _| scope != scope_key);
        for value in entities {
            state
                .entities
                .insert((scope_key.to_owned(), value.id.clone()), value);
        }
        for value in triples {
            state
                .triples
                .insert((scope_key.to_owned(), value.id.clone()), value);
        }
        Ok(())
    }
    async fn load_triples(&self, scope_key: &str) -> Result<Vec<Triple>> {
        let state = self.read()?;
        let refs = state
            .triples
            .iter()
            .filter(|((scope, _), _)| scope == scope_key)
            .map(|(_, value)| value)
            .take(crate::knowledge::KNOWLEDGE_READ_MAX_ROWS + 1)
            .collect::<Vec<_>>();
        if refs.len() > crate::knowledge::KNOWLEDGE_READ_MAX_ROWS {
            return Err(MemoryError::SourceReadLimit);
        }
        super::reserved_source_bytes(&refs, crate::knowledge::KNOWLEDGE_READ_MAX_BYTES)?;
        let values = refs.into_iter().cloned().collect::<Vec<_>>();
        auxiliary_at_state(&state, scope_key, &[], &values).map(|(_, triples)| triples)
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
            memory_source_revisions: Vec::new(),
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
            .ok_or(MemoryError::StaleSource)?;
        let (mut frame, valid, used) = read_memory_source_frame(&state, &[id], budget_bytes)?;
        let Some(mut read) = frame.remove(&id).flatten() else {
            return Ok(None);
        };
        read.materialized_bytes = used;
        if !valid {
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
    fn read_source_revisions(
        &self,
        roots: &[MemoryId],
        budget_bytes: usize,
    ) -> Result<(BTreeMap<MemoryId, Option<MemorySourceRevisionRead>>, usize)> {
        if roots.len() > crate::knowledge::KNOWLEDGE_READ_MAX_ROWS {
            return Err(MemoryError::SourceReadLimit);
        }
        let state = self.read()?;
        let (mut frame, _valid, spent) = read_memory_source_frame(&state, roots, budget_bytes)?;
        let requested = roots
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        frame.retain(|id, _| requested.contains(id));
        Ok((frame, spent))
    }

    fn read_source_revision(
        &self,
        source_ref: &str,
        budget_bytes: usize,
    ) -> Result<Option<MemorySourceRevisionRead>> {
        let read = self
            .source_revision_guard(source_ref, budget_bytes)?
            .map(|guard| guard.read.clone());
        if read
            .as_ref()
            .is_some_and(|read| read.legacy_provenance_unknown)
        {
            return Err(MemoryError::StaleSource);
        }
        Ok(read)
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

#[cfg(test)]
mod corruption_tests {
    use super::*;
    #[test]
    fn live_source_fence_without_its_original_body_is_unknown() {
        let store = EphemeralMemoryStore::new();
        let id = uuid::Uuid::new_v4();
        let source = format!("memory:{id}");
        store.write().unwrap().source_fences.insert(
            source.clone(),
            MemorySourceFence {
                source_ref: source.clone(),
                scope_key: "project_a".into(),
                delete_epoch: 0,
                retired: false,
            },
        );
        assert!(store.read_source_fence(&source).is_err());
        assert!(store.read_source_revision(&source, 4096).is_err());
    }
}

#[cfg(test)]
mod legacy_auxiliary_hold_tests {
    use super::*;

    #[tokio::test]
    async fn legacy_unknown_support_is_held_without_blocking_or_erasing_independent_graph_rows() {
        let store = std::sync::Arc::new(EphemeralMemoryStore::new());
        let now = chrono::Utc::now();
        let legacy = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L3,
            category: MemoryCategory::Reference,
            priority: crate::Priority::Normal,
            source: crate::MemorySource::AutoExtracted,
            title: "old unbound automatic input".into(),
            content: "held original bytes".into(),
            embedding: None,
            tags: vec![],
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: now,
            updated_at: now,
            last_accessed_at: None,
            scope: MemoryScope::Global,
            session_id: None,
            source_agent: None,
            visibility: crate::AgentVisibility::Shared,
        };
        let mut independent = legacy.clone();
        independent.id = uuid::Uuid::new_v4();
        independent.source = crate::MemorySource::Import;
        independent.title = "independent original input".into();
        store.insert(&legacy).await.unwrap();
        store.insert(&independent).await.unwrap();
        let node = |id: &str, source: Option<&MemoryEntry>| Entity {
            id: id.into(),
            name: id.into(),
            entity_type: crate::entity::EntityType::Concept,
            confidence: 1.0,
            frequency: 1,
            first_seen: now,
            last_seen: now,
            source_type: if source.is_some() { "memory" } else { "file" }.into(),
            source_ids: source.map(|s| vec![s.id.to_string()]).unwrap_or_default(),
            memory_source_revisions: source
                .map(|s| vec![MemoryEntryRevision::of(s).unwrap()])
                .unwrap_or_default(),
        };
        let mut child = legacy.clone();
        child.id = uuid::Uuid::new_v4();
        child.title = "old bound descendant of unknown input".into();
        child.session_id = Some("legacy-session".into());
        child.source_agent = Some("legacy-agent".into());
        let batch = "extraction-batch:00000000-0000-0000-0000-000000000001";
        child.tags = vec![batch.into()];
        let ctx = crate::MemoryTurnContext::new("legacy-session", "legacy-agent")
            .with_source_readset(Some(vec![MemoryEntryRevision::of(&legacy).unwrap()]));
        let origin = MemoryExtractionOrigin::for_entry(&ctx, &child, batch).unwrap();
        {
            let mut state = store.write().unwrap();
            state.entries.insert(child.id.to_string(), child.clone());
            state.kv.insert(
                MemoryExtractionOrigin::binding_key(child.id),
                serde_json::to_string(&origin).unwrap(),
            );
        }
        let held_child = node("old-held-child", Some(&child));
        let mut multi = node("multi-node", Some(&legacy));
        multi.source_ids.push(independent.id.to_string());
        multi
            .memory_source_revisions
            .push(MemoryEntryRevision::of(&independent).unwrap());
        let held = node("old-held-node", Some(&legacy));
        let good = node("independent-node", Some(&independent));
        let external = node("external-node", None);
        // Import the exact old native snapshot; do not add a production bypass
        // for creating a newly unbound automatic projection.
        {
            let mut state = store.write().unwrap();
            for e in [&held, &held_child, &good, &external, &multi] {
                state
                    .entities
                    .insert(("global".into(), e.id.clone()), e.clone());
            }
        }
        let visible = store.load_entities("global").await.unwrap();
        assert_eq!(
            visible
                .iter()
                .map(|e| e.id.as_str())
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from(["independent-node", "external-node", "multi-node"])
        );
        assert!(store
            .read_source_revision(
                &format!("memory:{}", legacy.id),
                crate::knowledge::KNOWLEDGE_READ_MAX_BYTES
            )
            .is_err());
        let (frame, _) = store
            .read_source_revisions(
                &[legacy.id, independent.id],
                crate::knowledge::KNOWLEDGE_READ_MAX_BYTES,
            )
            .unwrap();
        let unknown = frame[&legacy.id].as_ref().unwrap();
        assert!(unknown.legacy_provenance_unknown);
        assert!(!unknown.fence.retired);
        assert!(!unknown.origin_retired);
        assert!(unknown.revision.is_none());
        assert!(frame[&independent.id].as_ref().unwrap().revision.is_some());
        assert!(store
            .save_entities("global", std::slice::from_ref(&held))
            .await
            .is_err());
        store.save_entities("global", &visible).await.unwrap();
        let mut changed = visible.clone();
        changed
            .iter_mut()
            .find(|e| e.id == "multi-node")
            .unwrap()
            .name = "unproven metadata change".into();
        assert!(store.save_entities("global", &changed).await.is_err());
        store
            .save_knowledge_graph("global", &visible, &[])
            .await
            .unwrap();

        assert_eq!(
            serde_json::to_value(
                &store.read().unwrap().entities[&("global".into(), held.id.clone())]
            )
            .unwrap(),
            serde_json::to_value(&held).unwrap()
        );
        assert_eq!(
            serde_json::to_value(
                &store.read().unwrap().entities[&("global".into(), held_child.id.clone())]
            )
            .unwrap(),
            serde_json::to_value(&held_child).unwrap()
        );
        assert!(store
            .read_source_revision(
                &format!("memory:{}", child.id),
                crate::knowledge::KNOWLEDGE_READ_MAX_BYTES
            )
            .is_err());
        assert_eq!(
            serde_json::to_value(
                &store.read().unwrap().entities[&("global".into(), multi.id.clone())]
            )
            .unwrap(),
            serde_json::to_value(&multi).unwrap()
        );
        let root = tempfile::tempdir().unwrap();
        let mut config = crate::MemoryConfig::default();
        config.store.blob_dir = root.path().join("blobs");
        config.store.enable_vector_index = false;
        let manager =
            crate::CognitiveContextManager::new_with_selected_store(config, None, store.clone())
                .await
                .unwrap();
        assert_eq!(manager.list_entities().await.unwrap().len(), 3);
        assert_eq!(
            store
                .read_source_fence(&format!("memory:{}", legacy.id))
                .unwrap()
                .unwrap()
                .delete_epoch,
            0
        );
    }
}
