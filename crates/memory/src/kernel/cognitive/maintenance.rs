use super::*;

impl CognitiveContextManager {
    /// Run drift detection on L1 entries and check seed triggers at turn end.
    ///
    /// This covers steps 3 and 4 from the full turn-end sequence:
    ///   - Load essential layer entries, check each for staleness
    ///   - Prune stale entries via `orchestrator.forget`
    ///   - Check pre-authored seed trigger conditions against turn keywords
    ///
    /// Failures are logged and swallowed so they never abort the turn.
    pub async fn run_drift_and_seeds(&self, messages: &[Message]) -> Result<()> {
        let mut pruned_any = false;
        // ── 3. Drift detection on L1 entries ────────────────────────────────
        let l1_entries = self.orchestrator.load_fixed_layers().await?;
        for entry in &l1_entries {
            match self.drift.check(entry) {
                crate::drift::DriftVerdict::Prune { reason } => {
                    tracing::debug!(
                        id = %entry.id,
                        reason = %reason,
                        "drift: pruning entry"
                    );
                    let _ = self.orchestrator.forget(&entry.id).await;
                    pruned_any = true;
                }
                crate::drift::DriftVerdict::FlagForReview { reason } => {
                    tracing::debug!(
                        id = %entry.id,
                        reason = %reason,
                        "drift: entry flagged for review"
                    );
                }
                crate::drift::DriftVerdict::Ok => {}
            }
        }

        // ── 4. Check seed triggers at turn-end ──────────────────────────────
        let turn_keywords: Vec<String> = messages
            .iter()
            .flat_map(|m| m.content.split_whitespace().map(str::to_lowercase))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        {
            let mut reg = self.seeds.lock();
            reg.check_triggers("turn_end", &turn_keywords, Utc::now());
        }

        if pruned_any {
            self.invalidate_prepare_context_cache();
        }

        Ok(())
    }

    /// Compatibility entry point for non-runtime callers. Runtime-owned
    /// execution must use [`Self::on_turn_end_for_turn`] with its immutable
    /// turn context.
    pub async fn on_turn_end(&self, messages: &mut Vec<Message>) -> Result<()> {
        let turn = MemoryTurnContext::new("memory-api", "memory-api");
        self.on_turn_end_for_turn(&turn, messages).await
    }

    /// Run the full post-turn sequence for one explicitly identified turn.
    /// Extraction and drift/seed checks remain parallel, but every write is
    /// attributed to `turn` rather than an ambient process-global identity.
    pub async fn on_turn_end_for_turn(
        &self,
        turn: &MemoryTurnContext,
        messages: &mut Vec<Message>,
    ) -> Result<()> {
        // ── Delegation observation ────────────────────────────────────────────
        {
            let drained: Vec<_> = {
                let mut delegation_queue = self.delegation_results.lock();
                delegation_queue.drain(..).collect()
            };
            for d in drained {
                tracing::debug!(
                    agent_role = %d.agent_role,
                    task = %truncate_summary(&d.task, 40),
                    "delegation observation retained for Runtime TeamWorkingState; no direct L4 write"
                );
            }
        }

        // ── Extract ∥ Drift+Seeds ── Maintenance ──────────────────────
        let (extract_result, drift_result) = tokio::join!(
            async { self.extract_and_remember_for_turn(turn, messages).await },
            async { self.run_drift_and_seeds(messages).await },
        );
        if let Err(error) = extract_result {
            tracing::warn!(%error, "on_turn_end: extraction failed");
        }
        if let Err(error) = drift_result {
            tracing::warn!(%error, "on_turn_end: drift and seeds failed");
        }
        let result = self.run_memory_maintenance(turn, messages).await;

        // ── Auto-tune evaluation ──────────────────────────────────────────
        if self.auto_tuner.evaluate(&self.perf_monitor) {
            let cfg = self.auto_tuner.config();
            tracing::info!(
                adjustments = self.auto_tuner.adjustments_applied(),
                prefetch = cfg.prefetch_hot_topics,
                l0_ttl = cfg.l0_cache_ttl_secs,
                l1_ttl = cfg.l1_cache_ttl_secs,
                l2_ttl = cfg.l2_cache_ttl_secs,
                sandbox_lines = cfg.sandbox_min_lines,
                freshness_trigger = cfg.freshness_trigger_ratio,
                "auto_tuner: applied adjustments to TuningConfig"
            );
        }

        result
    }

    /// Remaining post-turn maintenance: fact-checker, tick,
    /// KG persistence, context rotation, closet/seeds save, etc.
    ///
    /// Call this *after* `extract_and_remember` and `run_drift_and_seeds`
    /// have completed (whether sequentially or via `tokio::join!`).
    pub async fn run_memory_maintenance(
        &self,
        turn: &MemoryTurnContext,
        messages: &mut Vec<Message>,
    ) -> Result<()> {
        let _post_turn_start = Instant::now();
        // ── 0c. Auto-correct contradictions via fact checker ──────────────
        {
            let mut fc = crate::orchestrator::get_fact_checker().lock();
            let report = fc.auto_correct();
            if report.corrected > 0 || report.pruned > 0 {
                tracing::info!(
                    corrected = report.corrected,
                    pruned = report.pruned,
                    flagged = report.flagged,
                    "auto-correction applied"
                );
            }
        }

        // Runtime retains ownership of the conversation transcript and its
        // sole semantic checkpoint. This memory-maintenance pass must not
        // apply a second threshold-driven summarizer to a copied transcript:
        // doing so would create recall noise without changing provider input.
        let _ = messages;

        // ── 5. Run orchestrator maintenance tick ─────────────────────────────
        self.orchestrator.tick().await?;

        // ── 5a. Check project KG staleness and auto-rebuild if needed ───────
        if let Some(ref mgr) = self.project_scope_mgr {
            if let Some(proj_path) = self.project_kg_path.lock().as_ref() {
                let pid = crate::project_scope::hash_path(proj_path);
                if mgr.is_kg_stale(&pid).unwrap_or(false) {
                    tracing::info!("project KG is stale, auto-rebuilding...");
                    if let Err(e) = self.load_project_kg(proj_path) {
                        tracing::warn!("auto-rebuild of project KG failed: {e}");
                    }
                }
            }
        }

        // ── 5a2. Periodic KG rebuild every 100 ticks (T1) ───────────────────
        {
            let tick = self.kg_rebuild_tick_counter.fetch_add(1, Ordering::Relaxed) + 1;
            if tick.is_multiple_of(100) {
                if let Some(proj_path) = self.project_kg_path.lock().as_ref() {
                    tracing::info!(tick, path = %proj_path.display(), "periodic KG rebuild triggered (every 100 ticks)");
                    if let Err(e) = self.load_project_kg(proj_path) {
                        tracing::warn!("periodic KG rebuild failed: {e}");
                    } else {
                        tracing::debug!("periodic KG rebuild succeeded");
                    }
                }
            }
        }

        // ── 5a3. Cross-store consistency verification every 50 ticks (T2) ────
        {
            let tick = self
                .cross_store_verify_counter
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            if tick.is_multiple_of(50) {
                let warnings = self.cross_store_verify().await;
                for w in &warnings {
                    tracing::warn!("cross-store-verify: {w}");
                }
                if !warnings.is_empty() {
                    tracing::warn!(
                        count = warnings.len(),
                        "cross-store consistency check found {} issues",
                        warnings.len()
                    );
                }
            }
        }

        // ── 5a4. Integrity anomaly detection every 50 ticks (T9) ────────────
        {
            let tick = self.integrity_check_counter.fetch_add(1, Ordering::Relaxed) + 1;
            if tick.is_multiple_of(50) {
                if let Some(ref checker) = self.integrity_checker {
                    match checker.check_anomalies() {
                        Ok(report) => {
                            if !report.anomalies.is_empty() {
                                for anomaly in &report.anomalies {
                                    tracing::warn!("integrity anomaly detected: {:?}", anomaly);
                                }
                                tracing::warn!(
                                    count = report.anomalies.len(),
                                    "integrity check found {} anomaly(ies)",
                                    report.anomalies.len()
                                );
                            }
                        }
                        Err(e) => tracing::warn!("integrity check failed: {e}"),
                    }
                }
            }
        }

        // ── 5b. Auto-rebuild Closet periodically ────────────────────────────
        if self.orchestrator.should_rebuild_closet() {
            if let Err(e) = self.orchestrator.force_rebuild_closet().await {
                tracing::warn!("auto closet rebuild failed: {e}");
            }
        }

        // ── 6. Persist vector index ─────────────────────────────────────────
        if let Err(e) = persist_vector_index_snapshot(&self.vector_index) {
            tracing::warn!("failed to persist vector index: {}", e);
        }

        // ── 7. Persist knowledge graph (every 10 ticks) ──────────────────────
        {
            let (entities, triples): (Vec<_>, Vec<_>) = {
                let kg = self.kg.lock();
                let entities: Vec<_> = kg.list_entities().into_iter().cloned().collect();
                let triples: Vec<_> = kg.list_triples().into_iter().cloned().collect();
                (entities, triples)
            };
            if !entities.is_empty() || !triples.is_empty() {
                if let Err(e) = self.orchestrator.store().save_entities(&entities).await {
                    tracing::warn!("failed to persist KG entities: {}", e);
                }
                if let Err(e) = self.orchestrator.store().save_triples(&triples).await {
                    tracing::warn!("failed to persist KG triples: {}", e);
                }
            }
        }

        // ── 8. Context rotation health check ────────────────────────────────
        {
            let total_tokens: u64 = messages.iter().map(|m| u64::from(m.token_estimate())).sum();
            let budget = self.compute_budget(&turn.agent_id);
            let mut monitor = self.context_rot_monitor.lock();
            match monitor.check(total_tokens, budget.total) {
                crate::context_rot::RotAlert::Warning(msg) => tracing::warn!("{msg}"),
                crate::context_rot::RotAlert::Critical(msg) => tracing::error!("{msg}"),
                crate::context_rot::RotAlert::None => {}
            }
        }

        // ── 9. Save Closet to KV store ────────────────────────────────────────
        match ClosetManager::build_from_orchestrator(&self.orchestrator).await {
            Ok(manager) => {
                let pointers = &manager.closet().pointers;
                match serde_json::to_string(pointers) {
                    Ok(json) => {
                        if let Err(e) = self.orchestrator.store().kv_put("closet", &json).await {
                            tracing::warn!("failed to save closet: {}", e);
                        } else {
                            let mut closet_guard = self.closet.lock();
                            *closet_guard = Some(manager.closet().clone());
                        }
                    }
                    Err(e) => tracing::warn!("failed to serialize closet pointers: {}", e),
                }
            }
            Err(e) => tracing::warn!("failed to build closet: {}", e),
        }

        // ── 10. Save Seeds to KV store ──────────────────────────────────────
        {
            let serialized = {
                let reg = self.seeds.lock();
                serde_json::to_string(reg.all_seeds())
            };
            match serialized {
                Ok(json) => {
                    if let Err(e) = self.orchestrator.store().kv_put("seeds", &json).await {
                        tracing::warn!("failed to save seeds: {}", e);
                    }
                }
                Err(e) => tracing::warn!("failed to serialize seeds: {}", e),
            }
        }

        let _post_turn_elapsed = _post_turn_start.elapsed();
        self.perf_monitor.record_extract(_post_turn_elapsed);

        Ok(())
    }

    // -----------------------------------------------------------------------
    // remember / recall
    // -----------------------------------------------------------------------

    /// Observe a child agent delegation result for later processing.
    ///
    /// Delegation results are queued and written to L4 in [`on_turn_end`].
    pub fn observe_delegation(
        &self,
        agent_role: &str,
        task: &str,
        result: &str,
        parent_session_id: Option<&str>,
    ) {
        let d = DelegationResult {
            agent_role: agent_role.to_string(),
            task: task.to_string(),
            result: result.to_string(),
            parent_session_id: parent_session_id.map(String::from),
            timestamp: Utc::now(),
        };
        let mut queue = self.delegation_results.lock();
        queue.push(d);
        tracing::debug!(
            agent_role = %agent_role,
            "delegation result queued"
        );
    }

    /// Scan current memories and enqueue reviewable lifecycle maintenance
    /// candidates. This never mutates or deletes memory entries.
    pub async fn scan_memory_maintenance(
        &self,
        config: MaintenanceScanConfig,
    ) -> Result<Vec<MaintenanceCandidate>> {
        let entries = self.list_all_entries().await?;
        self.scan_memory_maintenance_entries(&entries, config)
    }

    /// Scan an already-governed active projection.
    ///
    /// Callers that own lifecycle filtering use this entry point so archived
    /// evidence remains durable without re-entering the active review queue.
    pub fn scan_memory_maintenance_entries(
        &self,
        entries: &[crate::types::MemoryEntry],
        config: MaintenanceScanConfig,
    ) -> Result<Vec<MaintenanceCandidate>> {
        let candidates = scan_maintenance_candidates(&entries, &config);
        self.maintenance_queue.upsert_many(candidates.clone())?;
        Ok(candidates)
    }

    /// Analyze a full governance snapshot away from the async runtime worker.
    ///
    /// The returned entries are the same owned snapshot used for analysis, so
    /// callers do not clone a potentially large corpus merely to avoid
    /// blocking request and live-event tasks.
    pub async fn scan_memory_maintenance_entries_off_thread(
        &self,
        entries: Vec<crate::types::MemoryEntry>,
        config: MaintenanceScanConfig,
    ) -> Result<(Vec<crate::types::MemoryEntry>, Vec<MaintenanceCandidate>)> {
        let (entries, candidates) = tokio::task::spawn_blocking(move || {
            let candidates = scan_maintenance_candidates(&entries, &config);
            (entries, candidates)
        })
        .await
        .map_err(|error| {
            MemoryError::Other(format!("memory governance analysis failed: {error}"))
        })?;
        self.maintenance_queue.upsert_many(candidates.clone())?;
        Ok((entries, candidates))
    }

    /// List queued memory lifecycle candidates.
    pub fn list_memory_maintenance(
        &self,
        filter: MaintenanceCandidateFilter,
    ) -> Result<Vec<MaintenanceCandidate>> {
        self.maintenance_queue.list(filter)
    }

    /// Move a maintenance candidate through the explicit review lifecycle.
    pub fn transition_memory_maintenance(
        &self,
        id: &str,
        status: MaintenanceCandidateStatus,
    ) -> Result<Option<MaintenanceCandidate>> {
        self.maintenance_queue.transition(id, status)
    }

    /// Consume an explicit promotion batch produced by Runtime policy.
    pub fn process_memory_pulse(&self, batch: MemoryPulseBatch) -> Result<MemoryPulseReport> {
        MemoryPulseConsumer::new(self.maintenance_queue.clone()).process_batch(batch)
    }

    /// List persisted knowledge-graph entities.
    pub async fn list_entities(&self) -> Result<Vec<crate::entity::Entity>> {
        self.orchestrator.store().load_entities().await
    }

    /// List persisted knowledge-graph triples.
    pub async fn list_triples(&self) -> Result<Vec<crate::entity::Triple>> {
        self.orchestrator.store().load_triples().await
    }

    /// Link a code symbol to a memory entry for impact analysis and symbol-
    /// scoped recall.
    pub async fn link_symbol_to_memory(
        &self,
        symbol_id: &str,
        memory_id: MemoryId,
        turn_index: Option<i32>,
        reference_type: &str,
    ) -> Result<()> {
        self.orchestrator
            .store()
            .link_symbol_to_memory(
                symbol_id,
                &memory_id,
                turn_index,
                reference_type,
                chrono::Utc::now().timestamp_millis(),
            )
            .await?;
        self.invalidate_prepare_context_cache();
        Ok(())
    }

    /// Return full memory entries linked to a code symbol name or symbol ID.
    pub async fn find_memories_by_symbol(
        &self,
        symbol_name: &str,
    ) -> Result<Vec<crate::types::MemoryEntry>> {
        let ids = self
            .orchestrator
            .store()
            .find_memories_by_symbol(symbol_name)
            .await?;
        let mut entries = Vec::new();
        for id in ids {
            if let Some(entry) = self.orchestrator.store().get(&id).await? {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    /// Return recent memory write audit entries for enterprise export.
    pub fn audit_entries(&self, limit: usize) -> Result<Vec<AuditEntry>> {
        let limit = limit.min(1000);
        if let Some(ref log) = self.audit_log {
            return log
                .query_recent(limit)
                .map_err(|e| MemoryError::Store(format!("query audit log: {e}")));
        }
        if let Some(ref checker) = self.integrity_checker {
            return checker
                .audit_log()
                .query_recent(limit)
                .map_err(|e| MemoryError::Store(format!("query audit log: {e}")));
        }
        Ok(Vec::new())
    }

    // -----------------------------------------------------------------------
    // Vector Index Persistence
    // -----------------------------------------------------------------------

    /// Persist the vector index to disk for durability.
    ///
    /// This saves all embeddings to `blob_dir/vector_index.json`.
    /// Called automatically by [`on_turn_end`], but can be invoked manually
    /// for explicit checkpointing.
    pub fn persist_vector_index(&self) -> Result<()> {
        persist_vector_index_snapshot(&self.vector_index)
            .map_err(|e| MemoryError::Store(format!("persist vector index: {e}")))
    }

    /// Get the number of vectors currently indexed.
    #[must_use]
    pub fn vector_index_count(&self) -> usize {
        self.vector_index.read().count()
    }

    /// Evict a lifecycle-inactive memory from the rebuildable semantic index.
    pub fn evict_vector_entry(&self, id: &MemoryId) -> Result<()> {
        let snapshot = {
            let mut index = self.vector_index.write();
            index.remove(id)?;
            index.persistence_snapshot()
        };
        snapshot.persist()
    }

    /// Get vector index statistics.
    #[must_use]
    pub fn vector_index_stats(&self) -> VectorIndexStats {
        let stats = self.vector_index.read().runtime_stats();
        VectorIndexStats {
            count: stats.count,
            generation: stats.generation,
            persisted_generation: stats.persisted_generation,
            evictions: stats.evictions,
            persistence_failures: stats.persistence_failures,
            last_persistence_error: stats.last_persistence_error,
        }
    }

    // -----------------------------------------------------------------------
    // Decision threads
    // -----------------------------------------------------------------------

    /// Record a decision entry into `thread_id`'s decision thread.
    ///
    /// If the thread does not yet exist it is created automatically.
    pub fn record_decision(&self, thread_id: &str, decision: DecisionEntry) -> Result<()> {
        let mut store = self.decisions.lock();

        // Ensure the thread exists.
        store.create_thread(thread_id);

        // Append the entry using the record() compatibility API.
        store.record(
            thread_id,
            decision.summary,
            decision.rationale,
            decision.alternatives,
        );

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Build a [`TokenBudget`] from the current config, allocating by agent role.
    ///
    /// Role multipliers: Planner=0.40, Executor=0.25, Reviewer=0.15, Orchestrator=0.50.
    /// Unknown roles default to Orchestrator (0.50).
    pub(super) fn compute_budget(&self, agent_id: &str) -> TokenBudget {
        if self.config.budget.runtime_managed {
            return BudgetCalculator::new(self.config.budget.clone()).make_budget();
        }
        BudgetCalculator::new(self.config.budget.clone()).make_role_budget(agent_id)
    }

    /// Verify cross-store consistency: KG ↔ MemoryStore ↔ Verbatim ↔ Closet.
    ///
    /// Samples 10 random entries from each store and checks for referential
    /// integrity. Returns a list of warning strings. Kept lightweight (<10ms).
    pub(super) async fn cross_store_verify(&self) -> Vec<String> {
        let mut warnings = Vec::new();

        // 1. KG entities → MemoryStore: check a random sample of KG entities
        //    have corresponding MemoryStore entries.
        {
            let entities: Vec<_> = {
                let kg = self.kg.lock();
                kg.list_entities().into_iter().cloned().collect()
            }; // kg dropped before any .await
            let sample_size = 10usize.min(entities.len());
            if sample_size > 0 {
                let store = self.orchestrator.store();
                // Use a deterministic pseudo-random subset via modulo hash on entity id
                let step = (entities.len() / sample_size).max(1);
                let mut checked = 0usize;
                for (i, entity) in entities.iter().enumerate() {
                    if i % step != 0 || checked >= sample_size {
                        continue;
                    }
                    checked += 1;
                    // Check if entity name appears in store via FTS
                    let found = store.search_fts(&entity.name, 1).await;
                    match found {
                        Ok(results) if results.is_empty() => {
                            warnings.push(format!(
                                "kg-orphan: entity '{}' ({}) not found in MemoryStore FTS",
                                entity.name, entity.id
                            ));
                        }
                        Err(e) => {
                            warnings.push(format!(
                                "kg-orphan-check: entity '{}' FTS query failed: {e}",
                                entity.name
                            ));
                        }
                        _ => {} // OK
                    }
                }
            }
        }

        // 2. Closet pointers → MemoryStore: check a random sample of drawer_ids
        //    exist in MemoryStore.
        {
            let sampled_ids: Vec<String> = {
                let closet_guard = self.closet.lock();
                if let Some(ref closet) = *closet_guard {
                    let all_ids: Vec<&str> = closet
                        .pointers
                        .iter()
                        .flat_map(|p| p.drawer_ids.iter().map(String::as_str))
                        .collect();
                    let sample_size = 10usize.min(all_ids.len());
                    if sample_size > 0 {
                        let step = (all_ids.len() / sample_size).max(1);
                        let mut result = Vec::new();
                        let mut checked = 0usize;
                        for (i, drawer_id) in all_ids.iter().enumerate() {
                            if i % step != 0 || checked >= sample_size {
                                continue;
                            }
                            checked += 1;
                            result.push(drawer_id.to_string());
                        }
                        result
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                }
            }; // closet_guard dropped before any .await
            if !sampled_ids.is_empty() {
                let store = self.orchestrator.store();
                for drawer_id in &sampled_ids {
                    let uuid = match uuid::Uuid::parse_str(drawer_id) {
                        Ok(id) => id,
                        Err(_) => continue,
                    };
                    let found = store.get(&uuid).await;
                    match found {
                        Ok(None) => {
                            warnings.push(format!(
                                "closet-orphan: drawer_id '{drawer_id}' not found in MemoryStore"
                            ));
                        }
                        Err(e) => {
                            warnings.push(format!(
                                "closet-orphan-check: drawer_id '{drawer_id}' get failed: {e}"
                            ));
                        }
                        _ => {} // OK
                    }
                }
            }
        }

        // 3. Verbatim ↔ MemoryStore: sample MemoryStore entries and check
        //    verbatim counterparts exist (reverse check since we can't list verbatim).
        {
            let store = self.orchestrator.store();
            let all_entries = store.list_all().await;
            match all_entries {
                Ok(entries) if !entries.is_empty() => {
                    let sample_size = 10usize.min(entries.len());
                    let step = (entries.len() / sample_size).max(1);
                    let mut checked = 0usize;
                    for (i, entry) in entries.iter().enumerate() {
                        if i % step != 0 || checked >= sample_size {
                            continue;
                        }
                        checked += 1;
                        let verbatim = store.load_verbatim_by_id(&entry.id.to_string()).await;
                        match verbatim {
                            Ok(None) => {
                                warnings.push(format!(
                                    "verbatim-missing: MemoryStore entry {} has no Verbatim counterpart",
                                    entry.id
                                ));
                            }
                            Err(e) => {
                                warnings.push(format!(
                                    "verbatim-check: entry {} verbatim load failed: {e}",
                                    entry.id
                                ));
                            }
                            _ => {} // OK
                        }
                    }
                }
                Ok(_) => {} // empty store, nothing to verify
                Err(e) => {
                    warnings.push(format!("cross-store-verify: list_all failed: {e}"));
                }
            }
        }

        // 4. Coherence check: verify KG entity names appear in at least one
        //    MemoryStore entry with a minimum Jaccard similarity.
        {
            let entities: Vec<_> = {
                let kg = self.kg.lock();
                kg.list_entities().into_iter().cloned().collect()
            }; // kg dropped before any .await
            if !entities.is_empty() {
                let sample_size = 5usize.min(entities.len());
                let step = (entities.len() / sample_size).max(1);
                let store = self.orchestrator.store();
                let mut checked = 0usize;
                for (i, entity) in entities.iter().enumerate() {
                    if i % step != 0 || checked >= sample_size {
                        continue;
                    }
                    checked += 1;
                    let results = store.search_fts(&entity.name, 3).await;
                    match results {
                        Ok(entries) => {
                            let has_relevant = entries.iter().any(|e| {
                                coherence::jaccard_similarity(&entity.name, &e.content) > 0.1
                            });
                            if !has_relevant && !entries.is_empty() {
                                warnings.push(format!(
                                    "coherence-low: entity '{}' has no relevant MemoryStore entry (best checked: {})",
                                    entity.name,
                                    entries.first().map(|e| e.title.as_str()).unwrap_or("none")
                                ));
                            }
                        }
                        Err(e) => {
                            warnings.push(format!(
                                "coherence-check: entity '{}' FTS search failed: {e}",
                                entity.name
                            ));
                        }
                    }
                }
            }
        }

        warnings
    }

    /// Approximate token count for a slice of memory entries (chars / 4).
    pub(super) fn estimate_tokens_entries(&self, entries: &[MemoryEntry]) -> u64 {
        entries
            .iter()
            .map(|e| (e.content.len() as u64).div_ceil(4))
            .sum()
    }

    // -----------------------------------------------------------------------
    // Agent self-aware diagnostics
    // -----------------------------------------------------------------------

    /// Return the current context window health as a [`RotAlert`].
    ///
    /// This is a **read-only** diagnostic — it does not modify any internal
    /// state, trigger debounce logic, or update counters.  Callers can use
    /// this for agent-facing health checks without side effects.
    ///
    /// The method reads the stored `context_usage_ratio` from the
    /// [`ContextRotMonitor`] metrics and maps it to the appropriate alert
    /// level:
    ///
    /// | Ratio      | Alert    |
    /// |------------|----------|
    /// | ≤ 0.65     | `None`   |
    /// | 0.65–0.75  | `Warning`|
    /// | > 0.75     | `Critical`|
    #[must_use]
    pub fn ctx_health(&self) -> RotAlert {
        let monitor = self.context_rot_monitor.lock();
        let ratio = monitor.metrics.context_usage_ratio;
        let total = self.config.budget.context_window;
        let used = (ratio * total as f32) as u64;

        if ratio > 0.75 {
            RotAlert::Critical(format!(
                "⚠ CONTEXT ROT: {:.1}% usage ({} / {} tokens). Auto-record session state.",
                ratio * 100.0,
                used,
                total
            ))
        } else if ratio > 0.65 {
            RotAlert::Warning(format!(
                "⚠ Context usage at {:.1}% — inject agent-facing message.",
                ratio * 100.0
            ))
        } else {
            RotAlert::None
        }
    }

    #[must_use]
    pub fn background_extraction_health(&self) -> BackgroundExtractionHealth {
        let mut health = self.background_extraction_state.snapshot();
        let stats = self.vector_index_stats();
        health.vector_entries = stats.count as u64;
        health.vector_evictions = stats.evictions;
        health.vector_generation = stats.generation;
        health.vector_persisted_generation = stats.persisted_generation;
        health.vector_persistence_failures = stats.persistence_failures;
        health.vector_coverage_basis_points = if health.vector_active_entries == 0 {
            if health.vector_reconciliation_complete {
                10_000
            } else {
                0
            }
        } else {
            health
                .vector_indexed_active_entries
                .saturating_mul(10_000)
                .checked_div(health.vector_active_entries)
                .unwrap_or_default()
                .min(10_000)
        };
        health.degraded_to_fts = !self.embedding_capability.supports_semantic()
            || !health.vector_reconciliation_complete
            || health.vector_coverage_basis_points < 10_000
            || health.last_index_error.is_some()
            || stats.last_persistence_error.is_some();
        health
    }

    /// Stop every background execution body owned by this manager.
    ///
    /// Gateway calls this during normal shutdown and startup rollback. Handles
    /// are taken before awaiting, making repeated calls idempotent.
    pub async fn shutdown_background_tasks(&self) -> MemoryBackgroundShutdownReport {
        let _ = self.background_shutdown.send(true);
        let mut report = MemoryBackgroundShutdownReport::default();
        let watcher = self.background_watcher.lock().take();
        if let Some(watcher) = watcher {
            match tokio::task::spawn_blocking(move || watcher.shutdown()).await {
                Ok(Ok(())) => report.watcher_joined = true,
                Ok(Err(error)) => report.errors.push(error),
                Err(error) => report
                    .errors
                    .push(format!("join background watcher shutdown: {error}")),
            }
        } else {
            report.watcher_joined = true;
        }
        let handles = [
            self.extract_handle.take(),
            self.kg_rebuild_handle.take(),
            self.memory_usage_persist_handle.take(),
        ];
        for handle in handles.into_iter().flatten() {
            join_memory_background_task(handle, &mut report).await;
        }
        report
    }

    pub(crate) fn record_memory_usage_signal(&self, signal: MemoryUsageSignal) {
        let key = memory_usage_signal_key(&signal);
        let mut signals = self.memory_usage_signals.lock();
        if let Some(current) = signals.get_mut(&key) {
            current.selected_count = current.selected_count.saturating_add(signal.selected_count);
            current.last_reason = signal.last_reason;
        } else if signals.len() < MAX_MEMORY_USAGE_KEYS {
            signals.insert(key, signal);
        } else {
            self.memory_usage_writer_state
                .dropped_keys
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        drop(signals);
        match self.memory_usage_persist_tx.try_send(()) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.memory_usage_writer_state
                    .coalesced
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.memory_usage_writer_state
                    .persistence_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn memory_usage_summary(&self) -> MemoryUsageSummary {
        let signals = self
            .memory_usage_signals
            .lock()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        summarize_usage(&signals, 3)
    }

    #[must_use]
    pub fn memory_usage_writer_health(&self) -> MemoryUsageWriterHealth {
        MemoryUsageWriterHealth {
            keys: self.memory_usage_signals.lock().len(),
            persisted_batches: self
                .memory_usage_writer_state
                .persisted_batches
                .load(Ordering::Relaxed),
            coalesced: self
                .memory_usage_writer_state
                .coalesced
                .load(Ordering::Relaxed),
            dropped_keys: self
                .memory_usage_writer_state
                .dropped_keys
                .load(Ordering::Relaxed),
            persistence_failures: self
                .memory_usage_writer_state
                .persistence_failures
                .load(Ordering::Relaxed),
        }
    }

    // ── Performance report (P9.4) ────────────────────────────────────────

    /// Return a snapshot of current performance metrics and auto-tuner state.
    #[must_use]
    pub fn performance_report(&self) -> crate::performance_monitor::PerformanceReport {
        let last_tuning = self.auto_tuner.last_tuning_instant().map(|i| {
            let elapsed = i.elapsed();
            // Approximate wall-clock DateTime by subtracting from now
            Utc::now()
                - chrono::Duration::from_std(elapsed)
                    .unwrap_or_else(|_| chrono::Duration::seconds(0))
        });
        let tuning_applied = self.auto_tuner.adjustments_applied() > 0;
        let tuning_config = self.auto_tuner.config();
        self.perf_monitor
            .report(&tuning_config, tuning_applied, last_tuning)
    }
}

async fn join_memory_background_task(
    mut handle: tokio::task::JoinHandle<()>,
    report: &mut MemoryBackgroundShutdownReport,
) {
    match tokio::time::timeout(Duration::from_secs(5), &mut handle).await {
        Ok(Ok(())) => report.joined_tasks += 1,
        Ok(Err(error)) => report
            .errors
            .push(format!("memory background task failed: {error}")),
        Err(_) => {
            handle.abort();
            let _ = handle.await;
            report.forced_aborts += 1;
        }
    }
}
