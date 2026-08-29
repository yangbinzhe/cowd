use super::*;

impl CognitiveContextManager {
    /// Compatibility entry point for non-runtime callers. Runtime-owned
    /// execution must call [`Self::prepare_context_for_turn`] with its exact
    /// immutable turn identity and data lease.
    pub async fn prepare_context(
        &self,
        query: &str,
        messages: &[Message],
        session_id: Option<&str>,
    ) -> Result<PreparedContext> {
        let turn = MemoryTurnContext::new(session_id.unwrap_or("memory-api"), "memory-api");
        self.prepare_context_for_turn(&turn, query, messages).await
    }

    /// Assemble the optimal context for one explicitly identified model turn.
    ///
    /// Implements "progressive disclosure":
    /// 1. Load fixed layers L0 + L1.
    /// 2. Load project context L2.
    /// 3. Dynamic-load relevant deep memories L3 via multi-signal scoring.
    /// 4. Surface triggered seeds.
    /// 5. Sample context window pressure.
    /// 6. Compress if needed.
    pub async fn prepare_context_for_turn(
        &self,
        turn: &MemoryTurnContext,
        query: &str,
        messages: &[Message],
    ) -> Result<PreparedContext> {
        let _prepare_start = Instant::now();
        let mut entries: Vec<MemoryEntry> = Vec::new();

        let budget = self.compute_budget(&turn.agent_id);
        let cache_revision = self.memory_revision.load(Ordering::Relaxed);
        let cache_key = self.prepare_context_cache_key(query, messages, turn, &budget);
        if entries.is_empty() {
            if let Some(context) = self.cached_prepared_context(cache_key, cache_revision) {
                let elapsed = _prepare_start.elapsed();
                self.perf_monitor.record_cache_hit();
                self.perf_monitor.record_prepare_context(elapsed);
                tracing::debug!(
                    elapsed_ms = elapsed.as_millis(),
                    entries = context.entries.len(),
                    "prepare_context cache hit"
                );
                return Ok(context);
            }
        }

        // ═══════════════════════════════════════════════════════════════════
        // Step 0a: Closet LRU prefetch — preload hot topics based on
        //          access counts tracked in closet pointers (F19).
        // ═══════════════════════════════════════════════════════════════════
        {
            let k = self.config.tuning.prefetch_hot_topics;
            if k > 0 {
                // Collect hot topics (owned strings) while holding the lock,
                // then drop the lock before async operations.
                let hot_topics: Vec<String> = {
                    let closet_guard = self.orchestrator.closet_manager().lock();
                    closet_guard
                        .get_hot_pointers(k)
                        .into_iter()
                        .map(|p| p.topic.clone())
                        .collect()
                };
                for topic in hot_topics {
                    let prefetch_set: HashSet<MemoryId> = entries.iter().map(|e| e.id).collect();
                    let budget =
                        (self.config.budget.available_tokens() / 4).min(u64::from(u32::MAX)) as u32;
                    match self
                        .orchestrator
                        .recall_relevant(&topic, None, &prefetch_set, budget)
                        .await
                    {
                        Ok(mut recalled) => {
                            for entry in &mut recalled {
                                entry.content = format!("[PREFETCH: {}] {}", topic, entry.content);
                                entry.tags.push("prefetch".into());
                                entry.source = MemorySource::Prefetch;
                                entry.priority = Priority::High;
                            }
                            entries.extend(recalled);
                        }
                        Err(e) => {
                            tracing::debug!(
                                topic = %topic,
                                error = %e,
                                "closet prefetch: recall_relevant failed for hot topic"
                            );
                        }
                    }
                }
            }
        }

        // ═══════════════════════════════════════════════════════════════════
        // Group 1: Base layers (L0+L1) + Project layer (L2) — cache-aware
        // ═══════════════════════════════════════════════════════════════════

        // L0 + L1: check cache first; reload both together if either expired.
        {
            let l0_hit = self
                .l0_cache
                .lock()
                .as_ref()
                .filter(|c| {
                    c.cached_at.elapsed()
                        < Duration::from_secs(self.config.tuning.l0_cache_ttl_secs)
                })
                .map(|c| c.entries.clone());
            let l1_hit = self
                .l1_cache
                .lock()
                .as_ref()
                .filter(|c| {
                    c.cached_at.elapsed()
                        < Duration::from_secs(self.config.tuning.l1_cache_ttl_secs)
                })
                .map(|c| c.entries.clone());

            if let (Some(l0), Some(l1)) = (l0_hit, l1_hit) {
                self.perf_monitor.record_cache_hit();
                entries.extend(l0);
                entries.extend(l1);
            } else {
                self.perf_monitor.record_cache_miss();
                let fixed = self.orchestrator.load_fixed_layers().await?;
                let l0: Vec<_> = fixed
                    .iter()
                    .filter(|e| matches!(e.layer, MemoryLayer::L0))
                    .cloned()
                    .collect();
                let l1: Vec<_> = fixed
                    .iter()
                    .filter(|e| matches!(e.layer, MemoryLayer::L1))
                    .cloned()
                    .collect();
                let now = Instant::now();
                *self.l0_cache.lock() = Some(CachedLayer {
                    entries: l0.clone(),
                    knowledge_graph: String::new(),
                    code_context: String::new(),
                    cached_at: now,
                });
                *self.l1_cache.lock() = Some(CachedLayer {
                    entries: l1.clone(),
                    knowledge_graph: String::new(),
                    code_context: String::new(),
                    cached_at: now,
                });
                entries.extend(l0);
                entries.extend(l1);
            }
        }

        // L2: project context with cache
        {
            let l2_hit = self
                .l2_cache
                .lock()
                .as_ref()
                .filter(|c| {
                    c.cached_at.elapsed()
                        < Duration::from_secs(self.config.tuning.l2_cache_ttl_secs)
                })
                .map(|c| c.entries.clone());
            if let Some(l2) = l2_hit {
                self.perf_monitor.record_cache_hit();
                entries.extend(l2);
            } else {
                self.perf_monitor.record_cache_miss();
                let l2 = self.orchestrator.load_project_context().await?;
                *self.l2_cache.lock() = Some(CachedLayer {
                    entries: l2.clone(),
                    knowledge_graph: String::new(),
                    code_context: String::new(),
                    cached_at: Instant::now(),
                });
                entries.extend(l2);
            }
        }

        // Query embedding (async, independent of cached loads)
        let query_embedding = {
            if self.embedding_capability.supports_semantic() {
                match &self.embedding_capability {
                    EmbeddingCapability::Remote { client } => match client.embed_one(query).await {
                        Ok(embed) => {
                            tracing::debug!(
                                dim = embed.len(),
                                "query embedding generated for hybrid search"
                            );
                            Some(embed)
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "embedding failed, falling back to FTS5 search"
                            );
                            None
                        }
                    },
                    _ => None,
                }
            } else {
                None
            }
        };

        // Track which IDs are already loaded so other layers can skip them.
        let mut already_surfaced: HashSet<MemoryId> = entries.iter().map(|e| e.id).collect();

        // ── Step 2a2: State rebuild from previous session state ──────────────
        if let Some(ref rebuilder) = self.state_rebuilder {
            let rebuilt = rebuilder.quick_rebuild().await;
            if rebuilt.overall_confidence > self.config.tuning.rebuild_confidence {
                if let Some(ref summary) = rebuilt.context_summary {
                    entries.push(MemoryEntry {
                        id: uuid::Uuid::new_v4(),
                        layer: MemoryLayer::L2,
                        category: MemoryCategory::CompressedSummary,
                        priority: Priority::Normal,
                        source: MemorySource::AutoExtracted,
                        title: "Rebuilt Context Summary".into(),
                        content: format!(
                            "[REBUILT STATE confidence={:.2}] {}",
                            rebuilt.overall_confidence, summary.data
                        ),
                        embedding: None,
                        tags: vec!["rebuilt".into(), "state".into()],
                        relations: vec![],
                        confidence: summary.confidence,
                        access_count: 0,
                        staleness: 0.0,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        last_accessed_at: None,
                        scope: MemoryScope::default(),
                        session_id: None,
                        source_agent: None,
                        visibility: crate::types::AgentVisibility::default(),
                    });
                }
                for item in rebuilt.get_incomplete_work() {
                    entries.push(MemoryEntry {
                        id: uuid::Uuid::new_v4(),
                        layer: MemoryLayer::L1,
                        category: MemoryCategory::Reference,
                        priority: item.priority,
                        source: MemorySource::AutoExtracted,
                        title: format!("Rebuilt: {}", item.title),
                        content: format!("[REBUILT WORK ITEM] {}", item.description),
                        embedding: None,
                        tags: vec!["rebuilt".into(), "work".into()],
                        relations: vec![],
                        confidence: 0.7,
                        access_count: 0,
                        staleness: 0.0,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        last_accessed_at: None,
                        scope: MemoryScope::default(),
                        session_id: None,
                        source_agent: None,
                        visibility: crate::types::AgentVisibility::default(),
                    });
                }
                tracing::info!(
                    confidence = rebuilt.overall_confidence,
                    work_items = rebuilt.get_incomplete_work().len(),
                    "state_rebuilder: surfaced rebuilt state"
                );
            }
        }

        // ── Step 2b: P1 project knowledge graph query ───────────────────────
        {
            let kg = self.kg.lock();
            let query_tokens: Vec<String> =
                query.split_whitespace().map(str::to_lowercase).collect();
            let mut seen: HashSet<String> = HashSet::new();
            for token in &query_tokens {
                if seen.contains(token) {
                    continue;
                }
                if let Some(entity) = kg.get_entity_by_name(token) {
                    seen.insert(token.clone());
                    use crate::types::{MemoryCategory, MemoryLayer, MemorySource, Priority};
                    entries.push(MemoryEntry {
                        id: uuid::Uuid::new_v4(),
                        layer: MemoryLayer::L2,
                        category: MemoryCategory::ProjectKnowledge,
                        priority: Priority::Normal,
                        source: MemorySource::AutoExtracted,
                        title: format!("KG entity: {}", entity.name),
                        content: format!(
                            "Project entity '{}' (type: {}, confidence: {:.2})",
                            entity.name, entity.entity_type, entity.confidence
                        ),
                        embedding: None,
                        tags: vec!["kg".into(), "project".into()],
                        relations: vec![],
                        confidence: entity.confidence as f32,
                        access_count: 0,
                        staleness: 0.0,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        last_accessed_at: None,
                        scope: MemoryScope::default(),
                        session_id: None,
                        source_agent: None,
                        visibility: crate::types::AgentVisibility::default(),
                    });
                    tracing::debug!(
                        entity = %entity.name,
                        entity_type = %entity.entity_type,
                        "P1 KG: surfaced project entity"
                    );
                }
            }
        }

        // Compute budget for L3 token-aware recall
        let memory_budget = budget
            .available
            .saturating_sub(self.estimate_tokens_entries(&entries))
            .min(u64::from(u32::MAX)) as u32;

        // ═══════════════════════════════════════════════════════════════════
        // Group 2: L3 recall + session resume. Runtime's binding-aware
        // RealityRecallPort injects any promoted L4 knowledge explicitly;
        // cognitive preparation must not broadcast peer or global Team state.
        // ═══════════════════════════════════════════════════════════════════
        let (l3_result, resume_result) = tokio::join!(
            // L3 deep recall (hybrid semantic + BM25)
            async {
                self.orchestrator
                    .recall_relevant(
                        query,
                        query_embedding.as_deref(),
                        &already_surfaced,
                        memory_budget * 2, // over-fetch for hybrid re-ranking
                    )
                    .await
            },
            // Session resume from prior session context
            async {
                if let Some(ref resume) = self.session_resume {
                    let store_arc = self.orchestrator.store();
                    let store: &dyn crate::store::MemoryStore = store_arc.as_ref();
                    resume.resume_recent(query, Some(store), 5).await
                } else {
                    Ok(Vec::new())
                }
            },
        );

        // ── Process L3 results: hybrid re-ranking ──
        let deep_entries = l3_result?;

        // ── Hybrid re-ranking: combine vector + BM25 scores ──
        let re_ranked = if !deep_entries.is_empty() {
            let vector_results: Vec<(String, String, f64)> = deep_entries
                .iter()
                .map(|e| (e.id.to_string(), e.content.clone(), e.confidence as f64))
                .collect();
            let all_docs: Vec<String> = deep_entries.iter().map(|e| e.content.clone()).collect();
            let doc_ids: Vec<String> = deep_entries.iter().map(|e| e.id.to_string()).collect();
            let hybrid_results = self.hybrid_searcher.search(
                query,
                vector_results,
                &all_docs,
                &doc_ids,
                memory_budget as usize,
            );
            // Re-order deep_entries by hybrid score
            let mut scored: Vec<(usize, f64)> = hybrid_results
                .iter()
                .filter_map(|r| {
                    deep_entries
                        .iter()
                        .position(|e| e.id.to_string() == r.id)
                        .map(|idx| (idx, r.hybrid_score))
                })
                .collect();
            scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            scored
                .into_iter()
                .take(memory_budget as usize)
                .map(|(idx, _)| deep_entries[idx].clone())
                .collect()
        } else {
            deep_entries
        };

        for e in &re_ranked {
            already_surfaced.insert(e.id);
        }
        entries.extend(re_ranked);

        // ── Process SessionResume results (after L3 to avoid &self conflict) ──
        match resume_result {
            Ok(resumed) => {
                for mut entry in resumed {
                    if !already_surfaced.contains(&entry.id) {
                        entry.content = format!("[SESSION RESUME] {}", entry.content);
                        entry.tags.push("session_resume".into());
                        already_surfaced.insert(entry.id);
                        entries.push(entry);
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "session resume failed, continuing without it");
            }
        }

        // ── Session isolation filter (via ContextFence) ──
        let fence = crate::context_fence::fence_from_session(&turn.session_id, None, None);
        entries = crate::context_fence::filter_through_fence(&entries, &fence)
            .into_iter()
            .cloned()
            .collect();

        // ── Step 4: check seed triggers and inject as high-priority L1 entries ──
        let query_words: Vec<String> = query.split_whitespace().map(str::to_lowercase).collect();
        let triggered = {
            let mut reg = self.seeds.lock();
            reg.check_triggers("default", &query_words, Utc::now())
        };
        for seed in triggered {
            use crate::types::{MemoryCategory, MemoryLayer, MemorySource, Priority};
            entries.push(MemoryEntry {
                id: uuid::Uuid::new_v4(),
                layer: MemoryLayer::L1,
                category: MemoryCategory::Reference,
                priority: Priority::High,
                source: MemorySource::Import,
                title: format!("Seed: {}", seed.name),
                content: seed.content,
                embedding: None,
                tags: vec!["seed".into()],
                relations: vec![],
                confidence: 1.0,
                access_count: 0,
                staleness: 0.0,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                last_accessed_at: None,
                scope: MemoryScope::default(),
                session_id: None,
                source_agent: None,
                visibility: crate::types::AgentVisibility::default(),
            });
            tracing::debug!(seed_id = %seed.id, "injected seed into context");
        }

        // ── Step 5: sample context window pressure ───────────────────────────
        let total_message_tokens: u64 =
            messages.iter().map(|m| u64::from(m.token_estimate())).sum();
        let total_entry_tokens: u64 = self.estimate_tokens_entries(&entries);
        let used_tokens = total_message_tokens + total_entry_tokens;
        let _monitor_snapshot = self.monitor.sample(used_tokens);

        // ── Step 5b: freshness-priority loading when budget is tight ─────────
        // When token usage exceeds 80% of the available budget, switch to
        // freshness-priority loading via FreshContextManager.
        let budget_usage_ratio = if budget.available > 0 {
            used_tokens as f32 / budget.available as f32
        } else {
            1.0
        };
        if budget_usage_ratio > self.config.tuning.freshness_trigger_ratio {
            tracing::info!(
                ratio = %budget_usage_ratio,
                used = %used_tokens,
                available = %budget.available,
                "freshness priority activated: budget > {:.0}%",
                self.config.tuning.freshness_trigger_ratio * 100.0
            );
            let entry_count = entries.len();
            entries = self
                .fresh_ctx
                .load_fresh_entries("cognitive-default", entries, entry_count)
                .await;
        }

        // ── Step 6: Runtime owns transcript compaction ───────────────────────
        // Context preparation never mutates a transcript. Semantic session
        // checkpoints are created only by Runtime after a real provider
        // preflight proves that required input cannot fit.

        // Step 6c: inject recent entity evolutions from other agents
        {
            let registry_guard = self.entity_registry.lock();
            if let Some(ref registry) = *registry_guard {
                if registry.has_store() {
                    match registry.get_recent_evolutions(10) {
                        Ok(evolutions) if !evolutions.is_empty() => {
                            let mut story_lines: Vec<String> = Vec::new();
                            for ev in &evolutions {
                                story_lines.push(format!("  - {}", ev.to_sentence()));
                            }
                            let content = format!(
                                "Recent entity changes (cross-agent):\n{}",
                                story_lines.join("\n")
                            );
                            entries.push(MemoryEntry {
                                id: uuid::Uuid::new_v4(),
                                layer: MemoryLayer::L2,
                                category: MemoryCategory::Shared,
                                priority: Priority::Low,
                                source: MemorySource::AutoExtracted,
                                title: "Entity Evolution Context".into(),
                                content,
                                embedding: None,
                                tags: vec!["entity_evolution".into(), "cross_agent".into()],
                                relations: vec![],
                                confidence: 0.7,
                                access_count: 0,
                                staleness: 0.0,
                                created_at: Utc::now(),
                                updated_at: Utc::now(),
                                last_accessed_at: None,
                                scope: MemoryScope::default(),
                                session_id: None,
                                source_agent: None,
                                visibility: crate::types::AgentVisibility::default(),
                            });
                        }
                        Err(e) => {
                            tracing::debug!(
                                error = %e,
                                "entity evolution: failed to query recent evolutions"
                            );
                        }
                        _ => {}
                    }
                }
            }
        }

        // Step 7: auto-inject relevant code symbols (when applicable)
        let code_context = if is_code_query(query) {
            let symbols = self.orchestrator.find_relevant_symbols(query, 5).await;
            if symbols.is_empty() {
                None
            } else {
                Some(format_code_context(&symbols))
            }
        } else {
            None
        };

        // ── Step 7b: Tool output sandbox injection ──
        {
            let sandbox = self.tool_sandbox.lock();
            let count = sandbox.entry_count();
            if count > 0 {
                let snippets = sandbox.search_all(query, 3);
                for snip in snippets {
                    entries.push(MemoryEntry {
                        id: uuid::Uuid::new_v4(),
                        layer: MemoryLayer::L3,
                        category: MemoryCategory::Reference,
                        priority: Priority::Normal,
                        source: MemorySource::AutoExtracted,
                        title: format!(
                            "[SANDBOX] tool output L{}-L{}",
                            snip.line_start, snip.line_end
                        ),
                        content: format!("[TOOL OUTPUT] {}", snip.content),
                        embedding: None,
                        tags: vec!["sandbox".into(), "tool_output".into()],
                        relations: vec![],
                        confidence: 0.7,
                        access_count: 0,
                        staleness: 0.0,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        last_accessed_at: None,
                        scope: MemoryScope::default(),
                        session_id: None,
                        source_agent: None,
                        visibility: crate::types::AgentVisibility::default(),
                    });
                }
            }
        }

        // ── Step 7c: Hot code symbol injection ──
        if let Some(hot_ctx) = self.orchestrator.get_hot_symbols_context() {
            entries.push(MemoryEntry {
                id: uuid::Uuid::new_v4(),
                layer: MemoryLayer::L1,
                category: MemoryCategory::Reference,
                priority: Priority::Normal,
                source: MemorySource::AutoExtracted,
                title: "Hot Code Symbols".into(),
                content: hot_ctx,
                embedding: None,
                tags: vec!["hot_symbols".into(), "code".into()],
                relations: vec![],
                confidence: 0.9,
                access_count: 0,
                staleness: 0.0,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                last_accessed_at: None,
                scope: MemoryScope::default(),
                session_id: None,
                source_agent: None,
                visibility: crate::types::AgentVisibility::default(),
            });
        }

        // ── Step 7d: Symbol-memory linking ──
        // Reserve for future: link code symbols referenced in context to memory entries.
        // This is activated when code_context is populated by the code indexer.
        if code_context.is_some() {
            tracing::debug!("symbol-memory linking: code context present, linking reserved for Phase 4 get_callers/get_callees integration");
        }

        // Every backend search is deliberately over-inclusive for recall
        // quality. Before the prepared context is cached or exposed, enforce
        // the immutable Binding-derived lease again at this final boundary.
        entries.retain(|entry| memory_scope_visible_to_ctx(&entry.scope, turn));

        // ── Assemble PreparedContext ─────────────────────────────────────────
        let total_tokens = self.estimate_tokens_entries(&entries);
        let depth_scale = if total_tokens > budget.available {
            budget.available as f32 / total_tokens.max(1) as f32
        } else {
            1.0
        };

        let elapsed = _prepare_start.elapsed();
        self.perf_monitor.record_prepare_context(elapsed);
        tracing::debug!(
            elapsed_ms = elapsed.as_millis(),
            entries = entries.len(),
            total_tokens,
            "prepare_context complete"
        );

        let context = PreparedContext {
            entries,
            total_tokens,
            budget,
            depth_scale,
            prepared_at: Utc::now(),
            code_context,
        };
        self.store_prepared_context_cache(cache_key, cache_revision, &context);
        Ok(context)
    }

    /// Public entry point: prepare context with automatic code symbol injection.
    ///
    /// This wraps [`prepare_context`] and additionally injects relevant code
    /// symbols from the code indexer into [`PreparedContext::code_context`]
    /// when the query appears to be code-related.
    pub async fn build_context_with_code(
        &self,
        query: &str,
        messages: &[Message],
    ) -> Result<PreparedContext> {
        self.prepare_context(query, messages, None).await
    }

    /// Recall memories by relevance to `query`, returning up to `limit` entries.
    pub async fn recall(&self, query: &str, limit: usize) -> Result<Vec<MemoryEntry>> {
        let already_surfaced = HashSet::new();
        // Use a generous token budget so the limit parameter is the binding constraint.
        let token_budget = (limit as u32).saturating_mul(2000);
        let mut entries = self
            .orchestrator
            .recall_relevant(query, None, &already_surfaced, token_budget)
            .await?;
        entries.truncate(limit);
        Ok(entries)
    }

    /// List all memory entries in a specific layer.
    pub async fn list_layer_entries(
        &self,
        layer: crate::types::MemoryLayer,
    ) -> Result<Vec<crate::types::MemoryMeta>> {
        self.orchestrator.list_layer(layer).await
    }

    /// List full memory entries in a specific layer for product surfaces.
    pub async fn list_layer_full_entries(
        &self,
        layer: crate::types::MemoryLayer,
    ) -> Result<Vec<crate::types::MemoryEntry>> {
        self.orchestrator.store().search_by_layer(layer).await
    }

    /// Shared orchestrator handle for UI surfaces that need layer-level
    /// snapshots or L4 event subscriptions.
    pub fn orchestrator(&self) -> Arc<MemoryOrchestrator> {
        Arc::clone(&self.orchestrator)
    }

    /// List all memory entries across layers.
    pub async fn list_all_entries(&self) -> Result<Vec<crate::types::MemoryEntry>> {
        self.orchestrator.store().list_all().await
    }

    pub async fn store_aggregate(
        &self,
        stale_threshold: f32,
    ) -> Result<crate::store::MemoryStoreAggregate> {
        self.orchestrator.store().aggregate(stale_threshold).await
    }

    /// Durable L0 identity entries (role/language) used by startup self-checks
    /// and status projections (P9).
    pub async fn identity_entries(&self) -> Result<Vec<crate::types::MemoryEntry>> {
        self.orchestrator
            .store()
            .search_by_layer(crate::types::MemoryLayer::L0)
            .await
    }

    pub async fn authority_candidates(&self, query: AuthorityLookup) -> Result<Vec<MemoryEntry>> {
        self.orchestrator
            .store()
            .lookup_authority_candidates(query)
            .await
    }

    pub async fn tagged_candidates(
        &self,
        query: crate::store::TaggedLookup,
    ) -> Result<Vec<MemoryEntry>> {
        self.orchestrator
            .store()
            .lookup_tagged_candidates(query)
            .await
    }

    pub async fn fact_candidates(
        &self,
        scope: &crate::project_scope::MemoryScope,
        category: MemoryCategory,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>> {
        self.orchestrator
            .store()
            .lookup_fact_candidates(scope, category, limit)
            .await
    }

    pub(crate) async fn semantic_checkpoint_candidates(
        &self,
        scope: &crate::project_scope::MemoryScope,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>> {
        self.orchestrator
            .store()
            .search_semantic_checkpoints(scope, query, limit)
            .await
    }

    pub(crate) async fn kernel_kv_get_many(&self, keys: &[String]) -> Result<Vec<MemoryKeyValue>> {
        self.orchestrator.store().kv_get_many(keys).await
    }

    pub async fn scan_entries_page(
        &self,
        cursor: MemoryScanCursor,
        limit: usize,
    ) -> Result<MemoryScanPage> {
        self.orchestrator
            .store()
            .scan_entries_page(cursor, limit)
            .await
    }

    /// Read held scope migrations for operator review. These records remain
    /// excluded from normal recall until an explicit classification command is
    /// implemented by the management layer.
    pub async fn legacy_scope_migration_reports(
        &self,
    ) -> Result<Vec<crate::store::sqlite::LegacyScopeMigrationReport>> {
        self.orchestrator
            .store()
            .legacy_scope_migration_reports()
            .await
    }

    /// Snapshot the active token budget configuration used by the kernel.
    pub fn budget_config(&self) -> crate::config::BudgetConfig {
        self.config.budget.clone()
    }

    /// Recall entries through the in-process vector index.
    ///
    /// This is the runtime semantic recall source. The SQLite
    /// `MemoryStore::search_vector` path remains a backend capability boundary,
    /// but production context recall should use this method because embeddings
    /// are stored in `CognitiveContextManager::vector_index`.
    pub async fn vector_recall_candidates(
        &self,
        query: &str,
        already_surfaced: &HashSet<MemoryId>,
        limit: usize,
    ) -> Result<Vec<(MemoryEntry, f32)>> {
        let EmbeddingCapability::Remote { client } = &self.embedding_capability else {
            return Ok(Vec::new());
        };
        if self.vector_index.read().count() == 0 {
            return Ok(Vec::new());
        }
        let embedding = match client.embed_one(query).await {
            Ok(embedding) => embedding,
            Err(error) => {
                tracing::warn!(%error, "vector recall query embedding failed");
                return Ok(Vec::new());
            }
        };
        let scored = {
            let index = self.vector_index.read();
            index.search_with_filter(&embedding, limit.max(1) * 2, &|id| {
                !already_surfaced.contains(id)
            })?
        };
        let mut entries = Vec::new();
        for (id, score) in scored {
            if let Some(entry) = self.orchestrator.recall(&id).await? {
                entries.push((entry, score));
            }
            if entries.len() >= limit.max(1) {
                break;
            }
        }
        Ok(entries)
    }

    /// Return the current embedding capability level.
    #[must_use]
    pub fn embedding_capability(&self) -> &EmbeddingCapability {
        &self.embedding_capability
    }

    /// Return the search mode label for the current embedding capability.
    #[must_use]
    pub fn search_mode_label(&self) -> &'static str {
        self.embedding_capability.search_mode_label()
    }

    /// Access the pre-built BM25 session resume index, if available.
    ///
    /// The index is built at construction time from all persisted entries.
    #[must_use]
    pub fn session_resume(&self) -> Option<&SessionResume> {
        self.session_resume.as_ref()
    }

    // -----------------------------------------------------------------------
    // FTS5 Full-text search
    // -----------------------------------------------------------------------

    /// Perform full-text search across memories using FTS5.
    ///
    /// This method provides Hermes-Agent sessions-style FTS5 indexing with:
    /// - Category and layer filtering
    /// - Highlighted snippets for context
    /// - Matched keywords extraction
    /// - Both simple and boolean query modes
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let request = SearchMemoriesRequest {
    ///     query: "Rust async programming".to_string(),
    ///     category: Some(MemoryCategory::ProjectConvention),
    ///     limit: 5,
    ///     with_snippets: true,
    ///     with_keywords: true,
    ///     ..Default::default()
    /// };
    /// let result = manager.search_memories(request).await?;
    /// for (entry, snippet) in result.entries.iter().zip(result.snippets.iter()) {
    ///     println!("Title: {}", entry.title);
    ///     if let Some(snippet) = snippet {
    ///         println!("Snippet: {}", snippet.text);
    ///     }
    /// }
    /// ```
    pub async fn search_memories(
        &self,
        request: SearchMemoriesRequest,
    ) -> Result<SearchMemoriesResult> {
        // Build FTS5 query based on search mode
        let fts_query = match request.mode {
            SearchMode::Match => prepare_fts_query(&request.query),
            SearchMode::Boolean => request.query.clone(),
            SearchMode::Prefix => request
                .query
                .split_whitespace()
                .map(|w| format!("{}*", w))
                .collect::<Vec<_>>()
                .join(" "),
        };

        // Build search options
        let options = FtsSearchOptions {
            category: request.category,
            layer: request.layer,
            with_snippets: request.with_snippets,
            with_keywords: request.with_keywords,
        };

        // Execute search through the orchestrator's store
        let fts_result: FtsSearchResult = self
            .orchestrator
            .store()
            .search_fts_advanced(&fts_query, options, request.limit)
            .await?;

        // Convert snippets to SearchSnippet format
        let snippets: Vec<Option<SearchSnippet>> = fts_result
            .snippets
            .into_iter()
            .map(|opt| {
                opt.map(|text| SearchSnippet {
                    text,
                    positions: vec![],
                })
            })
            .collect();

        // Convert keywords
        let keywords: Vec<MatchedKeyword> = fts_result
            .keywords
            .into_iter()
            .map(|(keyword, count)| MatchedKeyword {
                keyword,
                count: count as u32,
            })
            .collect();

        // Collect unique categories found in results
        use std::collections::HashSet;
        let categories_found_set: HashSet<_> =
            fts_result.entries.iter().map(|e| e.category).collect();
        let categories_found: Vec<_> = categories_found_set.into_iter().collect();

        Ok(SearchMemoriesResult {
            entries: fts_result.entries,
            snippets,
            keywords,
            total_matches: fts_result.total_matches,
            query: request.query,
            categories_found,
            search_mode: self.search_mode_label().to_string(),
        })
    }

    /// Search only scopes already authorized by an exact Runtime Binding.
    ///
    /// Scope filtering happens inside each FTS query before ranking and
    /// limiting. This prevents a large unrelated project from displacing
    /// eligible results before the Memory kernel applies its final policy
    /// checks. Global rows are included by the store and remain subject to the
    /// kernel's visibility fence.
    pub(crate) async fn search_memories_in_scopes(
        &self,
        query: &str,
        scopes: &[MemoryScope],
        limit_per_scope: usize,
    ) -> Result<Vec<MemoryEntry>> {
        let mut entries = Vec::new();
        let mut seen = HashSet::new();
        for scope in scopes {
            for entry in self
                .orchestrator
                .store()
                .search_fts_scoped(query, scope, limit_per_scope.clamp(1, 128))
                .await?
            {
                if seen.insert(entry.id) {
                    entries.push(entry);
                }
            }
        }
        Ok(entries)
    }

    /// Quick FTS5 search with just a query string.
    ///
    /// Convenience method that creates a default request with the given query.
    pub async fn search(&self, query: &str) -> Result<Vec<MemoryEntry>> {
        let request = SearchMemoriesRequest {
            query: query.to_string(),
            ..Default::default()
        };
        let result = self.search_memories(request).await?;
        Ok(result.entries)
    }

    // -----------------------------------------------------------------------
    // Handoff
    // -----------------------------------------------------------------------

    /// Serialise the current session state into a [`HandoffData`] packet ready
    /// for cross-session resumption.
    pub async fn create_handoff(&self) -> Result<HandoffData> {
        let session_id = uuid::Uuid::new_v4().to_string();

        // Gather recent work items from L1 memories
        let recent = self
            .orchestrator
            .list_layer(MemoryLayer::L1)
            .await
            .unwrap_or_default();
        let work_items: Vec<WorkItem> = recent
            .iter()
            .map(|e| WorkItem {
                id: e.id.to_string(),
                title: e.title.clone(),
                description: e.title.clone(),
                status: WorkItemStatus::Pending,
                priority: e.priority,
            })
            .take(10)
            .collect();

        // Gather decisions from the decision thread store
        let decisions: Vec<Decision> = {
            let store = self.decisions.lock();
            let topics: Vec<String> = store
                .list_threads()
                .into_iter()
                .map(|s| s.to_owned())
                .collect();
            let mut result = Vec::new();
            for topic in &topics {
                if let Some(thread) = store.get_thread(topic) {
                    for entry in &thread.entries {
                        result.push(Decision {
                            id: entry.id.clone(),
                            summary: entry.summary.clone(),
                            rationale: entry.rationale.clone(),
                            status: entry.status,
                            made_at: entry.made_at,
                        });
                    }
                }
            }
            result
        };

        // Gather blockers from the tracked list
        let blockers: Vec<Blocker> = {
            let list = self.blockers.lock();
            list.iter()
                .enumerate()
                .map(|(i, desc)| Blocker {
                    id: format!("blocker-{i}"),
                    description: desc.clone(),
                    resolution_hint: None,
                })
                .collect()
        };

        // Build summary from last_action
        let last_action = self.last_action.lock().clone();
        let context_notes = format!(
            "Last action: {}. Session has {} memories and {} decisions logged.",
            last_action.as_deref().unwrap_or("none"),
            recent.len(),
            decisions.len(),
        );

        let handoff = self.handoff_mgr.create_handoff(
            &session_id,
            None, // current_task — not tracked yet
            work_items,
            vec![], // remaining items
            decisions,
            blockers,
            last_action.as_deref().unwrap_or(""),
            &context_notes,
        )?;
        self.handoff_mgr.save(&handoff)?;
        Ok(handoff)
    }

    /// Restore session state from a previously created [`HandoffData`] packet.
    pub async fn restore_handoff(&self, data: HandoffData) -> Result<()> {
        self.handoff_mgr.resume(data).await
    }

    // -----------------------------------------------------------------------
    // Session Restoration
    // -----------------------------------------------------------------------

    /// Restore memories from session history.
    ///
    /// This method reads the session history from `session_path` and extracts:
    /// - Memory entries from compressed messages
    /// - Decisions from decision messages
    /// - Work items from task-related messages
    ///
    /// Returns statistics about what was restored.
    pub async fn restore_from_session(
        &self,
        session_path: &std::path::Path,
        session_id: &str,
    ) -> Result<SessionRestoreStats> {
        use crate::types::{MemoryCategory, MemoryLayer, MemorySource, Priority};

        let mut stats = SessionRestoreStats::default();

        // Try to load the session file
        let contents = match std::fs::read_to_string(session_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("failed to read session file: {}", e);
                return Ok(stats);
            }
        };

        // Parse JSON or JSONL
        let messages: Vec<serde_json::Value> = if contents.trim().starts_with('{') {
            // Single JSON object
            match serde_json::from_str::<serde_json::Value>(&contents) {
                Ok(v) if v.get("messages").is_some() => v
                    .get("messages")
                    .and_then(|m: &serde_json::Value| m.as_array())
                    .cloned()
                    .unwrap_or_default(),
                _ => Vec::new(),
            }
        } else {
            // JSONL format - one JSON object per line
            contents
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .filter_map(|v: serde_json::Value| {
                    v.get("message").or_else(|| v.get("content")).cloned()
                })
                .collect()
        };

        // Extract memories from messages
        for msg in messages {
            // Try to extract content from message
            let text_opt: Option<String> = msg
                .as_str()
                .map(String::from)
                .or_else(|| {
                    msg.get("text")
                        .and_then(|v: &serde_json::Value| v.as_str())
                        .map(String::from)
                })
                .or_else(|| {
                    msg.get("content")
                        .and_then(|v: &serde_json::Value| v.as_str())
                        .map(String::from)
                });

            if let Some(text) = text_opt {
                // Skip very short messages
                if text.len() < 50 {
                    continue;
                }

                // Extract title from first line or truncate
                let first_line = text.lines().next().unwrap_or("");
                let title = if first_line.len() > 60 {
                    truncate_summary(first_line, 60)
                } else {
                    first_line.to_string()
                };

                // Create memory entry for this message
                let entry = MemoryEntry {
                    id: uuid::Uuid::new_v4(),
                    layer: MemoryLayer::L3, // Deep layer for restored memories
                    category: MemoryCategory::CompressedSummary,
                    priority: Priority::Normal,
                    source: MemorySource::Import,
                    title,
                    content: text.clone(),
                    embedding: None,
                    tags: vec!["restored".into(), "session".into(), session_id.into()],
                    relations: vec![],
                    confidence: 0.7, // Lower confidence for restored content
                    access_count: 0,
                    staleness: 0.0,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    last_accessed_at: None,
                    scope: MemoryScope::default(),
                    session_id: Some(session_id.to_string()),
                    source_agent: None,
                    visibility: crate::types::AgentVisibility::default(),
                };

                if let Err(e) = self.orchestrator.remember(entry).await {
                    tracing::warn!("failed to restore memory: {}", e);
                } else {
                    stats.memories_restored += 1;
                }
            }

            // Try to extract decisions
            if let Some(content_obj) = msg
                .get("content")
                .and_then(|v: &serde_json::Value| v.as_object())
            {
                if content_obj.contains_key("decision") || content_obj.contains_key("rationale") {
                    stats.decisions_restored += 1;
                }
            }
        }

        tracing::info!(
            "restored {} memories from session {}",
            stats.memories_restored,
            session_id
        );

        Ok(stats)
    }
}
