//! Multi-signal relevance scoring and dynamic memory loading.
//!
//! Combines five independent signals into a single relevance score used to
//! rank memory entries during context preparation:
//!
//! 1. **FTS** – keyword frequency in title, content and tags.
//! 2. **Vector** – cosine similarity between query and entry embeddings.
//! 3. **Time** – exponential decay since last access, boosted by access count.
//! 4. **Graph** – hop-distance in the knowledge graph between query entities
//!    and memory-associated entities.
//! 5. **Dependency** – frontmatter `provides`/`requires`/`affects` alignment
//!    with currently active memories.
//!
//! [`DynamicLoader`] orchestrates candidate retrieval, scoring and depth-scaled
//! content truncation so that the assembled context always fits within the
//! available token budget.

use std::collections::HashSet;

use chrono::{DateTime, Utc};

use crate::{
    error::MemoryError,
    store::{vector::VectorIndex, MemoryStore},
    types::{MemoryEntry, MemoryId, Priority, RelationKind},
};

// ---------------------------------------------------------------------------
// Result alias
// ---------------------------------------------------------------------------

type Result<T> = std::result::Result<T, MemoryError>;

// ---------------------------------------------------------------------------
// Weight configuration
// ---------------------------------------------------------------------------

/// Per-signal weights used when fusing the final relevance score.
///
/// The weights are **not** required to sum to 1.0; [`RelevanceScorer`] will
/// normalise them internally.
#[derive(Debug, Clone)]
pub struct RelevanceWeights {
    /// Weight for full-text keyword matching score.
    pub fts_weight: f32,
    /// Weight for vector semantic similarity score.
    pub vector_weight: f32,
    /// Weight for time-decay / recency score.
    pub time_weight: f32,
    /// Weight for knowledge-graph hop distance score.
    pub graph_weight: f32,
    /// Weight for frontmatter dependency alignment score.
    pub dependency_weight: f32,
}

impl Default for RelevanceWeights {
    fn default() -> Self {
        Self {
            fts_weight: 0.25,
            vector_weight: 0.30,
            time_weight: 0.15,
            graph_weight: 0.15,
            dependency_weight: 0.15,
        }
    }
}

// ---------------------------------------------------------------------------
// Score result types
// ---------------------------------------------------------------------------

/// Breakdown of each individual signal's contribution.
#[derive(Debug, Clone)]
pub struct SignalBreakdown {
    pub fts_score: f32,
    pub vector_score: f32,
    pub time_score: f32,
    pub graph_score: f32,
    pub dependency_score: f32,
}

/// A memory entry together with its composite relevance score.
#[derive(Debug, Clone)]
pub struct ScoredMemory {
    pub entry: MemoryEntry,
    /// Final weighted score in `[0.0, 1.0]`.
    pub score: f32,
    /// Per-signal breakdown for debugging / explainability.
    pub signals: SignalBreakdown,
}

// ---------------------------------------------------------------------------
// Scoring context
// ---------------------------------------------------------------------------

/// Contextual information supplied by the caller to assist scoring.
#[derive(Debug, Clone)]
pub struct ScoringContext {
    /// Wall-clock time to use for time-decay calculations.
    pub current_time: DateTime<Utc>,
    /// IDs of memory entries already loaded into the active context window.
    pub active_memory_ids: Vec<MemoryId>,
    /// Entities extracted from the current query (e.g. proper nouns, identifiers).
    pub query_entities: Vec<String>,
}

impl Default for ScoringContext {
    fn default() -> Self {
        Self {
            current_time: Utc::now(),
            active_memory_ids: Vec::new(),
            query_entities: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// RelevanceScorer
// ---------------------------------------------------------------------------

/// Scores a batch of [`MemoryEntry`] candidates against a query using five
/// independent signals fused via a configurable weighted sum.
pub struct RelevanceScorer {
    weights: RelevanceWeights,
}

impl RelevanceScorer {
    /// Create a scorer with custom signal weights.
    #[must_use] 
    pub fn new(weights: RelevanceWeights) -> Self {
        Self { weights }
    }

    /// Create a scorer with the default signal weights.
    #[must_use] 
    pub fn with_defaults() -> Self {
        Self::new(RelevanceWeights::default())
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Score every candidate in `candidates` and return them with their scores.
    ///
    /// The results are **not** sorted; callers should sort by `score` as needed.
    pub async fn score(
        &self,
        query: &str,
        query_embedding: Option<&[f32]>,
        candidates: &[MemoryEntry],
        store: &dyn MemoryStore,
        vector_index: &VectorIndex,
        context: &ScoringContext,
    ) -> Result<Vec<ScoredMemory>> {
        // Build a lookup: memory_id -> vector score (from VectorIndex).
        let vector_scores = self.build_vector_scores(query_embedding, candidates, vector_index);

        let mut results = Vec::with_capacity(candidates.len());
        for entry in candidates {
            let fts = self.score_fts(query, entry);
            let vector = *vector_scores.get(&entry.id).unwrap_or(&0.0);
            let time = self.score_time(entry, context.current_time);
            let graph = self
                .score_graph(entry, &context.query_entities, store)
                .await;
            let dependency = self
                .score_dependency(entry, &context.active_memory_ids, store)
                .await;

            let signals = SignalBreakdown {
                fts_score: fts,
                vector_score: vector,
                time_score: time,
                graph_score: graph,
                dependency_score: dependency,
            };
            let score = self.combine_scores(&signals);
            results.push(ScoredMemory {
                entry: entry.clone(),
                score,
                signals,
            });
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Signal 1: FTS keyword matching
    // -----------------------------------------------------------------------

    /// Score based on keyword hit rate across title (×2), content, and tags (×1.5).
    fn score_fts(&self, query: &str, entry: &MemoryEntry) -> f32 {
        if query.is_empty() {
            return 0.0;
        }
        let query_words: Vec<&str> = query.split_whitespace().collect();
        if query_words.is_empty() {
            return 0.0;
        }

        let mut total_hits = 0.0_f32;
        let max_possible = query_words.len() as f32 * (2.0 + 1.0 + 1.5); // title+content+tags

        for word in &query_words {
            let w = word.to_lowercase();
            // Title – weight 2.0
            if entry.title.to_lowercase().contains(w.as_str()) {
                total_hits += 2.0;
            }
            // Content – weight 1.0
            let content_lower = entry.content.to_lowercase();
            let content_words: Vec<&str> = content_lower.split_whitespace().collect();
            let hits_in_content = content_words.iter().filter(|cw| cw.contains(w.as_str())).count();
            if hits_in_content > 0 {
                // Normalised by content length to avoid bias toward long entries
                let content_len = content_words.len().max(1) as f32;
                total_hits += 1.0 + (hits_in_content as f32 / content_len).min(0.5);
            }
            // Tags – weight 1.5
            if entry.tags.iter().any(|t| t.to_lowercase().contains(w.as_str())) {
                total_hits += 1.5;
            }
        }

        (total_hits / max_possible).min(1.0)
    }

    // -----------------------------------------------------------------------
    // Signal 2: Vector semantic similarity
    // -----------------------------------------------------------------------

    /// Pre-compute cosine similarity between `query_embedding` and every candidate.
    ///
    /// We use the `VectorIndex` if possible; falls back to inline computation
    /// when the entry has an embedding but is absent from the index.
    fn build_vector_scores(
        &self,
        query_embedding: Option<&[f32]>,
        candidates: &[MemoryEntry],
        vector_index: &VectorIndex,
    ) -> std::collections::HashMap<MemoryId, f32> {
        let mut map = std::collections::HashMap::new();
        let Some(qe) = query_embedding else {
            return map;
        };
        if qe.is_empty() {
            return map;
        }

        // Get top-k from vector index (covers entries that are indexed).
        let limit = candidates.len().max(1);
        if let Ok(results) = vector_index.search(qe, limit) {
            for (id, sim) in results {
                // Cosine similarity is in [-1, 1]; normalise to [0, 1].
                map.insert(id, f32::midpoint(sim, 1.0));
            }
        }

        // For candidates not in the index but with an inline embedding,
        // fall back to direct computation.
        for entry in candidates {
            if map.contains_key(&entry.id) {
                continue;
            }
            if let Some(emb) = entry.embedding.as_deref() {
                if let Some(sim) = cosine_similarity(qe, emb) {
                    map.insert(entry.id, f32::midpoint(sim, 1.0));
                }
            }
        }

        map
    }

    // -----------------------------------------------------------------------
    // Signal 3: Time decay
    // -----------------------------------------------------------------------

    /// Exponential decay since last access, with an access-count bonus.
    ///
    /// `score = 0.95 ^ days_since_last_access * min(1 + access_count * 0.1, 2.0)`
    fn score_time(&self, entry: &MemoryEntry, now: DateTime<Utc>) -> f32 {
        const DECAY_FACTOR: f32 = 0.95;

        let reference = entry.last_accessed_at.unwrap_or(entry.updated_at);
        let days = (now - reference).num_seconds().max(0) as f32 / 86_400.0;
        let base_score = DECAY_FACTOR.powf(days);

        // Access-count bonus: each access adds 10 %, capped at ×2.
        let access_bonus = (1.0 + entry.access_count as f32 * 0.1).min(2.0);
        (base_score * access_bonus).min(1.0)
    }

    // -----------------------------------------------------------------------
    // Signal 4: Knowledge-graph hop distance
    // -----------------------------------------------------------------------

    /// Score based on shortest hop distance between query entities and the
    /// entities reachable from this memory entry via the relations graph.
    ///
    /// 1-hop → 1.0, 2-hop → 0.5, 3-hop → 0.25, >3 hops → 0.0
    async fn score_graph(
        &self,
        entry: &MemoryEntry,
        query_entities: &[String],
        store: &dyn MemoryStore,
    ) -> f32 {
        if query_entities.is_empty() || entry.relations.is_empty() {
            return 0.0;
        }

        // Collect the IDs that are directly related to this entry (1 hop).
        let related_ids: HashSet<String> = entry
            .relations
            .iter()
            .map(|r| r.target_id.to_string())
            .collect();

        let entry_id_str = entry.id.to_string();

        let mut best_score: f32 = 0.0;

        for entity in query_entities {
            // Check direct 1-hop match (query entity is a related entry id).
            if related_ids.contains(entity) {
                return 1.0; // Perfect match, no need to continue.
            }

            // Try 2- and 3-hop traversal via FTS-based heuristic.
            // We cannot do arbitrary graph BFS through the MemoryStore trait,
            // so we check whether the entity appears in the FTS index and then
            // look for an overlap with our directly related IDs.
            if let Ok(fts_hits) = store.search_fts(entity, 20).await {
                for hit in &fts_hits {
                    let hit_id = hit.id.to_string();
                    // 1 hop: hit IS the entry itself → already checked above.
                    // 2 hops: hit is in our directly related set.
                    if related_ids.contains(&hit_id) {
                        best_score = best_score.max(0.5);
                        continue;
                    }
                    // 3 hops: any of hit's relations overlap with our relations.
                    for rel in &hit.relations {
                        let rel_id = rel.target_id.to_string();
                        if related_ids.contains(&rel_id) || rel_id == entry_id_str {
                            best_score = best_score.max(0.25);
                        }
                    }
                }
            }
        }

        best_score
    }

    // -----------------------------------------------------------------------
    // Signal 5: Dependency graph (frontmatter)
    // -----------------------------------------------------------------------

    /// Score based on alignment between this entry's `provides` / `DependsOn`
    /// relations and the `requires`/`affects` of currently active memories.
    ///
    /// This signal is computed purely from the in-memory [`MemoryEntry::relations`]
    /// field (the `DependsOn` and `Related` relation kinds track the frontmatter
    /// `requires`, `provides`, and `affects` semantics).
    async fn score_dependency(
        &self,
        entry: &MemoryEntry,
        active_context: &[MemoryId],
        store: &dyn MemoryStore,
    ) -> f32 {
        if active_context.is_empty() {
            return 0.0;
        }

        // The entry "provides" something if other entries declare DependsOn → this entry.
        // Conversely the entry itself can have DependsOn relations pointing to the active set.
        let entry_id = entry.id;

        let mut score = 0.0_f32;
        let mut checked = 0usize;

        for active_id in active_context {
            let Ok(Some(active)) = store.get(active_id).await else {
                continue;
            };
            checked += 1;

            // Case A: active memory DependsOn this entry (this entry "provides" what active needs).
            let active_depends_on_us = active
                .relations
                .iter()
                .any(|r| r.target_id == entry_id && r.kind == RelationKind::DependsOn);

            // Case B: this entry DependsOn the active memory (we're building on active context).
            let we_depend_on_active = entry
                .relations
                .iter()
                .any(|r| r.target_id == *active_id && r.kind == RelationKind::DependsOn);

            // Case C: this entry is Related to the active memory.
            let related_to_active = entry
                .relations
                .iter()
                .any(|r| r.target_id == *active_id && r.kind == RelationKind::Related);

            if active_depends_on_us {
                score += 1.0;
            } else if we_depend_on_active {
                score += 0.8;
            } else if related_to_active {
                score += 0.4;
            }
        }

        if checked == 0 {
            0.0
        } else {
            (score / checked as f32).min(1.0)
        }
    }

    // -----------------------------------------------------------------------
    // Fusion
    // -----------------------------------------------------------------------

    /// Compute the weighted combination of all five signals, normalised so that
    /// the result is in `[0.0, 1.0]`.
    fn combine_scores(&self, signals: &SignalBreakdown) -> f32 {
        let w = &self.weights;
        let total_weight = w.fts_weight
            + w.vector_weight
            + w.time_weight
            + w.graph_weight
            + w.dependency_weight;

        if total_weight == 0.0 {
            return 0.0;
        }

        let weighted_sum = w.fts_weight * signals.fts_score
            + w.vector_weight * signals.vector_score
            + w.time_weight * signals.time_score
            + w.graph_weight * signals.graph_score
            + w.dependency_weight * signals.dependency_score;

        (weighted_sum / total_weight).clamp(0.0, 1.0)
    }
}

// ---------------------------------------------------------------------------
// ReadDepth & ReadDepthScaler
// ---------------------------------------------------------------------------

/// How much of each memory entry's content to load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadDepth {
    /// Only frontmatter metadata – a few dozen bytes.
    MetaOnly,
    /// Summary: the first 500 characters of the content.
    Summary,
    /// Full untruncated content.
    Full,
}

/// Scales the read depth of memory content based on the available token budget
/// so that context window pressure is relieved automatically.
pub struct ReadDepthScaler;

impl ReadDepthScaler {
    /// Decide how deeply to read each candidate given the available token budget
    /// and the number of candidates.
    #[must_use]
    pub fn scale(available_tokens: u32, total_candidates: usize) -> ReadDepth {
        let per_item_budget = available_tokens / total_candidates.max(1) as u32;
        if per_item_budget > 2_000 {
            ReadDepth::Full
        } else if per_item_budget > 500 {
            ReadDepth::Summary
        } else {
            ReadDepth::MetaOnly
        }
    }

    /// Truncate `content` according to the requested depth.
    ///
    /// - [`ReadDepth::Full`] – returns the content unchanged.
    /// - [`ReadDepth::Summary`] – returns the first 500 chars (on a char boundary).
    /// - [`ReadDepth::MetaOnly`] – returns an empty string.
    #[must_use]
    pub fn truncate_content(content: &str, depth: &ReadDepth) -> String {
        match depth {
            ReadDepth::Full => content.to_string(),
            ReadDepth::Summary => {
                const SUMMARY_CHARS: usize = 500;
                if content.len() <= SUMMARY_CHARS {
                    content.to_string()
                } else {
                    // Truncate on a char boundary to avoid splitting multi-byte chars.
                    let end = content
                        .char_indices()
                        .nth(SUMMARY_CHARS)
                        .map_or(content.len(), |(i, _)| i);
                    content[..end].to_string()
                }
            }
            ReadDepth::MetaOnly => String::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// DynamicLoader
// ---------------------------------------------------------------------------

/// High-level orchestrator that retrieves candidate memories, scores them with
/// five signals, applies read-depth scaling, and returns the top results that
/// fit within the token budget.
pub struct DynamicLoader {
    scorer: RelevanceScorer,
}

impl DynamicLoader {
    /// Create with default relevance weights.
    #[must_use] 
    pub fn new() -> Self {
        Self {
            scorer: RelevanceScorer::with_defaults(),
        }
    }

    /// Create with a pre-configured scorer.
    #[must_use] 
    pub fn with_scorer(scorer: RelevanceScorer) -> Self {
        Self { scorer }
    }

    /// Load the most relevant memories for `query`.
    ///
    /// # Steps
    /// 1. Retrieve candidates via FTS and (if `query_embedding` is given) vector search.
    /// 2. Filter out IDs that are already in `already_surfaced`.
    /// 3. Score remaining candidates with five signals.
    /// 4. Sort by score descending.
    /// 5. Apply [`ReadDepthScaler`] to truncate content to fit `token_budget`.
    pub async fn load_relevant(
        &self,
        query: &str,
        query_embedding: Option<&[f32]>,
        store: &dyn MemoryStore,
        vector_index: &VectorIndex,
        already_surfaced: &HashSet<MemoryId>,
        token_budget: u32,
    ) -> Result<Vec<ScoredMemory>> {
        // ── 1. Gather candidates ────────────────────────────────────────────
        let fts_limit = 50_usize;
        let mut candidates: Vec<MemoryEntry> = store.search_fts(query, fts_limit).await?;

        // Vector candidates.
        if let Some(qe) = query_embedding {
            if !qe.is_empty() {
                if let Ok(vec_hits) = vector_index.search(qe, 50) {
                    for (id, _score) in vec_hits {
                        if !candidates.iter().any(|c| c.id == id) {
                            if let Ok(Some(entry)) = store.get(&id).await {
                                candidates.push(entry);
                            }
                        }
                    }
                }
            }
        }

        // ── 2. Filter already surfaced ──────────────────────────────────────
        candidates.retain(|e| !already_surfaced.contains(&e.id));

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        // ── 3. Score ────────────────────────────────────────────────────────
        let context = ScoringContext {
            current_time: Utc::now(),
            active_memory_ids: already_surfaced.iter().copied().collect(),
            query_entities: extract_entities(query),
        };
        let mut scored = self
            .scorer
            .score(query, query_embedding, &candidates, store, vector_index, &context)
            .await?;

        // ── 4. Sort by score descending ─────────────────────────────────────
        scored.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

        // ── 5. Apply read-depth scaling and token budget ─────────────────────
        let depth = ReadDepthScaler::scale(token_budget, scored.len());

        // Rough estimation: 1 token ≈ 4 bytes. Truncate to budget.
        let mut total_tokens: u32 = 0;
        let mut result = Vec::with_capacity(scored.len());

        for mut sm in scored {
            let truncated = ReadDepthScaler::truncate_content(&sm.entry.content, &depth);
            let estimated_tokens = (truncated.len() / 4 + 1) as u32;

            if total_tokens + estimated_tokens > token_budget {
                break;
            }

            sm.entry.content = truncated;
            total_tokens += estimated_tokens;
            result.push(sm);
        }

        Ok(result)
    }
}

impl Default for DynamicLoader {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Backwards-compatible free function (kept for existing callers)
// ---------------------------------------------------------------------------

/// Computes a relevance score for a memory entry given a query context.
///
/// Returns a value in `[0.0, 1.0]` where `1.0` is maximally relevant.
/// This is a lightweight single-entry version that does not require the async
/// multi-signal pipeline; use [`DynamicLoader`] for full scoring.
#[must_use]
pub fn score(entry: &MemoryEntry, query_embedding: Option<&[f32]>) -> f32 {
    let recency = recency_score(entry);
    let frequency = frequency_score(entry);
    let priority = priority_score(entry.priority);
    let semantic = query_embedding
        .and_then(|q| cosine_similarity(q, entry.embedding.as_deref()?))
        .unwrap_or(0.5);

    // Weighted combination – weights sum to 1.0.
    0.30 * recency + 0.20 * frequency + 0.20 * priority + 0.30 * semantic
}

fn recency_score(entry: &MemoryEntry) -> f32 {
    let age_days = (Utc::now() - entry.updated_at).num_days().max(0) as f32;
    // Exponential decay with half-life of 30 days.
    (-age_days / 30.0_f32.ln_1p()).exp()
}

fn frequency_score(entry: &MemoryEntry) -> f32 {
    // Logarithmic scaling capped at 100 accesses → 1.0.
    (1.0 + entry.access_count as f32).ln() / (1.0 + 100.0_f32).ln()
}

fn priority_score(priority: Priority) -> f32 {
    match priority {
        Priority::Critical => 1.0,
        Priority::High => 0.75,
        Priority::Normal => 0.5,
        Priority::Low => 0.25,
    }
}

// ---------------------------------------------------------------------------
// Math helpers
// ---------------------------------------------------------------------------

fn cosine_similarity(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() || a.is_empty() {
        return None;
    }
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        None
    } else {
        Some(dot / (norm_a * norm_b))
    }
}

// ---------------------------------------------------------------------------
// Entity extraction heuristic
// ---------------------------------------------------------------------------

/// Very lightweight entity extraction: return tokens that look like proper
/// nouns (start with uppercase), identifiers (`snake_case` / camelCase), or
/// file paths.
fn extract_entities(query: &str) -> Vec<String> {
    query
        .split_whitespace()
        .filter(|w| {
            let first = w.chars().next().unwrap_or(' ');
            first.is_uppercase()
                || w.contains('_')
                || w.contains('/')
                || (w.len() > 4 && w.chars().any(char::is_uppercase) && w.chars().any(char::is_lowercase))
        })
        .map(|w| w.trim_matches(|c: char| !c.is_alphanumeric() && c != '_' && c != '/').to_string())
        .filter(|w| !w.is_empty())
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    use crate::types::{MemoryCategory, MemoryLayer, MemorySource};
    use crate::MemoryScope;

    fn make_entry(title: &str, content: &str, tags: Vec<&str>) -> MemoryEntry {
        MemoryEntry {
            id: Uuid::new_v4(),
            layer: MemoryLayer::L1,
            category: MemoryCategory::Reference,
            priority: Priority::Normal,
            source: MemorySource::UserExplicit,
            title: title.to_string(),
            content: content.to_string(),
            embedding: None,
            tags: tags.into_iter().map(|s| s.to_string()).collect(),
            relations: Vec::new(),
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
        }
    }

    #[test]
    fn fts_score_title_hit_weighted_higher() {
        let scorer = RelevanceScorer::with_defaults();
        let entry_with_title_hit = make_entry("rust programming", "unrelated text", vec![]);
        let entry_content_only = make_entry("unrelated title", "rust programming is great", vec![]);

        let score_title = scorer.score_fts("rust", &entry_with_title_hit);
        let score_content = scorer.score_fts("rust", &entry_content_only);

        assert!(score_title > score_content, "title hit should score higher");
    }

    #[test]
    fn fts_score_tag_match() {
        let scorer = RelevanceScorer::with_defaults();
        let entry = make_entry("no match", "no match", vec!["rust", "async"]);
        let score = scorer.score_fts("rust", &entry);
        assert!(score > 0.0, "tag hit should produce non-zero score");
    }

    #[test]
    fn time_score_recent_higher() {
        let scorer = RelevanceScorer::with_defaults();
        let now = Utc::now();

        let mut recent = make_entry("r", "r", vec![]);
        recent.last_accessed_at = Some(now - chrono::Duration::hours(1));

        let mut old = make_entry("o", "o", vec![]);
        old.last_accessed_at = Some(now - chrono::Duration::days(60));

        let s_recent = scorer.score_time(&recent, now);
        let s_old = scorer.score_time(&old, now);

        assert!(s_recent > s_old);
    }

    #[test]
    fn time_score_access_count_bonus() {
        let scorer = RelevanceScorer::with_defaults();
        let now = Utc::now();

        let mut low_access = make_entry("e", "e", vec![]);
        low_access.last_accessed_at = Some(now - chrono::Duration::days(5));
        low_access.access_count = 1;

        let mut high_access = make_entry("e", "e", vec![]);
        high_access.last_accessed_at = Some(now - chrono::Duration::days(5));
        high_access.access_count = 10;

        let s_low = scorer.score_time(&low_access, now);
        let s_high = scorer.score_time(&high_access, now);
        assert!(s_high > s_low);
    }

    #[test]
    fn read_depth_scaler_thresholds() {
        assert_eq!(ReadDepthScaler::scale(200_000, 10), ReadDepth::Full);
        assert_eq!(ReadDepthScaler::scale(10_000, 10), ReadDepth::Summary);
        assert_eq!(ReadDepthScaler::scale(1_000, 10), ReadDepth::MetaOnly);
    }

    #[test]
    fn truncate_summary_length() {
        let long_content = "a".repeat(1000);
        let truncated = ReadDepthScaler::truncate_content(&long_content, &ReadDepth::Summary);
        assert_eq!(truncated.len(), 500);
    }

    #[test]
    fn truncate_meta_only_empty() {
        let result = ReadDepthScaler::truncate_content("some content", &ReadDepth::MetaOnly);
        assert!(result.is_empty());
    }

    #[test]
    fn combine_scores_normalises() {
        let scorer = RelevanceScorer::with_defaults();
        let signals = SignalBreakdown {
            fts_score: 1.0,
            vector_score: 1.0,
            time_score: 1.0,
            graph_score: 1.0,
            dependency_score: 1.0,
        };
        let combined = scorer.combine_scores(&signals);
        assert!((combined - 1.0).abs() < 1e-6, "all-ones signals should produce 1.0");
    }

    #[test]
    fn combine_scores_zero_weights() {
        let scorer = RelevanceScorer::new(RelevanceWeights {
            fts_weight: 0.0,
            vector_weight: 0.0,
            time_weight: 0.0,
            graph_weight: 0.0,
            dependency_weight: 0.0,
        });
        let signals = SignalBreakdown {
            fts_score: 1.0,
            vector_score: 1.0,
            time_score: 1.0,
            graph_score: 1.0,
            dependency_score: 1.0,
        };
        assert_eq!(scorer.combine_scores(&signals), 0.0);
    }

    #[test]
    fn extract_entities_finds_capitalized() {
        let entities = extract_entities("Fix the RustParser and update src/lib.rs");
        assert!(entities.contains(&"RustParser".to_string()) || entities.iter().any(|e| e.contains("RustParser")));
    }
}
