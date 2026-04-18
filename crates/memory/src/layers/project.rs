//! L2 – Project-specific layer.
//!
//! Stores project conventions, architectural decisions, API contracts, and
//! coding standards.  Entries are persistent across sessions and scoped to
//! a particular project or workspace.
//!
//! Characteristics:
//! - ~3000 token budget
//! - Persistent across sessions within the same project scope
//! - Can auto-discover context from COWD.md, .cowd.json etc.
//! - `tick()` applies staleness decay

use async_trait::async_trait;
use chrono::Utc;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

use crate::{
    config::DriftConfig,
    layers::{LayerManager, Result},
    store::MemoryStore,
    types::{
        MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, PreparedContext,
        Priority, TokenBudget,
    },
};

/// Default maximum token budget for the project layer.
const DEFAULT_MAX_TOKENS: u64 = 3000;

/// Files that are recognised as project-context sources (in priority order).
const PROJECT_CONTEXT_FILES: &[&str] = &[
    "COWD.md",
    ".cowd.json",
    "AGENTS.md",
    "CONTRIBUTING.md",
    "README.md",
];

/// Manager for the L2 project-specific layer.
pub struct ProjectLayer {
    store: Arc<dyn MemoryStore>,
    max_tokens: u64,
    workspace_root: Option<PathBuf>,
    drift: DriftConfig,
}

impl ProjectLayer {
    /// Create with default settings and no workspace root.
    pub fn new(store: Arc<dyn MemoryStore>) -> Self {
        Self {
            store,
            max_tokens: DEFAULT_MAX_TOKENS,
            workspace_root: None,
            drift: DriftConfig::default(),
        }
    }

    /// Create with a workspace root for project-context discovery.
    pub fn with_workspace(
        store: Arc<dyn MemoryStore>,
        workspace_root: PathBuf,
        max_tokens: u64,
        drift: DriftConfig,
    ) -> Self {
        Self {
            store,
            max_tokens,
            workspace_root: Some(workspace_root),
            drift,
        }
    }

    /// Load all L2 entries sorted by priority, truncated to `max_tokens`.
    pub async fn load(&self) -> Result<Vec<MemoryEntry>> {
        let entries = self.store.search_by_layer(MemoryLayer::L2).await?;
        Ok(truncate_to_budget(entries, self.max_tokens))
    }

    /// Add a project-level memory entry.
    pub async fn add(
        &self,
        category: MemoryCategory,
        title: &str,
        content: &str,
        priority: Priority,
        source: MemorySource,
        tags: Vec<String>,
        scope: Option<String>,
    ) -> Result<MemoryId> {
        let now = Utc::now();
        let entry = MemoryEntry {
            id: Uuid::new_v4(),
            layer: MemoryLayer::L2,
            category,
            priority,
            source,
            title: title.to_string(),
            content: content.to_string(),
            embedding: None,
            tags,
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: now,
            updated_at: now,
            last_accessed_at: None,
            scope,
            session_id: None,
        };
        let id = self.store.insert(&entry).await?;
        Ok(id)
    }

    /// Discover and return project context from known files in the workspace.
    ///
    /// Returns a list of `(filename, content)` pairs for each file found.
    pub async fn discover_project_context(&self) -> Result<Vec<(String, String)>> {
        let root = match &self.workspace_root {
            Some(p) => p.clone(),
            None => return Ok(Vec::new()),
        };

        let mut found = Vec::new();
        for filename in PROJECT_CONTEXT_FILES {
            let path = root.join(filename);
            if path.exists() {
                match tokio::fs::read_to_string(&path).await {
                    Ok(content) if !content.trim().is_empty() => {
                        found.push((filename.to_string(), content));
                    }
                    _ => {}
                }
            }
        }
        Ok(found)
    }

    /// Ingest project context files into the store as L2 entries.
    ///
    /// Already-ingested files (matched by title) are skipped.
    pub async fn ingest_project_context(&self, scope: Option<String>) -> Result<Vec<MemoryId>> {
        let files = self.discover_project_context().await?;
        let existing = self.store.search_by_layer(MemoryLayer::L2).await?;
        let existing_titles: std::collections::HashSet<String> =
            existing.into_iter().map(|e| e.title).collect();

        let mut ids = Vec::new();
        for (filename, content) in files {
            let title = format!("project:{filename}");
            if existing_titles.contains(&title) {
                continue;
            }
            // Truncate very long files to avoid blowing the token budget.
            let content = if content.len() > 8000 {
                format!("{}\n… (truncated)", &content[..8000])
            } else {
                content
            };
            let id = self
                .add(
                    MemoryCategory::ProjectConvention,
                    &title,
                    &content,
                    Priority::High,
                    MemorySource::Import,
                    vec!["project-context".to_string(), filename.clone()],
                    scope.clone(),
                )
                .await?;
            ids.push(id);
        }
        Ok(ids)
    }
}

// ---------------------------------------------------------------------------
// LayerManager implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl LayerManager for ProjectLayer {
    fn layer(&self) -> MemoryLayer {
        MemoryLayer::L2
    }

    /// Insert an entry, overriding its layer to L2.
    async fn insert(&self, mut entry: MemoryEntry) -> Result<MemoryId> {
        entry.layer = MemoryLayer::L2;
        let id = self.store.insert(&entry).await?;
        Ok(id)
    }

    async fn remove(&self, id: &MemoryId) -> Result<()> {
        self.store.delete(id).await
    }

    /// Prepare L2 context within the given token budget.
    async fn prepare_context(&self, budget: &TokenBudget) -> Result<PreparedContext> {
        let available = budget.available.min(self.max_tokens);
        let entries = self.store.search_by_layer(MemoryLayer::L2).await?;
        let kept = truncate_to_budget(entries, available);
        let used_tokens: u64 = kept.iter().map(|e| estimate_tokens(&e.content)).sum();

        Ok(PreparedContext {
            entries: kept,
            total_tokens: used_tokens,
            budget: budget.clone(),
            depth_scale: 0.7,
            prepared_at: Utc::now(),
        })
    }

    /// Apply staleness decay to L2 entries.
    async fn tick(&self) -> Result<()> {
        let entries = self.store.search_by_layer(MemoryLayer::L2).await?;
        let decay = self.drift.staleness_decay_per_day;

        for mut entry in entries {
            entry.staleness = (entry.staleness + decay).min(1.0);

            // Only prune if above the hard prune threshold and low priority.
            if entry.priority == Priority::Low
                && entry.staleness >= self.drift.prune_threshold
            {
                self.store.delete(&entry.id).await?;
                continue;
            }

            self.store.update(&entry).await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Sort entries by priority then recency and truncate to token budget.
fn truncate_to_budget(mut entries: Vec<MemoryEntry>, max_tokens: u64) -> Vec<MemoryEntry> {
    entries.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then(b.updated_at.cmp(&a.updated_at))
    });

    let mut used: u64 = 0;
    let mut kept = Vec::new();
    for e in entries {
        let tokens = estimate_tokens(&e.content);
        if used + tokens > max_tokens {
            break;
        }
        used += tokens;
        kept.push(e);
    }
    kept
}

/// Estimate token count from content length (4 chars ≈ 1 token).
fn estimate_tokens(content: &str) -> u64 {
    (content.len() as u64).div_ceil(4)
}

/// Canonicalise path for display (strips workspace root prefix).
#[allow(dead_code)]
fn rel_path<'a>(path: &'a Path, root: &Path) -> &'a Path {
    path.strip_prefix(root).unwrap_or(path)
}
