//! L2 – Project-specific layer.
//!
//! Stores project conventions, architectural decisions, API contracts, and
//! coding standards.  Entries are persistent across sessions and scoped to
//! a particular project or workspace.
//!
//! Characteristics:
//! - ~3000 token budget
//! - Persistent across sessions within the same project scope
//! - Can auto-discover context from CLAUDE.md, config.yaml etc.
//! - `tick()` applies staleness decay

use async_trait::async_trait;
use chrono::Utc;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

use crate::{
    code_indexer::{CodeIndexer, CodeSymbol, IndexStats},
    config::DriftConfig,
    layers::{LayerManager, Result},
    project_scope::MemoryScope,
    store::MemoryStore,
    types::{
        MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, PreparedContext,
        Priority, TokenBudget,
    },
};

/// Default maximum token budget for the project layer.
const DEFAULT_MAX_TOKENS: u64 = 3000;

/// Files that are recognised as project-context sources (in priority order).
const PROJECT_CONTEXT_FILES: &[&str] = &["CLAUDE.md", "AGENTS.md", "CONTRIBUTING.md", "README.md"];

/// Manager for the L2 project-specific layer.
pub struct ProjectLayer {
    store: Arc<dyn MemoryStore>,
    max_tokens: u64,
    workspace_root: Option<PathBuf>,
    drift: DriftConfig,
    /// Optional code indexer for project source code graph integration.
    code_indexer: Option<CodeIndexer>,
}

impl ProjectLayer {
    /// Create with default settings and no workspace root.
    pub fn new(store: Arc<dyn MemoryStore>) -> Self {
        Self {
            store,
            max_tokens: DEFAULT_MAX_TOKENS,
            workspace_root: None,
            drift: DriftConfig::default(),
            code_indexer: None,
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
            code_indexer: None,
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
        scope: MemoryScope,
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
            source_agent: None,
            visibility: crate::types::AgentVisibility::default(),
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
    pub async fn ingest_project_context(&self, scope: MemoryScope) -> Result<Vec<MemoryId>> {
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

    /// Initialise the code graph by indexing project source code.
    ///
    /// Creates a [`CodeIndexer`] for the given project root, walks all
    /// source files, and persists extracted symbols to the backing store.
    /// This is **optional** — only call when the project has source code
    /// to index.
    ///
    /// Returns statistics about the indexing run.
    pub async fn init_code_graph(&mut self, root: &Path) -> Result<IndexStats> {
        let mut indexer = CodeIndexer::new(root)?;
        let mut stats = IndexStats::default();

        for entry in walkdir::WalkDir::new(root)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let path = entry.path();
            if crate::code_indexer::IndexLanguage::is_indexable(path) {
                match indexer.index_file(path) {
                    Ok((symbols, edges)) => {
                        stats.files_processed += 1;
                        stats.symbols_found += symbols.len();
                        // Persist each symbol via the store trait
                        for sym in &symbols {
                            if self.store.insert_symbol(sym).await.is_err() {
                                // Continue even if one symbol fails
                            }
                        }
                        // Persist each edge (calls/imports/extends/implements)
                        for edge in &edges {
                            if self.store.insert_edge(edge).await.is_err() {
                                // Continue even if one edge fails
                            }
                        }
                    }
                    Err(_) => {
                        stats.files_processed += 1;
                    }
                }
            }
        }

        self.code_indexer = Some(indexer);
        Ok(stats)
    }

    /// Query the code graph for symbols relevant to the given search term.
    ///
    /// Uses FTS5 full-text search on the `code_symbols_fts` virtual table.
    /// Returns an empty vector if no code indexer is available (graceful
    /// degradation).
    pub async fn find_relevant_symbols(&self, query: &str, limit: usize) -> Vec<CodeSymbol> {
        if self.code_indexer.is_none() {
            return Vec::new();
        }
        self.store
            .search_symbols(query, limit)
            .await
            .unwrap_or_default()
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
            code_context: None,
        })
    }

    /// Apply staleness decay to L2 entries.
    async fn tick(&self) -> Result<()> {
        let entries = self.store.search_by_layer(MemoryLayer::L2).await?;
        let decay = self.drift.staleness_decay_per_day;

        for mut entry in entries {
            entry.staleness = (entry.staleness + decay).min(1.0);

            // Only prune if above the hard prune threshold and low priority.
            if entry.priority == Priority::Low && entry.staleness >= self.drift.prune_threshold {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DriftConfig;
    use crate::store::sqlite::SqliteStore;

    fn in_memory() -> Arc<dyn MemoryStore> {
        let tmp = Box::leak(Box::new(tempfile::TempDir::new().unwrap()));
        Arc::new(SqliteStore::open_path(&tmp.path().join("test.db")).unwrap())
    }

    #[tokio::test]
    async fn new_has_no_workspace_root() {
        let layer = ProjectLayer::new(in_memory());
        let files = layer.discover_project_context().await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn with_workspace_can_discover_context() {
        let drift = DriftConfig::default();
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::write(tmp.path().join("CLAUDE.md"), "Test project.").unwrap();

        let layer = ProjectLayer::with_workspace(in_memory(), tmp.path().to_path_buf(), 500, drift);
        assert_eq!(layer.max_tokens, 500);
        let files = layer.discover_project_context().await.unwrap();
        assert!(!files.is_empty());
    }

    #[tokio::test]
    async fn add_creates_entry() {
        let layer = ProjectLayer::new(in_memory());
        let id = layer
            .add(
                MemoryCategory::ProjectConvention,
                "coding-standard",
                "Use 4 spaces",
                Priority::High,
                MemorySource::Import,
                vec!["style".into()],
                MemoryScope::Project("repo-1".into()),
            )
            .await
            .unwrap();

        let entries = layer.load().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, id);
        assert_eq!(entries[0].layer, MemoryLayer::L2);
        assert_eq!(entries[0].category, MemoryCategory::ProjectConvention);
        assert_eq!(entries[0].title, "coding-standard");
        assert_eq!(entries[0].scope, MemoryScope::Project("repo-1".into()));
    }

    #[tokio::test]
    async fn insert_overrides_layer_to_l2() {
        let layer = ProjectLayer::new(in_memory());
        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer: MemoryLayer::L1,
            category: MemoryCategory::Decision,
            priority: Priority::Normal,
            source: MemorySource::AutoExtracted,
            title: "t".into(),
            content: "c".into(),
            embedding: None,
            tags: vec![],
            relations: vec![],
            confidence: 1.0,
            access_count: 0,
            staleness: 0.0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::default(),
            session_id: None,
            source_agent: None,
            visibility: crate::types::AgentVisibility::default(),
        };
        let id = layer.insert(entry).await.unwrap();
        let loaded = layer
            .load()
            .await
            .unwrap()
            .into_iter()
            .find(|e| e.id == id)
            .unwrap();
        assert_eq!(loaded.layer, MemoryLayer::L2);
    }

    #[tokio::test]
    async fn discover_project_context_finds_claude_md() {
        let tmp = tempfile::TempDir::new().unwrap();
        let claude_md = tmp.path().join("CLAUDE.md");
        std::fs::write(&claude_md, "# Project\n\nA test project.").unwrap();

        let layer = ProjectLayer::with_workspace(
            in_memory(),
            tmp.path().to_path_buf(),
            3000,
            DriftConfig::default(),
        );
        let files = layer.discover_project_context().await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, "CLAUDE.md");
        assert!(files[0].1.contains("Project"));
    }

    #[tokio::test]
    async fn discover_project_context_skips_empty_files() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::write(tmp.path().join("CLAUDE.md"), "").unwrap();

        let layer = ProjectLayer::with_workspace(
            in_memory(),
            tmp.path().to_path_buf(),
            3000,
            DriftConfig::default(),
        );
        let files = layer.discover_project_context().await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn discover_project_context_returns_empty_without_workspace() {
        let layer = ProjectLayer::new(in_memory());
        let files = layer.discover_project_context().await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn ingest_project_context_creates_entries() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::write(tmp.path().join("CLAUDE.md"), "Test project.").unwrap();

        let layer = ProjectLayer::with_workspace(
            in_memory(),
            tmp.path().to_path_buf(),
            3000,
            DriftConfig::default(),
        );
        let ids = layer
            .ingest_project_context(MemoryScope::Project("p1".into()))
            .await
            .unwrap();
        assert_eq!(ids.len(), 1);

        let entries = layer.load().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].title, "project:CLAUDE.md");
    }

    #[tokio::test]
    async fn ingest_project_context_skips_already_ingested() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::write(tmp.path().join("CLAUDE.md"), "Test.").unwrap();

        let layer = ProjectLayer::with_workspace(
            in_memory(),
            tmp.path().to_path_buf(),
            3000,
            DriftConfig::default(),
        );
        let ids1 = layer
            .ingest_project_context(MemoryScope::default())
            .await
            .unwrap();
        assert_eq!(ids1.len(), 1);
        let ids2 = layer
            .ingest_project_context(MemoryScope::default())
            .await
            .unwrap();
        assert!(ids2.is_empty());
    }

    #[tokio::test]
    async fn tick_prunes_stale_low_priority() {
        let drift = DriftConfig {
            staleness_decay_per_day: 0.9,
            prune_threshold: 0.5,
            ..Default::default()
        };
        let layer = ProjectLayer::with_workspace(
            in_memory(),
            std::path::PathBuf::from("/tmp/test"),
            3000,
            drift,
        );
        layer
            .add(
                MemoryCategory::ProjectConvention,
                "T",
                "C",
                Priority::Low,
                MemorySource::Import,
                vec![],
                MemoryScope::default(),
            )
            .await
            .unwrap();
        layer.tick().await.unwrap();
        assert!(layer.load().await.unwrap().is_empty());
    }

    #[test]
    fn layer_returns_l2() {
        assert_eq!(ProjectLayer::new(in_memory()).layer(), MemoryLayer::L2);
    }

    // -----------------------------------------------------------------------
    // T4: Code graph integration tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_no_symbols_when_no_code() {
        let layer = ProjectLayer::new(in_memory());
        let symbols = layer.find_relevant_symbols("auth", 5).await;
        assert!(symbols.is_empty());
    }

    #[tokio::test]
    async fn test_project_layer_injects_code_symbols() {
        let tmp = tempfile::TempDir::new().unwrap();
        let project_root = tmp.path().to_path_buf();

        // Create a sample Rust source file
        let src_dir = project_root.join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        let lib_rs = src_dir.join("lib.rs");
        std::fs::write(
            &lib_rs,
            r#"
/// Authenticate a user with the given credentials.
pub fn authenticate_user(username: &str, password: &str) -> bool {
    validate_credentials(username, password)
}

fn validate_credentials(username: &str, password: &str) -> bool {
    !username.is_empty() && !password.is_empty()
}

pub struct AuthService {
    pub enabled: bool,
}

pub enum AuthError {
    InvalidCredentials,
    ExpiredToken,
}
"#,
        )
        .unwrap();

        // Create a ProjectLayer with workspace and code_indexer set to None initially
        let mut layer = ProjectLayer::with_workspace(
            in_memory(),
            project_root.clone(),
            3000,
            DriftConfig::default(),
        );

        // Index the code
        let stats = layer.init_code_graph(&project_root).await.unwrap();
        assert!(stats.files_processed > 0);
        assert!(stats.symbols_found > 0);

        // Verify symbols were persisted — search via find_relevant_symbols
        let symbols = layer.find_relevant_symbols("authenticate", 10).await;
        assert!(!symbols.is_empty(), "should find authenticate_user symbol");
        assert!(symbols.iter().any(|s| s.name.contains("authenticate_user")));

        // Search for struct
        let struct_syms = layer.find_relevant_symbols("AuthService", 10).await;
        assert!(
            struct_syms.iter().any(|s| s.name.contains("AuthService")),
            "should find AuthService struct"
        );

        // Search for something not in the code
        let missing = layer.find_relevant_symbols("nonexistent_function", 5).await;
        assert!(missing.is_empty());
    }

    #[tokio::test]
    async fn test_auto_index_on_init() {
        let tmp = tempfile::TempDir::new().unwrap();
        let project_root = tmp.path().to_path_buf();

        // Create a Python source file
        let py_file = project_root.join("auth.py");
        std::fs::write(
            &py_file,
            "def authenticate_user(username, password):\n    return True\n\nclass TokenManager:\n    pass\n",
        )
        .unwrap();

        let mut layer = ProjectLayer::with_workspace(
            in_memory(),
            project_root.clone(),
            3000,
            DriftConfig::default(),
        );

        // Init code graph should work for Python too
        let stats = layer.init_code_graph(&project_root).await.unwrap();
        assert!(stats.files_processed > 0);
        assert!(stats.symbols_found > 0);

        let symbols = layer.find_relevant_symbols("authenticate_user", 5).await;
        assert!(!symbols.is_empty());
        assert!(symbols.iter().any(|s| s.name == "authenticate_user"));

        // TokenManager should also be indexed
        let class_syms = layer.find_relevant_symbols("TokenManager", 5).await;
        assert!(!class_syms.is_empty());
    }
}
