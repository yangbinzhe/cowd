//! Project-scoped memory management.
//!
//! Each registered project gets its own SQLite database (`memory_<12-char-hash>.db`)
//! stored alongside the global `memory.db`.  The [`ProjectScopeManager`] tracks
//! registered projects, provides per-project stores, and allows switching the
//! active project at runtime.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::code_indexer::{CodeIndexer, CodeSymbol, SymbolEdge};
use crate::config::StoreConfig;
use crate::entity::{Entity, EntityType, KnowledgeGraph};
use crate::error::MemoryError;
use crate::store::sqlite::SqliteStore;

// ---------------------------------------------------------------------------
// MemoryScope
// ---------------------------------------------------------------------------

/// Scoping level for memory entries — determines which store an entry belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemoryScope {
    /// Global scope — shared across all projects and sessions.
    Global,
    /// Project-scoped memory, keyed by the derived project ID.
    Project(String),
    /// Session-scoped memory, keyed by a session identifier.
    Session(String),
    /// Agent-scoped memory, keyed by an agent identifier.
    Agent(String),
}

impl MemoryScope {
    /// Returns a unique string key suitable for store routing.
    ///
    /// # Examples
    ///
    /// ```
    /// use cowd_memory::project_scope::MemoryScope;
    ///
    /// assert_eq!(MemoryScope::Global.scope_key(), "global");
    /// assert_eq!(MemoryScope::Project("abc".into()).scope_key(), "project_abc");
    /// assert_eq!(MemoryScope::Session("s1".into()).scope_key(), "session_s1");
    /// assert_eq!(MemoryScope::Agent("a1".into()).scope_key(), "agent_a1");
    /// ```
    pub fn scope_key(&self) -> String {
        match self {
            MemoryScope::Global => "global".to_string(),
            MemoryScope::Project(id) => format!("project_{id}"),
            MemoryScope::Session(id) => format!("session_{id}"),
            MemoryScope::Agent(id) => format!("agent_{id}"),
        }
    }

    /// Returns true if this scope is Global (visible everywhere).
    pub fn is_global(&self) -> bool {
        matches!(self, MemoryScope::Global)
    }
}

impl std::fmt::Display for MemoryScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.scope_key())
    }
}

impl std::str::FromStr for MemoryScope {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "global" => Ok(MemoryScope::Global),
            _ if s.starts_with("project_") => Ok(MemoryScope::Project(s[8..].to_string())),
            _ if s.starts_with("session_") => Ok(MemoryScope::Session(s[8..].to_string())),
            _ if s.starts_with("agent_") => Ok(MemoryScope::Agent(s[6..].to_string())),
            other => Err(format!("unknown scope key: {other}")),
        }
    }
}

impl Default for MemoryScope {
    fn default() -> Self {
        MemoryScope::Session(String::new())
    }
}

// ---------------------------------------------------------------------------
// ProjectManifest
// ---------------------------------------------------------------------------

/// Metadata about a registered project.
#[derive(Debug, Clone)]
pub struct ProjectManifest {
    /// Derived hash ID from the canonical project path (16 hex chars).
    pub project_id: String,
    /// Canonical filesystem path of the project.
    pub path: PathBuf,
    /// Human-readable project name (may be empty initially).
    pub name: String,
    /// When the project was first registered.
    pub indexed_at: DateTime<Utc>,
    /// Timestamp of the most recent interaction.
    pub last_activity: DateTime<Utc>,
    /// Modification timestamps (Unix seconds) of indexed source files,
    /// keyed by absolute path.  Used to detect staleness so the KG
    /// can be auto-rebuilt when source files change.
    pub indexed_file_mtimes: HashMap<String, u64>,
}

// ---------------------------------------------------------------------------
// ProjectScopeManager
// ---------------------------------------------------------------------------

/// Internal mutable state — protected by [`Mutex`] for thread safety.
struct Inner {
    /// Path to the global memory database file.
    global_path: PathBuf,
    /// The global store (always available, never destroyed).
    global_store: SqliteStore,
    /// All registered projects, keyed by project ID.
    projects: HashMap<String, ProjectManifest>,
    /// The currently active project ID.
    active_project: Option<String>,
    /// Cached per-project stores, keyed by project ID.
    project_stores: HashMap<String, SqliteStore>,
    /// Optional callback invoked after each project registration.
    on_project_registered: Option<Box<dyn Fn(&PathBuf) + Send + Sync>>,
}

/// Manages global and per-project memory stores.
///
/// # Usage
///
/// ```rust,no_run
/// use std::path::PathBuf;
/// use cowd_memory::project_scope::ProjectScopeManager;
///
/// let manager = ProjectScopeManager::new(PathBuf::from("memory.db")).unwrap();
/// let pid = manager.register_project(std::path::Path::new("/my/project")).unwrap();
/// manager.switch_project(&pid).unwrap();
/// assert!(manager.current_project().is_some());
/// ```
pub struct ProjectScopeManager {
    inner: Mutex<Inner>,
}

impl ProjectScopeManager {
    /// Create a new manager with a global store at `global_path`.
    ///
    /// The global store is opened immediately; per-project stores are created
    /// lazily on [`register_project`](Self::register_project).
    pub fn new(global_path: PathBuf) -> Result<Self, MemoryError> {
        let config = StoreConfig {
            sqlite_path: global_path.clone(),
            ..Default::default()
        };
        let global_store = SqliteStore::open(&config)?;

        Ok(Self {
            inner: Mutex::new(Inner {
                global_path,
                global_store,
                projects: HashMap::new(),
                active_project: None,
                project_stores: HashMap::new(),
                on_project_registered: None,
            }),
        })
    }

    /// Set a callback to be invoked after each successful project registration.
    ///
    /// The callback receives the canonical project path and is called after
    /// the project store is opened and the project knowledge graph is built.
    pub fn on_project_registered<F>(self, callback: F) -> Self
    where
        F: Fn(&PathBuf) + Send + Sync + 'static,
    {
        self.inner.lock().unwrap().on_project_registered = Some(Box::new(callback));
        self
    }

    /// Register a project at `path`, returning its derived project ID.
    ///
    /// The project ID is a deterministic hex hash of the canonical path.
    /// Calling this with the same path multiple times is **idempotent**:
    /// it returns the same ID and does not create duplicate stores.
    pub fn register_project(&self, path: &Path) -> Result<String, MemoryError> {
        let canonical = path.canonicalize().map_err(|e| {
            MemoryError::Store(format!("failed to canonicalize path: {e}"))
        })?;
        let project_id = hash_path(&canonical);
        let db_filename = format!("memory_{}.db", &project_id[..12.min(project_id.len())]);
        let db_path = if let Some(parent) = self.inner.lock().unwrap().global_path.parent() {
            parent.join(&db_filename)
        } else {
            PathBuf::from(&db_filename)
        };

        let mut inner = self.inner.lock().unwrap();

        // Idempotent: return existing ID if already registered.
        if let Some(existing) = inner.projects.values().find(|m| m.path == canonical) {
            return Ok(existing.project_id.clone());
        }

        // Open (or create) the per-project SQLite store.
        let config = StoreConfig {
            sqlite_path: db_path,
            ..Default::default()
        };
        let store = SqliteStore::open(&config)?;

        let canonical_clone = canonical.clone();
        let now = Utc::now();
        let manifest = ProjectManifest {
            project_id: project_id.clone(),
            path: canonical,
            name: String::new(),
            indexed_at: now,
            last_activity: now,
            indexed_file_mtimes: HashMap::new(),
        };

        inner.projects.insert(project_id.clone(), manifest);
        inner.project_stores.insert(project_id.clone(), store.clone());
        let project_registered_cb = inner.on_project_registered.as_ref().map(|_| ());
        drop(inner);

        // Auto-build project knowledge graph on registration
        let (_kg, file_mtimes) = build_project_kg(&canonical_clone);
        {
            let mut inner = self.inner.lock().unwrap();
            if let Some(manifest) = inner.projects.get_mut(&project_id) {
                manifest.indexed_file_mtimes = file_mtimes;
            }
        }

        if project_registered_cb.is_some() {
            let inner = self.inner.lock().unwrap();
            if let Some(ref cb) = inner.on_project_registered {
                cb(&canonical_clone);
            }
        }

        Ok(project_id)
    }

    /// Switch the active project to `project_id`.
    ///
    /// Returns the [`SqliteStore`] for that project so callers can start
    /// reading/writing immediately.
    pub fn switch_project(&self, project_id: &str) -> Result<SqliteStore, MemoryError> {
        let mut inner = self.inner.lock().unwrap();

        let store = inner
            .project_stores
            .get(project_id)
            .cloned()
            .ok_or_else(|| MemoryError::NotFound(format!("project not registered: {project_id}")))?;

        // Update last_activity.
        if let Some(manifest) = inner.projects.get_mut(project_id) {
            manifest.last_activity = Utc::now();
        }

        inner.active_project = Some(project_id.to_string());

        Ok(store)
    }

    /// Return the manifest of the currently active project, if any.
    pub fn current_project(&self) -> Option<ProjectManifest> {
        let inner = self.inner.lock().unwrap();
        inner
            .active_project
            .as_ref()
            .and_then(|id| inner.projects.get(id))
            .cloned()
    }

    /// Return a clone of the global store.
    ///
    /// The global store is always available and never destroyed.
    pub fn global_store(&self) -> SqliteStore {
        self.inner.lock().unwrap().global_store.clone()
    }

    /// Check whether the indexed files for a registered project have changed
    /// since the last KG build.
    ///
    /// Compares the current on-disk modification time of each indexed file
    /// against the timestamp stored in the manifest.  Returns `Ok(true)` if
    /// any file has a different mtime (or was deleted), meaning the KG is
    /// stale and should be rebuilt.
    pub fn is_kg_stale(&self, project_id: &str) -> Result<bool, MemoryError> {
        let inner = self.inner.lock().unwrap();
        let manifest = inner
            .projects
            .get(project_id)
            .ok_or_else(|| MemoryError::NotFound(format!("project not registered: {project_id}")))?;

        for (file_path, stored_mtime) in &manifest.indexed_file_mtimes {
            match std::fs::metadata(file_path) {
                Ok(meta) => match meta.modified() {
                    Ok(modified) => {
                        let current_secs = modified
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        if current_secs != *stored_mtime {
                            return Ok(true);
                        }
                    }
                    Err(_) => return Ok(true),
                },
                Err(_) => return Ok(true),
            }
        }

        Ok(false)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compute a deterministic 16-char hex hash from a canonical path.
///
/// Uses FNV-1a 64-bit (RFC 7353) which is deterministic across runs, unlike
/// [`std::hash::DefaultHasher`] whose random seed changes per-process.
pub(crate) fn hash_path(path: &Path) -> String {
    // FNV-1a 64-bit constants.
    let offset_basis: u64 = 0xcbf29ce484222325;
    let prime: u64 = 0x100000001b3;

    let mut hash: u64 = offset_basis;
    for b in path.to_string_lossy().as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(prime);
    }
    format!("{hash:016x}")
}

// ---------------------------------------------------------------------------
// Project Knowledge Graph Building
// ---------------------------------------------------------------------------

/// Maximum file size to scan (1 MiB). Larger files are skipped.
const MAX_FILE_SIZE: u64 = 1_048_576;

/// Pre-compiled patterns for a language group.
struct LangPatterns {
    extensions: Vec<String>,
    compiled: Vec<(regex::Regex, EntityType)>,
}

// ---------------------------------------------------------------------------
// Unified scan result
// ---------------------------------------------------------------------------

/// Result of a unified single-pass project scan.
///
/// Combines regex-based knowledge-graph entities and tree-sitter-based code
/// symbols in a single struct, produced by one walkdir traversal.
#[derive(Debug, Clone)]
pub struct UnifiedScanResult {
    /// Knowledge graph entities discovered via regex extraction.
    pub kg: KnowledgeGraph,
    /// Code symbols discovered via tree-sitter extraction (5 supported languages).
    pub symbols: Vec<CodeSymbol>,
    /// Edges between code symbols (calls, imports, etc.).
    pub edges: Vec<SymbolEdge>,
    /// Map of indexed file paths to their modification timestamps (Unix seconds).
    pub mtimes: HashMap<String, u64>,
}

/// Scan project files in a **single walkdir pass**, producing both regex-based
/// knowledge-graph entities and tree-sitter-based code symbols/edges.
///
/// When `indexer` is `Some`, supported language files (Rust, Python,
/// TypeScript, Go, Java) are additionally parsed with tree-sitter to extract
/// structured symbols.  When `None`, only regex extraction is performed.
///
/// Returns a [`UnifiedScanResult`] with all collected data.
pub fn unified_scan(
    project_path: &Path,
    mut indexer: Option<&mut CodeIndexer>,
) -> UnifiedScanResult {
    let patterns = get_patterns();
    let mut kg = KnowledgeGraph::new();
    let mut symbols = Vec::new();
    let mut edges = Vec::new();
    let mut file_mtimes: HashMap<String, u64> = HashMap::new();
    let now = Utc::now();

    for entry in walkdir::WalkDir::new(project_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let path = entry.path();

        // Skip oversized files
        if let Ok(meta) = std::fs::metadata(path) {
            if meta.len() > MAX_FILE_SIZE {
                continue;
            }
            // Record mtime for staleness detection
            if let Ok(modified) = meta.modified() {
                let secs = modified
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                file_mtimes.insert(path.to_string_lossy().to_string(), secs);
            }
        }

        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => continue,
        };

        // Skip binary (null byte in first 512 bytes)
        if content.as_bytes().iter().take(512).any(|&b| b == 0) {
            continue;
        }

        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        let source_file = format!("file:{}", path.to_string_lossy());

        // --- Regex extraction (all files) ---
        if let Some(lang) = patterns
            .code_langs
            .iter()
            .find(|l| l.extensions.iter().any(|e| e == &ext))
        {
            process_code(&content, &source_file, &lang.compiled, "code", &now, &mut kg);
        } else {
            match ext.as_str() {
                "md" | "mdx" | "rst" | "adoc" | "txt" | "text" => process_doc(
                    &content,
                    &source_file,
                    patterns.heading_re.as_ref(),
                    patterns.bold_re.as_ref(),
                    patterns.code_fence_re.as_ref(),
                    &patterns.all_code_patterns,
                    &now,
                    &mut kg,
                ),
                "yaml" | "yml" => {
                    process_config(
                        &content,
                        &source_file,
                        patterns.yaml_key_re.as_ref(),
                        "config",
                        &now,
                        &mut kg,
                    );
                }
                "toml" => {
                    process_config(
                        &content,
                        &source_file,
                        patterns.toml_key_re.as_ref(),
                        "config",
                        &now,
                        &mut kg,
                    );
                    // Also extract [section] headers
                    if let Some(re) = patterns.toml_section_re.as_ref() {
                        for cap in re.captures_iter(&content) {
                            if let Some(m) = cap.get(1) {
                                add_entity(
                                    m.as_str(),
                                    EntityType::ConfigKey,
                                    0.7,
                                    &source_file,
                                    "config",
                                    &now,
                                    &mut kg,
                                );
                            }
                        }
                    }
                }
                "json" | "jsonc" | "json5" => {
                    process_config(
                        &content,
                        &source_file,
                        patterns.json_key_re.as_ref(),
                        "config",
                        &now,
                        &mut kg,
                    );
                }
                "html" | "htm" => {
                    process_html(
                        &content,
                        &source_file,
                        patterns.tag_re.as_ref(),
                        patterns.custom_elem_re.as_ref(),
                        patterns.aria_role_re.as_ref(),
                        patterns.semantic_tags,
                        &now,
                        &mut kg,
                    );
                }
                "xml" | "svg" => {
                    process_web(&content, &source_file, patterns.tag_re.as_ref(), &now, &mut kg);
                }
                "css" | "scss" | "less" => {
                    process_css(
                        &content,
                        &source_file,
                        patterns.css_class_re.as_ref(),
                        patterns.css_id_re.as_ref(),
                        patterns.css_keyframes_re.as_ref(),
                        patterns.css_media_re.as_ref(),
                        &now,
                        &mut kg,
                    );
                }
                "vue" => {
                    process_vue(
                        &content,
                        &source_file,
                        path,
                        patterns.vue_script_re.as_ref(),
                        patterns.vue_template_re.as_ref(),
                        patterns.vue_component_name_re.as_ref(),
                        patterns.vue_method_re.as_ref(),
                        patterns.vue_arrow_fn_re.as_ref(),
                        patterns.tag_re.as_ref(),
                        &patterns.all_code_patterns,
                        &now,
                        &mut kg,
                    );
                }
                _ => process_unknown(&content, &source_file, &now, &mut kg),
            }
        }

        // --- Tree-sitter extraction (5 supported langs) ---
        if let Some(ref mut idx) = indexer {
            if crate::code_indexer::IndexLanguage::from_extension(&ext).is_some() {
                if let Ok((file_symbols, file_edges)) = idx.index_content(&content, path) {
                    symbols.extend(file_symbols);
                    edges.extend(file_edges);
                }
            }
        }
    }

    UnifiedScanResult {
        kg,
        symbols,
        edges,
        mtimes: file_mtimes,
    }
}

// ---------------------------------------------------------------------------
// Cached pattern store (OnceLock)
// ---------------------------------------------------------------------------

/// All compiled regex patterns, initialised once and reused across scans.
struct AllPatterns {
    code_langs: Vec<LangPatterns>,
    all_code_patterns: Vec<(regex::Regex, EntityType)>,
    heading_re: Option<regex::Regex>,
    bold_re: Option<regex::Regex>,
    code_fence_re: Option<regex::Regex>,
    yaml_key_re: Option<regex::Regex>,
    toml_key_re: Option<regex::Regex>,
    json_key_re: Option<regex::Regex>,
    toml_section_re: Option<regex::Regex>,
    tag_re: Option<regex::Regex>,
    custom_elem_re: Option<regex::Regex>,
    aria_role_re: Option<regex::Regex>,
    semantic_tags: &'static [&'static str],
    css_class_re: Option<regex::Regex>,
    css_id_re: Option<regex::Regex>,
    css_keyframes_re: Option<regex::Regex>,
    css_media_re: Option<regex::Regex>,
    vue_script_re: Option<regex::Regex>,
    vue_template_re: Option<regex::Regex>,
    vue_component_name_re: Option<regex::Regex>,
    vue_method_re: Option<regex::Regex>,
    vue_arrow_fn_re: Option<regex::Regex>,
}

static PATTERNS: OnceLock<AllPatterns> = OnceLock::new();

/// Return a reference to the statically-cached compiled regex patterns.
fn get_patterns() -> &'static AllPatterns {
    PATTERNS.get_or_init(|| {
        #[allow(clippy::type_complexity)]
        let code_langs_raw: &[(&[&str], &[(&str, EntityType)])] = &[
            // Rust
            (
                &["rs"],
                &[
                    ("(?:pub(?:\\s+async)?\\s+)?fn\\s+(\\w+)\\s*[<(]", EntityType::Tool),
                    ("struct (\\w+)", EntityType::Concept),
                    ("trait (\\w+)", EntityType::Concept),
                    ("impl (\\w+)", EntityType::Concept),
                    ("enum (\\w+)", EntityType::Concept),
                ],
            ),
            // TypeScript / JavaScript
            (
                &["ts", "tsx", "js", "jsx", "mjs", "cjs"],
                &[
                    ("(?:export\\s+)?(?:async\\s+)?function\\s+(\\w+)", EntityType::Tool),
                    ("class (\\w+)", EntityType::Concept),
                    ("interface (\\w+)", EntityType::Concept),
                ],
            ),
            // Python
            (
                &["py"],
                &[
                    ("(?:async\\s+)?def\\s+(\\w+)", EntityType::Tool),
                    ("class (\\w+)", EntityType::Concept),
                ],
            ),
            // Go
            (
                &["go"],
                &[
                    ("func\\s+(?:\\([^)]*\\)\\s*)?(\\w+)\\s*\\(", EntityType::Tool),
                    ("type\\s+(\\w+)\\s+(?:struct|interface)", EntityType::Concept),
                ],
            ),
            // Java
            (
                &["java"],
                &[
                    ("class\\s+(\\w+)", EntityType::Concept),
                    ("interface\\s+(\\w+)", EntityType::Concept),
                    (
                        "(?:public|private|protected|static|\\s)+[\\w<>\\[\\]]+\\s+(\\w+)\\s*\\(",
                        EntityType::Tool,
                    ),
                ],
            ),
            // C / C++
            (
                &["c", "cpp", "cc", "cxx", "h", "hpp", "hxx"],
                &[
                    (
                        "(?:void|int|bool|char|float|double|long|short|size_t|auto)\\s+(\\w+)\\s*\\(",
                        EntityType::Tool,
                    ),
                    ("struct\\s+(\\w+)", EntityType::Concept),
                    ("class\\s+(\\w+)", EntityType::Concept),
                ],
            ),
            // Swift
            (
                &["swift"],
                &[
                    ("func\\s+(\\w+)", EntityType::Tool),
                    ("class\\s+(\\w+)", EntityType::Concept),
                    ("struct\\s+(\\w+)", EntityType::Concept),
                    ("protocol\\s+(\\w+)", EntityType::Concept),
                ],
            ),
            // Kotlin
            (
                &["kt", "kts"],
                &[
                    ("fun\\s+(\\w+)", EntityType::Tool),
                    ("class\\s+(\\w+)", EntityType::Concept),
                    ("interface\\s+(\\w+)", EntityType::Concept),
                ],
            ),
            // Ruby
            (
                &["rb"],
                &[
                    ("def\\s+(\\w+)", EntityType::Tool),
                    ("class\\s+(\\w+)", EntityType::Concept),
                    ("module\\s+(\\w+)", EntityType::Concept),
                ],
            ),
            // Shell
            (
                &["sh", "bash", "zsh"],
                &[
                    ("(?m)^(\\w+)\\s*\\(\\s*\\)", EntityType::Tool),
                    ("function\\s+(\\w+)", EntityType::Tool),
                ],
            ),
            // SQL
            (
                &["sql"],
                &[
                    ("(?i)CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?(\\w+)", EntityType::Concept),
                    ("(?i)CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?(\\w+)", EntityType::Concept),
                ],
            ),
        ];

        let mut code_langs: Vec<LangPatterns> = Vec::with_capacity(code_langs_raw.len());
        for (exts, pats) in code_langs_raw {
            let compiled: Vec<(regex::Regex, EntityType)> = pats
                .iter()
                .filter_map(|(pat, etype)| regex::Regex::new(pat).ok().map(|re| (re, *etype)))
                .collect();
            if !compiled.is_empty() {
                code_langs.push(LangPatterns {
                    extensions: exts.iter().map(|s| (*s).to_string()).collect(),
                    compiled,
                });
            }
        }

        let all_code_patterns: Vec<(regex::Regex, EntityType)> = code_langs
            .iter()
            .flat_map(|l| l.compiled.clone())
            .collect();

        AllPatterns {
            code_langs,
            all_code_patterns,
            heading_re: regex::Regex::new(r"(?m)^#{1,6}\s+(.+)").ok(),
            bold_re: regex::Regex::new(r"\*\*(.+?)\*\*").ok(),
            code_fence_re: regex::Regex::new(r"(?s)```(\w*)\s*\n(.*?)```").ok(),
            yaml_key_re: regex::Regex::new(r"(?m)^([\w][\w._-]*)\s*:").ok(),
            toml_key_re: regex::Regex::new(r"(?m)^([\w][\w._-]*)\s*=").ok(),
            json_key_re: regex::Regex::new(r#""(\w[\w_-]*)"\s*:"#).ok(),
            toml_section_re: regex::Regex::new(r"(?m)^\[(\w[\w_.-]*)\]").ok(),
            tag_re: regex::Regex::new(r"<([\w-]+)").ok(),
            custom_elem_re: regex::Regex::new(r"<([a-z]+-[a-z][\w-]*)").ok(),
            aria_role_re: regex::Regex::new(r#"role="([^"]+)""#).ok(),
            semantic_tags: &["main", "nav", "article", "section", "header", "footer"],
            css_class_re: regex::Regex::new(r"\.([a-zA-Z][\w-]+)").ok(),
            css_id_re: regex::Regex::new(r"#([a-zA-Z][\w-]+)").ok(),
            css_keyframes_re: regex::Regex::new(r"@keyframes\s+([\w-]+)").ok(),
            css_media_re: regex::Regex::new(r"@media").ok(),
            vue_script_re: regex::Regex::new(r"(?s)<script[^>]*>(.*?)</script>").ok(),
            vue_template_re: regex::Regex::new(r"(?s)<template[^>]*>(.*?)</template>").ok(),
            vue_component_name_re: regex::Regex::new(
                r#"(?s)export\s+default\s*\{[^}]*?name\s*:\s*['\"]([^'\"]+)['\"]"#,
            )
            .ok(),
            vue_method_re: regex::Regex::new(r"(?m)^\s*(?:async\s+)?(\w+)\s*\([^)]*\)\s*\{").ok(),
            vue_arrow_fn_re: regex::Regex::new(r"(?m)(?:const|let|var)\s+(\w+)\s*=\s*(?:async\s*)?\(")
                .ok(),
        }
    })
}

/// Scan project files and build a [`KnowledgeGraph`] of all discoverable symbols.
///
/// Convenience wrapper around [`unified_scan`] that only returns the knowledge
/// graph and file mtimes (no tree-sitter extraction).
///
/// Walks `project_path` **once** and dispatches to specialised extractors by
/// extension:
///
/// | Extension(s)           | Extractor | Entity types                |
/// |------------------------|-----------|-----------------------------|
/// | .rs .ts .py .go .java …| code      | Tool / Concept              |
/// | .md .rst .txt .adoc     | doc       | DocHeading / DocTerm        |
/// | .yaml .toml .json       | config    | ConfigKey                   |
/// | .html .xml .svg         | web       | DataField                   |
/// | * (any other text)      | unknown   | Concept (first line)        |
///
/// Binary files (null byte in first 512 bytes) and files > 1 MiB are skipped.
///
/// Returns the knowledge graph and a map of indexed file paths to their
/// modification timestamps (Unix seconds), used for staleness detection.
pub fn build_project_kg(project_path: &Path) -> (KnowledgeGraph, HashMap<String, u64>) {
    let result = unified_scan(project_path, None);
    (result.kg, result.mtimes)
}

// ---------------------------------------------------------------------------
// Extractor helpers (file-private)
// ---------------------------------------------------------------------------

/// Run compiled code patterns against `content` and insert matches as entities.
fn process_code(
    content: &str,
    source_file: &str,
    patterns: &[(regex::Regex, EntityType)],
    source_type: &str,
    now: &DateTime<Utc>,
    kg: &mut KnowledgeGraph,
) {
    for (re, etype) in patterns {
        for cap in re.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                let name = m.as_str().to_string();
                if !name.is_empty() {
                    add_entity(&name, *etype, 0.8, source_file, source_type, now, kg);
                }
            }
        }
    }
}

/// Extract headings, bold terms, and code-block symbols from doc-like files.
fn process_doc(
    content: &str,
    source_file: &str,
    heading_re: Option<&regex::Regex>,
    bold_re: Option<&regex::Regex>,
    code_fence_re: Option<&regex::Regex>,
    all_code_patterns: &[(regex::Regex, EntityType)],
    now: &DateTime<Utc>,
    kg: &mut KnowledgeGraph,
) {
    // Headings
    if let Some(re) = heading_re {
        for cap in re.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                let heading = m.as_str().trim();
                if !heading.is_empty() && heading.len() <= 120 {
                    add_entity(heading, EntityType::DocHeading, 0.7, source_file, "doc", now, kg);
                }
            }
        }
    }

    // Bold / strong terms
    if let Some(re) = bold_re {
        for cap in re.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                let term = m.as_str().trim();
                if !term.is_empty() && term.len() <= 80 && !term.contains('\n') {
                    add_entity(term, EntityType::DocTerm, 0.5, source_file, "doc", now, kg);
                }
            }
        }
    }

    // Fenced code blocks → run all code patterns
    if let Some(re) = code_fence_re {
        for cap in re.captures_iter(content) {
            if let Some(block) = cap.get(2) {
                let code = block.as_str();
                if code.len() > 100_000 {
                    continue; // skip giant blocks
                }
                for (pattern_re, etype) in all_code_patterns {
                    for m in pattern_re.captures_iter(code) {
                        if let Some(n) = m.get(1) {
                            let name = n.as_str().to_string();
                            if !name.is_empty() {
                                add_entity(&name, *etype, 0.6, source_file, "doc", now, kg);
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Shared config-key extractor (YAML / TOML / JSON).
fn process_config(
    content: &str,
    source_file: &str,
    key_re: Option<&regex::Regex>,
    source_type: &str,
    now: &DateTime<Utc>,
    kg: &mut KnowledgeGraph,
) {
    if let Some(re) = key_re {
        for cap in re.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                let key = m.as_str();
                if !key.is_empty() && key.len() <= 80 {
                    add_entity(key, EntityType::ConfigKey, 0.7, source_file, source_type, now, kg);
                }
            }
        }
    }
}

/// Extract tag / element names from HTML / XML / SVG.
fn process_web(
    content: &str,
    source_file: &str,
    tag_re: Option<&regex::Regex>,
    now: &DateTime<Utc>,
    kg: &mut KnowledgeGraph,
) {
    if let Some(re) = tag_re {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for cap in re.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                let tag = m.as_str().to_lowercase();
                // Skip structural boilerplate
                if matches!(
                    tag.as_str(),
                    "html" | "head" | "body" | "meta" | "link" | "script" | "style"
                        | "br" | "hr" | "!doctype" | "!DOCTYPE"
                ) {
                    continue;
                }
                if tag.len() <= 60 && seen.insert(tag.clone()) {
                    add_entity(&tag, EntityType::DataField, 0.6, source_file, "data", now, kg);
                }
            }
        }
    }
}

/// Extract meaningful entities from HTML: custom elements, semantic tags,
/// aria-roles, and remaining structural tag names.
fn process_html(
    content: &str,
    source_file: &str,
    tag_re: Option<&regex::Regex>,
    custom_elem_re: Option<&regex::Regex>,
    aria_role_re: Option<&regex::Regex>,
    semantic_tags: &[&str],
    now: &DateTime<Utc>,
    kg: &mut KnowledgeGraph,
) {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Extract custom elements (dash-case tags) → Concept
    if let Some(re) = custom_elem_re {
        for cap in re.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                let name = m.as_str().to_lowercase();
                if name.len() <= 60 && seen.insert(name.clone()) {
                    add_entity(&name, EntityType::Concept, 0.8, source_file, "frontend", now, kg);
                }
            }
        }
    }

    // Extract aria-role values → ConfigKey
    if let Some(re) = aria_role_re {
        for cap in re.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                let role = m.as_str().trim().to_lowercase();
                if !role.is_empty() && role.len() <= 60 && seen.insert(role.clone()) {
                    add_entity(&role, EntityType::ConfigKey, 0.7, source_file, "frontend", now, kg);
                }
            }
        }
    }

    // Extract all tags, classify by type
    if let Some(re) = tag_re {
        for cap in re.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                let tag = m.as_str().to_lowercase();
                // Skip structural boilerplate
                if matches!(
                    tag.as_str(),
                    "html" | "head" | "body" | "meta" | "link" | "script" | "style"
                        | "br" | "hr" | "!doctype"
                ) {
                    continue;
                }
                if tag.len() > 60 {
                    continue;
                }
                if !seen.insert(tag.clone()) {
                    continue;
                }

                if semantic_tags.contains(&tag.as_str()) {
                    add_entity(&tag, EntityType::DocHeading, 0.7, source_file, "frontend", now, kg);
                } else if tag.contains('-') {
                    // Already handled by custom_elem_re above; skip duplicates
                } else {
                    add_entity(&tag, EntityType::DataField, 0.6, source_file, "frontend", now, kg);
                }
            }
        }
    }
}

/// Extract selectors, keyframes, and media queries from CSS / SCSS / LESS.
fn process_css(
    content: &str,
    source_file: &str,
    class_re: Option<&regex::Regex>,
    id_re: Option<&regex::Regex>,
    keyframes_re: Option<&regex::Regex>,
    media_re: Option<&regex::Regex>,
    now: &DateTime<Utc>,
    kg: &mut KnowledgeGraph,
) {
    // Class selectors → DataField
    if let Some(re) = class_re {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for cap in re.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                let name = m.as_str();
                if !name.is_empty() && name.len() <= 60 && seen.insert(name.to_string()) {
                    add_entity(name, EntityType::DataField, 0.7, source_file, "style", now, kg);
                }
            }
        }
    }

    // ID selectors → DataField
    if let Some(re) = id_re {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for cap in re.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                let name = m.as_str();
                if !name.is_empty() && name.len() <= 60 && seen.insert(name.to_string()) {
                    add_entity(name, EntityType::DataField, 0.7, source_file, "style", now, kg);
                }
            }
        }
    }

    // @keyframes names → Concept
    if let Some(re) = keyframes_re {
        for cap in re.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                let name = m.as_str();
                if !name.is_empty() && name.len() <= 60 {
                    add_entity(name, EntityType::Concept, 0.8, source_file, "style", now, kg);
                }
            }
        }
    }

    // @media queries: count them
    if let Some(re) = media_re {
        let count = re.find_iter(content).count();
        if count > 0 {
            add_entity(
                &format!("{}-media-queries", count),
                EntityType::DataField,
                0.4,
                source_file,
                "style",
                now,
                kg,
            );
        }
    }
}

/// Parse Vue single-file components: extract component name, scan `<script>`
/// for JS/TS patterns, and scan `<template>` for HTML elements.
fn process_vue(
    content: &str,
    source_file: &str,
    path: &Path,
    script_re: Option<&regex::Regex>,
    template_re: Option<&regex::Regex>,
    component_name_re: Option<&regex::Regex>,
    method_re: Option<&regex::Regex>,
    arrow_fn_re: Option<&regex::Regex>,
    tag_re: Option<&regex::Regex>,
    all_code_patterns: &[(regex::Regex, EntityType)],
    now: &DateTime<Utc>,
    kg: &mut KnowledgeGraph,
) {
    // Derive component name from filename (fallback)
    let filename_component = path
        .file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
        .unwrap_or_default();

    // Scan <script> section
    let mut found_component_name = false;
    if let Some(re) = script_re {
        for cap in re.captures_iter(content) {
            if let Some(block) = cap.get(1) {
                let script = block.as_str();
                if script.len() > 200_000 {
                    continue;
                }

                // Extract component name from `export default { name: 'X' }`
                if let Some(name_re) = component_name_re {
                    if let Some(nc) = name_re.captures(script) {
                        if let Some(m) = nc.get(1) {
                            let name = m.as_str();
                            if !name.is_empty() && name.len() <= 80 {
                                add_entity(
                                    name,
                                    EntityType::Concept,
                                    0.9,
                                    source_file,
                                    "frontend",
                                    now,
                                    kg,
                                );
                                found_component_name = true;
                            }
                        }
                    }
                }

                // Detect method shorthand: `methodName() {` or `async methodName() {`
                if let Some(re) = method_re {
                    for m in re.captures_iter(script) {
                        if let Some(n) = m.get(1) {
                            let name = n.as_str().to_string();
                            if !name.is_empty()
                                && name.len() <= 80
                                && !name.starts_with("__")
                                && !is_js_keyword(&name)
                            {
                                add_entity(
                                    &name,
                                    EntityType::Tool,
                                    0.7,
                                    source_file,
                                    "frontend",
                                    now,
                                    kg,
                                );
                            }
                        }
                    }
                }

                // Detect arrow function consts: `const fnName = (...) =>`
                if let Some(re) = arrow_fn_re {
                    for m in re.captures_iter(script) {
                        if let Some(n) = m.get(1) {
                            let name = n.as_str().to_string();
                            if !name.is_empty()
                                && name.len() <= 80
                                && !name.starts_with("__")
                                && !is_js_keyword(&name)
                            {
                                add_entity(
                                    &name,
                                    EntityType::Tool,
                                    0.7,
                                    source_file,
                                    "frontend",
                                    now,
                                    kg,
                                );
                            }
                        }
                    }
                }

                // Run JS/TS code patterns on the script content
                for (pattern_re, etype) in all_code_patterns {
                    for m in pattern_re.captures_iter(script) {
                        if let Some(n) = m.get(1) {
                            let name = n.as_str().to_string();
                            if !name.is_empty()
                                && name.len() <= 80
                                && !name.starts_with("__")
                            {
                                add_entity(
                                    &name,
                                    *etype,
                                    0.7,
                                    source_file,
                                    "frontend",
                                    now,
                                    kg,
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    // Fallback: use filename as component name if not found in <script>
    if !found_component_name && !filename_component.is_empty() {
        add_entity(
            &filename_component,
            EntityType::Concept,
            0.7,
            source_file,
            "frontend",
            now,
            kg,
        );
    }

    // Scan <template> section for HTML elements
    if let Some(re) = template_re {
        if let Some(tag_re) = tag_re {
            for cap in re.captures_iter(content) {
                if let Some(block) = cap.get(1) {
                    let template = block.as_str();
                    let mut seen: std::collections::HashSet<String> =
                        std::collections::HashSet::new();
                    for tc in tag_re.captures_iter(template) {
                        if let Some(m) = tc.get(1) {
                            let tag = m.as_str().to_lowercase();
                            if matches!(
                                tag.as_str(),
                                "html" | "head" | "body" | "meta" | "link"
                                    | "script" | "style" | "br" | "hr"
                                    | "!doctype"
                            ) {
                                continue;
                            }
                            if tag.len() <= 60 && seen.insert(tag.clone()) {
                                add_entity(
                                    &tag,
                                    EntityType::DataField,
                                    0.6,
                                    source_file,
                                    "frontend",
                                    now,
                                    kg,
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Filter out JavaScript keywords that would be false positives in method detection.
fn is_js_keyword(name: &str) -> bool {
    matches!(
        name,
        "if" | "else"
            | "for"
            | "while"
            | "do"
            | "switch"
            | "case"
            | "try"
            | "catch"
            | "finally"
            | "throw"
            | "return"
            | "break"
            | "continue"
            | "typeof"
            | "instanceof"
            | "new"
            | "delete"
            | "void"
            | "in"
            | "of"
            | "default"
            | "export"
            | "import"
            | "from"
            | "as"
            | "class"
            | "extends"
            | "super"
            | "this"
            | "true"
            | "false"
            | "null"
            | "undefined"
            | "let"
            | "var"
            | "const"
            | "async"
            | "await"
    )
}

/// Fallback for unrecognised text: use the first non-empty line as a Concept.
fn process_unknown(
    content: &str,
    source_file: &str,
    now: &DateTime<Utc>,
    kg: &mut KnowledgeGraph,
) {
    for line in content.lines() {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            let name = &trimmed[..trimmed.len().min(100)];
            add_entity(name, EntityType::Concept, 0.3, source_file, "unknown", now, kg);
            break;
        }
    }
}

/// Convenience: build and insert an [`Entity`].
fn add_entity(
    name: &str,
    entity_type: EntityType,
    confidence: f64,
    source_file: &str,
    source_type: &str,
    now: &DateTime<Utc>,
    kg: &mut KnowledgeGraph,
) {
    let entity = Entity {
        id: format!(
            "project-kg-{}-{}",
            entity_type,
            uuid::Uuid::new_v4().as_simple()
        ),
        name: name.to_string(),
        entity_type,
        confidence,
        frequency: 1,
        first_seen: *now,
        last_seen: *now,
        source_ids: vec![source_file.to_string()],
        source_type: source_type.to_string(),
    };
    kg.add_entity(entity);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_scope_keys() {
        assert_eq!(MemoryScope::Global.scope_key(), "global");
        assert_eq!(
            MemoryScope::Project("abc123".into()).scope_key(),
            "project_abc123"
        );
        assert_eq!(
            MemoryScope::Session("sess_1".into()).scope_key(),
            "session_sess_1"
        );
        assert_eq!(
            MemoryScope::Agent("agent_x".into()).scope_key(),
            "agent_agent_x"
        );
    }

    #[test]
    fn hash_path_deterministic() {
        let a = hash_path(Path::new("/home/user/projects/foo"));
        let b = hash_path(Path::new("/home/user/projects/foo"));
        assert_eq!(a, b, "hash must be deterministic");
        assert_eq!(a.len(), 16, "hex string of u64");
    }

    #[test]
    fn hash_path_different() {
        let a = hash_path(Path::new("/a"));
        let b = hash_path(Path::new("/b"));
        assert_ne!(a, b);
    }
}
