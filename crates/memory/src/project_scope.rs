//! Project-scoped memory management.
//!
//! Each registered project gets its own SQLite database (`memory_<12-char-hash>.db`)
//! stored alongside the global `memory.db`.  The [`ProjectScopeManager`] tracks
//! registered projects, provides per-project stores, and allows switching the
//! active project at runtime.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
        };

        inner.projects.insert(project_id.clone(), manifest);
        inner.project_stores.insert(project_id.clone(), store.clone());
        let project_registered_cb = inner.on_project_registered.as_ref().map(|_| ());
        drop(inner);

        // Auto-build project knowledge graph on registration
        let _kg = build_project_kg(&canonical_clone);

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
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compute a deterministic 16-char hex hash from a canonical path.
///
/// Uses FNV-1a 64-bit (RFC 7353) which is deterministic across runs, unlike
/// [`std::hash::DefaultHasher`] whose random seed changes per-process.
fn hash_path(path: &Path) -> String {
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

/// Scan project files and build a [`KnowledgeGraph`] of all discoverable symbols.
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
pub fn build_project_kg(project_path: &Path) -> KnowledgeGraph {
    // -- language definitions -------------------------------------------------
    #[allow(clippy::type_complexity)]
    let code_langs: &[(&[&str], &[(&str, EntityType)])] = &[
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

    // Pre-compile code regexes per language group ----------------------------
    let mut compiled_langs: Vec<LangPatterns> = Vec::with_capacity(code_langs.len());
    for (exts, patterns) in code_langs {
        let compiled: Vec<(regex::Regex, EntityType)> = patterns
            .iter()
            .filter_map(|(pat, etype)| regex::Regex::new(pat).ok().map(|re| (re, *etype)))
            .collect();
        if !compiled.is_empty() {
            compiled_langs.push(LangPatterns {
                extensions: exts.iter().map(|s| (*s).to_string()).collect(),
                compiled,
            });
        }
    }

    // Collect all code patterns for fenced-code-block scanning inside docs
    let all_code_patterns: Vec<(regex::Regex, EntityType)> = compiled_langs
        .iter()
        .flat_map(|l| l.compiled.clone())
        .collect();

    // Doc patterns -----------------------------------------------------------
    let heading_re = regex::Regex::new(r"(?m)^#{1,6}\s+(.+)").ok();
    let bold_re = regex::Regex::new(r"\*\*(.+?)\*\*").ok();
    let code_fence_re = regex::Regex::new(r"(?s)```(\w*)\s*\n(.*?)```").ok();

    // Config patterns --------------------------------------------------------
    let yaml_key_re = regex::Regex::new(r"(?m)^([\w][\w._-]*)\s*:").ok();
    let toml_key_re = regex::Regex::new(r"(?m)^([\w][\w._-]*)\s*=").ok();
    let json_key_re = regex::Regex::new(r#""(\w[\w_-]*)"\s*:"#).ok();
    let toml_section_re = regex::Regex::new(r"(?m)^\[(\w[\w_.-]*)\]").ok();

    // Web patterns -----------------------------------------------------------
    let tag_re = regex::Regex::new(r"<([\w-]+)").ok();

    // HTML-specific patterns -------------------------------------------------
    let custom_elem_re = regex::Regex::new(r"<([a-z]+-[a-z][\w-]*)").ok();
    let aria_role_re = regex::Regex::new(r#"role="([^"]+)""#).ok();
    let semantic_tags: &[&str] = &["main", "nav", "article", "section", "header", "footer"];

    // CSS patterns -----------------------------------------------------------
    let css_class_re = regex::Regex::new(r"\.([a-zA-Z][\w-]+)").ok();
    let css_id_re = regex::Regex::new(r"#([a-zA-Z][\w-]+)").ok();
    let css_keyframes_re = regex::Regex::new(r"@keyframes\s+([\w-]+)").ok();
    let css_media_re = regex::Regex::new(r"@media").ok();

    // Vue patterns -----------------------------------------------------------
    let vue_script_re = regex::Regex::new(r"(?s)<script[^>]*>(.*?)</script>").ok();
    let vue_template_re = regex::Regex::new(r"(?s)<template[^>]*>(.*?)</template>").ok();
    let vue_component_name_re = regex::Regex::new(r#"(?s)export\s+default\s*\{[^}]*?name\s*:\s*['\"]([^'\"]+)['\"]"#).ok();
    let vue_method_re = regex::Regex::new(r"(?m)^\s*(?:async\s+)?(\w+)\s*\([^)]*\)\s*\{").ok();
    let vue_arrow_fn_re = regex::Regex::new(r"(?m)(?:const|let|var)\s+(\w+)\s*=\s*(?:async\s*)?\(").ok();

    // -- single walk ---------------------------------------------------------
    let mut kg = KnowledgeGraph::new();
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

        // Dispatch
        if let Some(lang) = compiled_langs.iter().find(|l| l.extensions.iter().any(|e| e == &ext))
        {
            process_code(&content, &source_file, &lang.compiled, "code", &now, &mut kg);
        } else {
            match ext.as_str() {
                "md" | "mdx" | "rst" | "adoc" | "txt" | "text" => process_doc(
                    &content,
                    &source_file,
                    heading_re.as_ref(),
                    bold_re.as_ref(),
                    code_fence_re.as_ref(),
                    &all_code_patterns,
                    &now,
                    &mut kg,
                ),
                "yaml" | "yml" => {
                    process_config(&content, &source_file, yaml_key_re.as_ref(), "config", &now, &mut kg);
                }
                "toml" => {
                    process_config(&content, &source_file, toml_key_re.as_ref(), "config", &now, &mut kg);
                    // Also extract [section] headers
                    if let Some(re) = toml_section_re.as_ref() {
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
                    process_config(&content, &source_file, json_key_re.as_ref(), "config", &now, &mut kg);
                }
                "html" | "htm" => {
                    process_html(
                        &content,
                        &source_file,
                        tag_re.as_ref(),
                        custom_elem_re.as_ref(),
                        aria_role_re.as_ref(),
                        semantic_tags,
                        &now,
                        &mut kg,
                    );
                }
                "xml" | "svg" => {
                    process_web(&content, &source_file, tag_re.as_ref(), &now, &mut kg);
                }
                "css" | "scss" | "less" => {
                    process_css(
                        &content,
                        &source_file,
                        css_class_re.as_ref(),
                        css_id_re.as_ref(),
                        css_keyframes_re.as_ref(),
                        css_media_re.as_ref(),
                        &now,
                        &mut kg,
                    );
                }
                "vue" => {
                    process_vue(
                        &content,
                        &source_file,
                        path,
                        vue_script_re.as_ref(),
                        vue_template_re.as_ref(),
                        vue_component_name_re.as_ref(),
                        vue_method_re.as_ref(),
                        vue_arrow_fn_re.as_ref(),
                        tag_re.as_ref(),
                        &all_code_patterns,
                        &now,
                        &mut kg,
                    );
                }
                _ => process_unknown(&content, &source_file, &now, &mut kg),
            }
        }
    }

    kg
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
