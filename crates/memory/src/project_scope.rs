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
            }),
        })
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
        drop(inner);

        // Auto-build project knowledge graph on registration
        let _kg = build_project_kg(&canonical_clone);
        
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

/// Regex patterns per language for extracting code symbols.
struct CodePatterns {
    extensions: &'static [&'static str],
    patterns: &'static [(&'static str, EntityType)],
}

/// Scan project files and build a [`KnowledgeGraph`] of code symbols.
///
/// Walks `project_path` for source files (`.rs`, `.ts`, `.py`) and uses
/// regex-based extraction to register functions, structs, traits, classes,
/// and interfaces as KG entities.
///
/// # Returns
///
/// A freshly built [`KnowledgeGraph`] containing all extracted symbols.
pub fn build_project_kg(project_path: &Path) -> KnowledgeGraph {
    let lang_patterns: &[CodePatterns] = &[
        CodePatterns {
            extensions: &["rs"],
            patterns: &[
                ("(?:pub(?:\\s+async)?\\s+)?fn\\s+(\\w+)\\s*[<(]", EntityType::Tool),
                ("struct (\\w+)", EntityType::Concept),
                ("trait (\\w+)", EntityType::Concept),
                ("impl (\\w+)", EntityType::Concept),
                ("enum (\\w+)", EntityType::Concept),
            ],
        },
        CodePatterns {
            extensions: &["ts", "tsx"],
            patterns: &[
                ("(?:export\\s+)?(?:async\\s+)?function\\s+(\\w+)", EntityType::Tool),
                ("class (\\w+)", EntityType::Concept),
                ("interface (\\w+)", EntityType::Concept),
            ],
        },
        CodePatterns {
            extensions: &["py"],
            patterns: &[
                ("(?:async\\s+)?def\\s+(\\w+)", EntityType::Tool),
                ("class (\\w+)", EntityType::Concept),
            ],
        },
    ];

    let mut kg = KnowledgeGraph::new();
    let now = Utc::now();

    for lang in lang_patterns {
        // Compile regexes once per language group.
        let compiled: Vec<(regex::Regex, EntityType)> = lang
            .patterns
            .iter()
            .filter_map(|(pat, etype)| regex::Regex::new(pat).ok().map(|re| (re, *etype)))
            .collect();

        if compiled.is_empty() {
            continue;
        }

        // Walk the project directory, filtering by extension.
        for ext in lang.extensions {
            for entry in walkdir::WalkDir::new(project_path)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_type().is_file()
                        && e.path()
                            .extension()
                            .map(|os| os == *ext)
                            .unwrap_or(false)
                })
            {
                let content = match std::fs::read_to_string(entry.path()) {
                    Ok(c) => c,
                    Err(_) => continue,
                };

                for (re, etype) in &compiled {
                    for cap in re.captures_iter(&content) {
                        if let Some(m) = cap.get(1) {
                            let name = m.as_str().to_string();
                            if name.is_empty() {
                                continue;
                            }
                            let entity = Entity {
                                id: format!(
                                    "project-kg-{}-{}",
                                    etype,
                                    uuid::Uuid::new_v4().as_simple()
                                ),
                                name,
                                entity_type: *etype,
                                confidence: 0.8,
                                frequency: 1,
                                first_seen: now,
                                last_seen: now,
                                source_ids: vec![entry.path().to_string_lossy().to_string()],
                            };
                            kg.add_entity(entity);
                        }
                    }
                }
            }
        }
    }

    kg
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
