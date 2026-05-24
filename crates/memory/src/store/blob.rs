//! File-system blob storage for memory payloads.
//!
//! Each memory entry is stored as a Markdown file with YAML frontmatter under a
//! structured directory tree:
//!
//! ```text
//! {root}/{layer}/{category}/{id}.md
//! ```
//!
//! Writes are atomic: content is first written to a `.tmp` file in the same
//! directory, then renamed into place to prevent corruption on interrupted writes.

use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use chrono::Utc;

use crate::{
    config::StoreConfig,
    error::MemoryError,
    types::{MemoryCategory, MemoryId, MemoryLayer},
};

// ─── Storage statistics ──────────────────────────────────────────────────────

/// Aggregate usage statistics for the blob store.
#[derive(Debug, Default)]
pub struct StoreStats {
    /// Total number of `.md` files stored.
    pub file_count: u64,
    /// Total bytes consumed by all files.
    pub total_bytes: u64,
    /// Per-layer breakdown `(count, bytes)`.
    pub by_layer: HashMap<MemoryLayer, (u64, u64)>,
}

// ─── BlobStore ────────────────────────────────────────────────────────────────

/// File-system blob store that persists memory content as Markdown files.
pub struct BlobStore {
    root: PathBuf,
}

impl BlobStore {
    /// Initialise the blob store, creating `root` if it does not exist.
    pub fn new(config: &StoreConfig) -> Result<Self, MemoryError> {
        fs::create_dir_all(&config.blob_dir)
            .map_err(|e| MemoryError::Store(format!("create blob dir: {e}")))?;
        Ok(Self {
            root: config.blob_dir.clone(),
        })
    }

    /// Build the canonical path for a memory file.
    ///
    /// Format: `{root}/{layer}/{category}/{id}.md`
    fn path_for(&self, id: &MemoryId, layer: MemoryLayer, category: MemoryCategory) -> PathBuf {
        self.root
            .join(layer_str(layer))
            .join(category_str(category))
            .join(format!("{id}.md"))
    }

    /// Atomically write `content` to `path`.
    ///
    /// The file is first written to `{path}.tmp` and then renamed, ensuring the
    /// destination is either the old content or the new content — never a partial
    /// write.
    fn atomic_write(&self, path: &Path, content: &str) -> Result<(), MemoryError> {
        // Ensure parent directory exists.
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| MemoryError::Store(format!("create dir {}: {e}", parent.display())))?;
        }

        let tmp_path = path.with_extension("md.tmp");
        {
            let mut tmp = fs::File::create(&tmp_path)
                .map_err(|e| MemoryError::Store(format!("create tmp file: {e}")))?;
            tmp.write_all(content.as_bytes())
                .map_err(|e| MemoryError::Store(format!("write tmp file: {e}")))?;
            tmp.flush()
                .map_err(|e| MemoryError::Store(format!("flush tmp file: {e}")))?;
        }
        fs::rename(&tmp_path, path)
            .map_err(|e| MemoryError::Store(format!("rename tmp -> final: {e}")))?;
        Ok(())
    }

    // ─── Public API ───────────────────────────────────────────────────────────

    /// Write `content` for the given memory, embedding YAML frontmatter.
    ///
    /// The frontmatter records `id`, `layer`, `category`, and `created_at` so
    /// that the file is self-describing and can be recovered without the
    /// database.
    ///
    /// # Errors
    /// Returns [`MemoryError::InvalidArgument`] if `id` contains a path
    /// separator character.
    pub fn write(
        &self,
        id: &MemoryId,
        layer: MemoryLayer,
        category: MemoryCategory,
        content: &str,
    ) -> Result<(), MemoryError> {
        let id_str = id.to_string();
        validate_id(&id_str)?;

        let path = self.path_for(id, layer, category);
        let now = Utc::now().to_rfc3339();
        let frontmatter = format!(
            "---\nid: {id_str}\nlayer: {layer}\ncategory: {category}\ncreated_at: {now}\n---\n\n",
            layer = layer_str(layer),
            category = category_str(category),
        );
        let full = format!("{frontmatter}{content}");
        self.atomic_write(&path, &full)
    }

    /// Read the *body* content (everything after the frontmatter) for a memory.
    ///
    /// Returns `Ok(None)` when the file does not exist.
    pub fn read(
        &self,
        id: &MemoryId,
        layer: MemoryLayer,
        category: MemoryCategory,
    ) -> Result<Option<String>, MemoryError> {
        let path = self.path_for(id, layer, category);
        match fs::read_to_string(&path) {
            Ok(raw) => Ok(Some(strip_frontmatter(&raw))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(MemoryError::Store(format!("read {}: {e}", path.display()))),
        }
    }

    /// Delete the file for a memory.
    ///
    /// Returns `Ok(())` if the file did not exist (idempotent).
    pub fn delete(
        &self,
        id: &MemoryId,
        layer: MemoryLayer,
        category: MemoryCategory,
    ) -> Result<(), MemoryError> {
        let path = self.path_for(id, layer, category);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(MemoryError::Store(format!("delete {}: {e}", path.display()))),
        }
    }

    /// List all memory IDs stored under `layer` (across all categories).
    pub fn list(&self, layer: MemoryLayer) -> Result<Vec<MemoryId>, MemoryError> {
        let layer_dir = self.root.join(layer_str(layer));
        if !layer_dir.exists() {
            return Ok(Vec::new());
        }

        let mut ids = Vec::new();
        for cat_entry in read_dir_entries(&layer_dir)? {
            if !cat_entry.is_dir() {
                continue;
            }
            for file_entry in read_dir_entries(&cat_entry)? {
                if file_entry.extension().and_then(|e| e.to_str()) == Some("md") {
                    if let Some(stem) = file_entry.file_stem().and_then(|s| s.to_str()) {
                        if let Ok(id) = stem.parse::<MemoryId>() {
                            ids.push(id);
                        }
                    }
                }
            }
        }
        Ok(ids)
    }

    /// Compute aggregate storage statistics.
    pub fn stats(&self) -> Result<StoreStats, MemoryError> {
        let mut stats = StoreStats::default();

        for layer in ALL_LAYERS {
            let layer_dir = self.root.join(layer_str(layer));
            if !layer_dir.exists() {
                continue;
            }
            let mut layer_count: u64 = 0;
            let mut layer_bytes: u64 = 0;

            for cat_entry in read_dir_entries(&layer_dir)? {
                if !cat_entry.is_dir() {
                    continue;
                }
                for file_entry in read_dir_entries(&cat_entry)? {
                    if file_entry.extension().and_then(|e| e.to_str()) == Some("md") {
                        let file_size = fs::metadata(&file_entry)
                            .map(|m| m.len())
                            .unwrap_or(0);
                        layer_count += 1;
                        layer_bytes += file_size;
                    }
                }
            }

            if layer_count > 0 {
                stats.file_count += layer_count;
                stats.total_bytes += layer_bytes;
                stats.by_layer.insert(layer, (layer_count, layer_bytes));
            }
        }

        Ok(stats)
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

const ALL_LAYERS: [MemoryLayer; 5] = [
    MemoryLayer::L0,
    MemoryLayer::L1,
    MemoryLayer::L2,
    MemoryLayer::L3,
    MemoryLayer::L4,
];

fn layer_str(layer: MemoryLayer) -> &'static str {
    match layer {
        MemoryLayer::L0 => "l0",
        MemoryLayer::L1 => "l1",
        MemoryLayer::L2 => "l2",
        MemoryLayer::L3 => "l3",
        MemoryLayer::L4 => "l4",
    }
}

fn category_str(cat: MemoryCategory) -> &'static str {
    match cat {
        MemoryCategory::UserPreference => "user_preference",
        MemoryCategory::ProjectConvention => "project_convention",
        MemoryCategory::Decision => "decision",
        MemoryCategory::Reference => "reference",
        MemoryCategory::Shared => "shared",
        MemoryCategory::CompressedSummary => "compressed_summary",
        MemoryCategory::ProjectKnowledge => "project_knowledge",
    }
}

/// Reject IDs that contain filesystem path separators.
fn validate_id(id: &str) -> Result<(), MemoryError> {
    if id.contains('/') || id.contains('\\') || id.contains("..") {
        return Err(MemoryError::InvalidArgument(format!(
            "memory id contains unsafe characters: {id}"
        )));
    }
    Ok(())
}

/// Strip the YAML frontmatter block (`---\n...\n---\n`) from a Markdown string
/// and return the body.
fn strip_frontmatter(raw: &str) -> String {
    if !raw.starts_with("---\n") {
        return raw.to_owned();
    }
    // Find the closing `---` delimiter.
    if let Some(end) = raw[4..].find("\n---\n") {
        // Skip past the closing delimiter and the following blank line if present.
        let body_start = 4 + end + 5; // 4 (opening) + offset + len("\n---\n")
        raw[body_start..].trim_start_matches('\n').to_owned()
    } else {
        raw.to_owned()
    }
}

/// Read the direct children of `dir` as a `Vec<PathBuf>`.
fn read_dir_entries(dir: &Path) -> Result<Vec<PathBuf>, MemoryError> {
    let iter = fs::read_dir(dir)
        .map_err(|e| MemoryError::Store(format!("read dir {}: {e}", dir.display())))?;
    let mut entries = Vec::new();
    for entry in iter {
        let entry =
            entry.map_err(|e| MemoryError::Store(format!("dir entry in {}: {e}", dir.display())))?;
        entries.push(entry.path());
    }
    Ok(entries)
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StoreConfig;
    use tempfile::TempDir;

    fn make_store() -> (BlobStore, TempDir) {
        let tmp = TempDir::new().unwrap();
        let config = StoreConfig {
            blob_dir: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let store = BlobStore::new(&config).unwrap();
        (store, tmp)
    }

    #[test]
    fn write_and_read_roundtrip() {
        let (store, _tmp) = make_store();
        let id = MemoryId::new_v4();
        store
            .write(&id, MemoryLayer::L1, MemoryCategory::Decision, "hello world")
            .unwrap();
        let content = store
            .read(&id, MemoryLayer::L1, MemoryCategory::Decision)
            .unwrap()
            .expect("should exist");
        assert_eq!(content.trim(), "hello world");
    }

    #[test]
    fn read_missing_returns_none() {
        let (store, _tmp) = make_store();
        let id = MemoryId::new_v4();
        let result = store
            .read(&id, MemoryLayer::L0, MemoryCategory::Reference)
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn delete_is_idempotent() {
        let (store, _tmp) = make_store();
        let id = MemoryId::new_v4();
        store
            .delete(&id, MemoryLayer::L2, MemoryCategory::Shared)
            .unwrap();
    }

    #[test]
    fn list_returns_written_ids() {
        let (store, _tmp) = make_store();
        let id1 = MemoryId::new_v4();
        let id2 = MemoryId::new_v4();
        store
            .write(&id1, MemoryLayer::L3, MemoryCategory::UserPreference, "a")
            .unwrap();
        store
            .write(&id2, MemoryLayer::L3, MemoryCategory::Decision, "b")
            .unwrap();
        let mut ids = store.list(MemoryLayer::L3).unwrap();
        ids.sort();
        let mut expected = vec![id1, id2];
        expected.sort();
        assert_eq!(ids, expected);
    }

    #[test]
    fn stats_counts_files() {
        let (store, _tmp) = make_store();
        let id = MemoryId::new_v4();
        store
            .write(&id, MemoryLayer::L1, MemoryCategory::Reference, "x")
            .unwrap();
        let s = store.stats().unwrap();
        assert_eq!(s.file_count, 1);
        assert!(s.total_bytes > 0);
    }
}
