// M3: UnifiedSessionStore — functional JSONL-based session persistence.
// Merges runtime::Session + memory::SqliteSessionStore + session_store::SessionStore
// Derived from opencode's single session/ module pattern.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedSession {
    pub id: String,
    pub title: String,
    pub model: String,
    pub messages: Vec<UnifiedMessage>,
    pub created_at: i64,
    pub updated_at: i64,
    pub compaction_count: u32,
    pub input_tokens: u64,
    pub output_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedMessage {
    pub role: String,
    pub content: String,
    pub timestamp: i64,
}

#[derive(Debug)]
pub struct UnifiedSessionStore {
    db_path: PathBuf,
    jsonl_export_dir: PathBuf,
}

impl UnifiedSessionStore {
    pub fn new(db_path: PathBuf) -> Self {
        let _ = fs::create_dir_all(&db_path);
        let export_dir = db_path.parent().unwrap_or_else(|| std::path::Path::new(".")).join("exports");
        let _ = fs::create_dir_all(&export_dir);
        Self { db_path, jsonl_export_dir: export_dir }
    }

    pub fn save(&self, session: &UnifiedSession) -> Result<(), String> {
        let path = self.session_path(&session.id);
        let mut f = fs::File::create(&path).map_err(|e| format!("create: {e}"))?;
        let json = serde_json::to_string(session).map_err(|e| format!("serialize: {e}"))?;
        f.write_all(json.as_bytes()).map_err(|e| format!("write: {e}"))?;
        f.write_all(b"\n").map_err(|e| format!("write: {e}"))?;
        Ok(())
    }

    pub fn load(&self, id: &str) -> Result<Option<UnifiedSession>, String> {
        let path = self.session_path(id);
        if !path.exists() { return Ok(None); }
        let f = fs::File::open(&path).map_err(|e| format!("open: {e}"))?;
        let reader = BufReader::new(f);
        for line in reader.lines() {
            let line = line.map_err(|e| format!("read: {e}"))?;
            if line.trim().is_empty() { continue; }
            return serde_json::from_str(&line).map(Some).map_err(|e| format!("parse: {e}"));
        }
        Ok(None)
    }

    pub fn list(&self) -> Result<Vec<UnifiedSession>, String> {
        let mut sessions = Vec::new();
        let entries = fs::read_dir(&self.db_path).map_err(|e| format!("read_dir: {e}"))?;
        for entry in entries {
            let entry = entry.map_err(|e| format!("entry: {e}"))?;
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "jsonl") {
                let id = path.file_stem().unwrap_or_default().to_string_lossy().to_string();
                if let Ok(Some(s)) = self.load(&id) {
                    sessions.push(s);
                }
            }
        }
        sessions.sort_by_key(|s| -s.updated_at);
        Ok(sessions)
    }

    pub fn delete(&self, id: &str) -> Result<(), String> {
        let path = self.session_path(id);
        if path.exists() { fs::remove_file(&path).map_err(|e| format!("remove: {e}"))?; }
        Ok(())
    }

    pub fn export_jsonl(&self, id: &str) -> Result<PathBuf, String> {
        let src = self.session_path(id);
        let dst = self.jsonl_export_dir.join(format!("{}.jsonl", id));
        if src.exists() && !dst.exists() {
            fs::copy(&src, &dst).map_err(|e| format!("copy: {e}"))?;
        }
        Ok(dst)
    }

    /// M3-L1-3: Import old .jsonl session files into the unified store
    pub fn import_legacy_jsonl(&self, path: &std::path::Path) -> Result<usize, String> {
        let content = fs::read_to_string(path).map_err(|e| format!("read: {e}"))?;
        let mut count = 0;
        for line in content.lines() {
            if line.trim().is_empty() { continue; }
            if let Ok(mut session) = serde_json::from_str::<UnifiedSession>(line) {
                session.updated_at = now_ms();
                self.save(&session)?;
                count += 1;
            }
        }
        Ok(count)
    }

    fn session_path(&self, id: &str) -> PathBuf {
        self.db_path.join(format!("{}.jsonl", id))
    }
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn m3_save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = UnifiedSessionStore::new(dir.path().to_path_buf());
        let s = UnifiedSession {
            id: "test-1".into(), title: "Test".into(), model: "sonnet".into(),
            messages: vec![UnifiedMessage { role: "user".into(), content: "hi".into(), timestamp: 1 }],
            created_at: 1, updated_at: 2, compaction_count: 0, input_tokens: 10, output_tokens: 5,
        };
        store.save(&s).unwrap();
        let loaded = store.load("test-1").unwrap().unwrap();
        assert_eq!(loaded.id, "test-1");
        assert_eq!(loaded.messages.len(), 1);
    }

    #[test]
    fn m3_list_returns_saved_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let store = UnifiedSessionStore::new(dir.path().to_path_buf());
        for i in 0..3 {
            let s = UnifiedSession {
                id: format!("s{}", i), title: format!("Session {}", i), model: "sonnet".into(),
                messages: vec![], created_at: i, updated_at: i * 10,
                compaction_count: 0, input_tokens: 0, output_tokens: 0,
            };
            store.save(&s).unwrap();
        }
        let list = store.list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].id, "s2"); // sorted by -updated_at
    }

    #[test]
    fn m3_delete_removes_session() {
        let dir = tempfile::tempdir().unwrap();
        let store = UnifiedSessionStore::new(dir.path().to_path_buf());
        let s = UnifiedSession {
            id: "del".into(), title: "D".into(), model: "m".into(),
            messages: vec![], created_at: 0, updated_at: 0,
            compaction_count: 0, input_tokens: 0, output_tokens: 0,
        };
        store.save(&s).unwrap();
        assert!(store.load("del").unwrap().is_some());
        store.delete("del").unwrap();
        assert!(store.load("del").unwrap().is_none());
    }

    #[test]
    fn m3_export_jsonl_makes_copy() {
        let dir = tempfile::tempdir().unwrap();
        let store = UnifiedSessionStore::new(dir.path().to_path_buf());
        let s = UnifiedSession {
            id: "export".into(), title: "E".into(), model: "m".into(),
            messages: vec![], created_at: 0, updated_at: 0,
            compaction_count: 0, input_tokens: 0, output_tokens: 0,
        };
        store.save(&s).unwrap();
        let exported = store.export_jsonl("export").unwrap();
        assert!(exported.exists());
    }

    #[test]
    fn m3_import_legacy_jsonl_works() {
        let dir = tempfile::tempdir().unwrap();
        let store = UnifiedSessionStore::new(dir.path().to_path_buf());
        let legacy = dir.path().join("legacy.jsonl");
        let s = UnifiedSession {
            id: "legacy-1".into(), title: "Legacy".into(), model: "old".into(),
            messages: vec![], created_at: 0, updated_at: 0,
            compaction_count: 0, input_tokens: 0, output_tokens: 0,
        };
        std::fs::write(&legacy, serde_json::to_string(&s).unwrap() + "\n").unwrap();
        let count = store.import_legacy_jsonl(&legacy).unwrap();
        assert_eq!(count, 1);
        assert!(store.load("legacy-1").unwrap().is_some());
    }
}