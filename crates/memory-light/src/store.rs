use std::path::Path;
use std::sync::Mutex;

use chrono::Utc;
use rusqlite::{params, Connection};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryLayer { L1, L2 }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryCategory { Decision, Preference, Convention, Reference, Summary, Verbatim }

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority { Low = 0, Normal = 1, High = 2, Critical = 3 }

#[derive(Debug, Clone)]
pub struct MemoryEntry {
    pub id: String, pub layer: MemoryLayer, pub category: MemoryCategory,
    pub priority: Priority, pub title: String, pub content: String,
    pub tags: Vec<String>, pub created_at: chrono::DateTime<Utc>,
    pub updated_at: chrono::DateTime<Utc>, pub access_count: i64,
}

pub struct MemoryStore { conn: Mutex<Connection> }

fn l2s(l: MemoryLayer) -> &'static str { match l { MemoryLayer::L1 => "L1", MemoryLayer::L2 => "L2" } }
fn c2s(c: MemoryCategory) -> &'static str { match c { MemoryCategory::Decision => "decision", MemoryCategory::Preference => "preference", MemoryCategory::Convention => "convention", MemoryCategory::Reference => "reference", MemoryCategory::Summary => "summary", MemoryCategory::Verbatim => "verbatim" } }
fn p2i(p: Priority) -> i32 { match p { Priority::Low => 0, Priority::Normal => 1, Priority::High => 2, Priority::Critical => 3 } }

impl MemoryStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        conn.execute_batch("CREATE TABLE IF NOT EXISTS memories (id TEXT PRIMARY KEY, layer TEXT NOT NULL, category TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 1, title TEXT NOT NULL, content TEXT NOT NULL, tags TEXT DEFAULT '[]', created_at TEXT NOT NULL, updated_at TEXT NOT NULL, access_count INTEGER DEFAULT 0); CREATE INDEX IF NOT EXISTS idx_mem_layer ON memories(layer); CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(title, content, tokenize='trigram');")?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn insert(&self, entry: &MemoryEntry) -> Result<(), rusqlite::Error> {
        let c = self.conn.lock().unwrap();
        c.execute("INSERT INTO memories (id,layer,category,priority,title,content,tags,created_at,updated_at,access_count) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![entry.id, l2s(entry.layer), c2s(entry.category), p2i(entry.priority), entry.title, entry.content, serde_json::to_string(&entry.tags).unwrap_or_default(), entry.created_at.to_rfc3339(), entry.updated_at.to_rfc3339(), entry.access_count],
        )?;
        Ok(())
    }

    pub fn get_top_entries(&self, layer: MemoryLayer, min_priority: Priority, limit: usize) -> Result<Vec<MemoryEntry>, rusqlite::Error> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare("SELECT id,layer,category,priority,title,content,tags,created_at,updated_at,access_count FROM memories WHERE layer=?1 AND priority>=?2 ORDER BY priority DESC, updated_at DESC LIMIT ?3")?;
        let rows = stmt.query_map(params![l2s(layer), p2i(min_priority), limit as i64], |row| Ok(MemoryEntry {
            id: row.get(0)?, layer, category: MemoryCategory::Reference,
            priority: match row.get::<_, i32>(3)? { 0 => Priority::Low, 1 => Priority::Normal, 2 => Priority::High, _ => Priority::Critical },
            title: row.get(4)?, content: row.get(5)?,
            tags: serde_json::from_str(&row.get::<_, String>(6).unwrap_or_default()).unwrap_or_default(),
            created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(7).unwrap_or_default()).map(|d| d.with_timezone(&Utc)).unwrap_or_else(|_| Utc::now()),
            updated_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(8).unwrap_or_default()).map(|d| d.with_timezone(&Utc)).unwrap_or_else(|_| Utc::now()),
            access_count: row.get(9)?,
        }))?;
        let mut entries = Vec::new();
        for row in rows { entries.push(row?); }
        Ok(entries)
    }

    pub fn search_fts(&self, query: &str, limit: usize) -> Result<Vec<MemoryEntry>, rusqlite::Error> {
        let fts = query.split_whitespace().filter(|w| !w.is_empty()).map(|w| format!("\"{}\"", w.replace('"', "\"\""))).collect::<Vec<_>>().join(" OR ");
        if fts.is_empty() { return Ok(Vec::new()); }
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare("SELECT m.id,m.layer,m.category,m.priority,m.title,m.content,m.tags,m.created_at,m.updated_at,m.access_count FROM memories_fts fts JOIN memories m ON m.rowid=fts.rowid WHERE memories_fts MATCH ?1 ORDER BY rank LIMIT ?2")?;
        let rows = stmt.query_map(params![fts, limit as i64], |row| Ok(MemoryEntry {
            id: row.get(0)?, layer: MemoryLayer::L1, category: MemoryCategory::Reference,
            priority: match row.get::<_, i32>(3)? { 0 => Priority::Low, 1 => Priority::Normal, 2 => Priority::High, _ => Priority::Critical },
            title: row.get(4)?, content: row.get(5)?,
            tags: serde_json::from_str(&row.get::<_, String>(6).unwrap_or_default()).unwrap_or_default(),
            created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(7).unwrap_or_default()).map(|d| d.with_timezone(&Utc)).unwrap_or_else(|_| Utc::now()),
            updated_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(8).unwrap_or_default()).map(|d| d.with_timezone(&Utc)).unwrap_or_else(|_| Utc::now()),
            access_count: row.get(9)?,
        }))?;
        let mut entries = Vec::new();
        for row in rows { entries.push(row?); }
        Ok(entries)
    }

    pub fn delete_entry(&self, id: &str) -> Result<bool, rusqlite::Error> {
        Ok(self.conn.lock().unwrap().execute("DELETE FROM memories WHERE id=?1", params![id])? > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn temp_store() -> MemoryStore {
        let path = format!("/tmp/cowd_test_{}.db", uuid::Uuid::new_v4());
        MemoryStore::open(&path).unwrap()
    }
    #[test] fn test_store_open() {
        let _store = temp_store();
    }
    #[test] fn test_empty_search() {
        let store = temp_store();
        let results = store.search_fts("nonexistent", 5).unwrap();
        assert!(results.is_empty());
    }
    #[test] fn test_delete() {
        let store = temp_store();
        let id = "test_delete_id".to_string();
        let entry = MemoryEntry { id: id.clone(), layer: MemoryLayer::L1, category: MemoryCategory::Reference, priority: Priority::Normal, title: "x".into(), content: "y".into(), tags: vec![], created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(), access_count: 0 };
        store.insert(&entry).unwrap();
        assert!(store.delete_entry(&id).unwrap());
        assert!(!store.delete_entry(&id).unwrap());
    }
}
