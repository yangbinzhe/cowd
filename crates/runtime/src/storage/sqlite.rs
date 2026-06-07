use std::path::Path;
use std::sync::Mutex;

use rusqlite::Connection;

use crate::error::CowdError;
use crate::storage::StorageBackend;

pub struct SqliteStorage {
    conn: Mutex<Connection>,
}

impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CowdError> {
        let conn = Connection::open(path).map_err(|e| CowdError::other(e.to_string()))?;
        conn.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))
            .map_err(|e| CowdError::other(e.to_string()))?;
        conn.query_row("PRAGMA busy_timeout=5000", [], |_| Ok(()))
            .map_err(|e| CowdError::other(e.to_string()))?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")
            .map_err(|e| CowdError::other(e.to_string()))?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            )",
        )
        .map_err(|e| CowdError::other(e.to_string()))?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl StorageBackend for SqliteStorage {
    fn write(&self, key: &str, value: &[u8]) -> Result<(), CowdError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| CowdError::other(e.to_string()))?;
        conn.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
            rusqlite::params![key, value],
        )
        .map_err(|e| CowdError::other(e.to_string()))?;
        Ok(())
    }

    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, CowdError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| CowdError::other(e.to_string()))?;
        let mut stmt = conn
            .prepare("SELECT value FROM kv WHERE key = ?1")
            .map_err(|e| CowdError::other(e.to_string()))?;
        let mut rows = stmt
            .query_map(rusqlite::params![key], |row| row.get::<_, Vec<u8>>(0))
            .map_err(|e| CowdError::other(e.to_string()))?;
        let result = rows
            .next()
            .transpose()
            .map_err(|e| CowdError::other(e.to_string()))?;
        Ok(result)
    }

    fn delete(&self, key: &str) -> Result<(), CowdError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| CowdError::other(e.to_string()))?;
        conn.execute("DELETE FROM kv WHERE key = ?1", rusqlite::params![key])
            .map_err(|e| CowdError::other(e.to_string()))?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CowdError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| CowdError::other(e.to_string()))?;
        let mut stmt = conn
            .prepare("SELECT key FROM kv WHERE key LIKE ?1")
            .map_err(|e| CowdError::other(e.to_string()))?;
        let pattern = format!("{prefix}%");
        let keys: Vec<String> = stmt
            .query_map(rusqlite::params![pattern], |row| row.get(0))
            .map_err(|e| CowdError::other(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(keys)
    }

    fn flush(&self) -> Result<(), CowdError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_db() -> (SqliteStorage, String) {
        let counter = DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = format!(
            "{}/cowd-sqlite-test-{}-{counter}.db",
            std::env::temp_dir().display(),
            std::process::id()
        );
        let _ = fs::remove_file(&path);
        let s = SqliteStorage::open(&path).expect("open sqlite");
        (s, path)
    }

    #[test]
    fn sqlite_write_and_read() {
        let (s, path) = temp_db();
        s.write("k1", b"hello").unwrap();
        let v = s.read("k1").unwrap().expect("should exist");
        assert_eq!(v, b"hello");
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn sqlite_read_missing_returns_none() {
        let (s, path) = temp_db();
        assert!(s.read("no-such").unwrap().is_none());
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn sqlite_update_overwrites() {
        let (s, path) = temp_db();
        s.write("k1", b"old").unwrap();
        s.write("k1", b"new").unwrap();
        assert_eq!(s.read("k1").unwrap().unwrap(), b"new");
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn sqlite_delete() {
        let (s, path) = temp_db();
        s.write("k1", b"x").unwrap();
        s.delete("k1").unwrap();
        assert!(s.read("k1").unwrap().is_none());
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn sqlite_list_by_prefix() {
        let (s, path) = temp_db();
        s.write("sess-1", b"a").unwrap();
        s.write("sess-2", b"b").unwrap();
        s.write("other", b"c").unwrap();
        let keys = s.list("sess").unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"sess-1".to_string()));
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn sqlite_flush_is_noop() {
        let (s, path) = temp_db();
        s.flush().unwrap();
        let _ = fs::remove_file(&path);
    }
}
