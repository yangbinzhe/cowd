use std::path::Path;
use std::sync::Mutex;

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::{StorageBackend, StorageError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlitePragmaConfig {
    pub busy_timeout_ms: u64,
    pub journal_mode: String,
    pub foreign_keys: bool,
    pub synchronous: String,
    pub temp_store: String,
    pub mmap_size_bytes: Option<u64>,
}

impl Default for SqlitePragmaConfig {
    fn default() -> Self {
        Self {
            busy_timeout_ms: 5_000,
            journal_mode: "WAL".to_string(),
            foreign_keys: true,
            synchronous: "NORMAL".to_string(),
            temp_store: "MEMORY".to_string(),
            mmap_size_bytes: None,
        }
    }
}

pub struct SqliteConnectionFactory {
    pragma: SqlitePragmaConfig,
}

impl SqliteConnectionFactory {
    pub fn new(pragma: SqlitePragmaConfig) -> Self {
        Self { pragma }
    }

    pub fn open(&self, path: impl AsRef<Path>) -> Result<Connection, StorageError> {
        let connection = Connection::open(path)?;
        self.apply_pragmas(&connection)?;
        Ok(connection)
    }

    pub fn open_in_memory(&self) -> Result<Connection, StorageError> {
        let connection = Connection::open_in_memory()?;
        self.apply_pragmas(&connection)?;
        Ok(connection)
    }

    pub fn apply_pragmas(&self, connection: &Connection) -> Result<(), StorageError> {
        connection.pragma_update(None, "busy_timeout", self.pragma.busy_timeout_ms)?;
        connection.pragma_update(None, "journal_mode", self.pragma.journal_mode.as_str())?;
        connection.pragma_update(None, "foreign_keys", self.pragma.foreign_keys)?;
        connection.pragma_update(None, "synchronous", self.pragma.synchronous.as_str())?;
        connection.pragma_update(None, "temp_store", self.pragma.temp_store.as_str())?;
        if let Some(mmap_size) = self.pragma.mmap_size_bytes {
            connection.pragma_update(None, "mmap_size", mmap_size)?;
        }
        Ok(())
    }
}

impl Default for SqliteConnectionFactory {
    fn default() -> Self {
        Self::new(SqlitePragmaConfig::default())
    }
}

pub struct SqliteStorage {
    conn: Mutex<Connection>,
}

impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let conn = SqliteConnectionFactory::default().open(path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            )",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl StorageBackend for SqliteStorage {
    fn write(&self, key: &str, value: &[u8]) -> Result<(), StorageError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StorageError::Other(e.to_string()))?;
        conn.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
            rusqlite::params![key, value],
        )?;
        Ok(())
    }

    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let mut stmt = conn.prepare("SELECT value FROM kv WHERE key = ?1")?;
        let mut rows = stmt.query_map(rusqlite::params![key], |row| row.get::<_, Vec<u8>>(0))?;
        rows.next().transpose().map_err(StorageError::from)
    }

    fn delete(&self, key: &str) -> Result<(), StorageError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StorageError::Other(e.to_string()))?;
        conn.execute("DELETE FROM kv WHERE key = ?1", rusqlite::params![key])?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let mut stmt = conn.prepare("SELECT key FROM kv WHERE key LIKE ?1")?;
        let pattern = format!("{prefix}%");
        let keys: Vec<String> = stmt
            .query_map(rusqlite::params![pattern], |row| row.get(0))?
            .filter_map(Result::ok)
            .collect();
        Ok(keys)
    }

    fn flush(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_factory_applies_core_pragmas() {
        let conn = SqliteConnectionFactory::default().open_in_memory().unwrap();
        let busy_timeout: u64 = conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .unwrap();
        let foreign_keys: u64 = conn
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .unwrap();
        let synchronous: u64 = conn
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .unwrap();
        let temp_store: u64 = conn
            .query_row("PRAGMA temp_store", [], |row| row.get(0))
            .unwrap();
        assert_eq!(busy_timeout, 5_000);
        assert_eq!(foreign_keys, 1);
        assert_eq!(synchronous, 1);
        assert_eq!(temp_store, 2);
    }
}
