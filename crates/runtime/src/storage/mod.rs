mod jsonl;
mod sqlite;

pub use jsonl::JsonlStorage;
pub use sqlite::SqliteStorage;

use crate::error::{CowdError, StorageError};
use std::path::PathBuf;

pub trait StorageBackend: Send + Sync {
    fn write(&self, key: &str, value: &[u8]) -> Result<(), CowdError>;
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, CowdError>;
    fn delete(&self, key: &str) -> Result<(), CowdError>;
    fn list(&self, prefix: &str) -> Result<Vec<String>, CowdError>;
    fn flush(&self) -> Result<(), CowdError>;
}

pub enum StorageType {
    Jsonl { path: PathBuf },
    Sqlite { path: PathBuf },
}

pub fn create_storage(st: StorageType) -> Result<Box<dyn StorageBackend>, CowdError> {
    match st {
        StorageType::Jsonl { path } => Err(CowdError::Storage(StorageError::Corruption(format!(
            "JSONL session storage is retired from runtime creation; import explicitly instead: {}",
            path.display()
        )))),
        StorageType::Sqlite { path } => {
            SqliteStorage::open(path).map(|s| Box::new(s) as Box<dyn StorageBackend>)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_dir() -> PathBuf {
        let n = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("cowd-storage-test-{n}"));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn jsonl_write_and_read() {
        let dir = temp_dir();
        let s = JsonlStorage::open(&dir).unwrap();
        s.write("session", b"{\"id\":\"abc\"}").unwrap();
        let data = s.read("session").unwrap().expect("should exist");
        assert!(String::from_utf8_lossy(&data).contains("abc"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn jsonl_append_preserves_lines() {
        let dir = temp_dir();
        let s = JsonlStorage::open(&dir).unwrap();
        s.write("log", b"line1").unwrap();
        s.write("log", b"line2").unwrap();
        let data = s.read("log").unwrap().expect("should exist");
        let text = String::from_utf8_lossy(&data);
        assert!(text.contains("line1"));
        assert!(text.contains("line2"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn jsonl_read_missing_returns_none() {
        let dir = temp_dir();
        let s = JsonlStorage::open(&dir).unwrap();
        assert!(s.read("no-such").unwrap().is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn jsonl_delete() {
        let dir = temp_dir();
        let s = JsonlStorage::open(&dir).unwrap();
        s.write("tmp", b"xxx").unwrap();
        assert!(s.read("tmp").unwrap().is_some());
        s.delete("tmp").unwrap();
        assert!(s.read("tmp").unwrap().is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn jsonl_list_by_prefix() {
        let dir = temp_dir();
        let s = JsonlStorage::open(&dir).unwrap();
        s.write("sess-1", b"a").unwrap();
        s.write("sess-2", b"b").unwrap();
        s.write("other", b"c").unwrap();
        let keys = s.list("sess").unwrap();
        assert_eq!(keys.len(), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn jsonl_flush_is_noop() {
        let dir = temp_dir();
        let s = JsonlStorage::open(&dir).unwrap();
        s.flush().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sqlite_write_and_read() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).ok();
        let path = dir.join("test.db");
        let s = SqliteStorage::open(&path).unwrap();
        s.write("k1", b"hello").unwrap();
        assert_eq!(s.read("k1").unwrap().unwrap(), b"hello");
    }

    #[test]
    fn sqlite_update_overwrites() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).ok();
        let path = dir.join("test.db");
        let _ = fs::remove_file(&path);
        let s = SqliteStorage::open(&path).unwrap();
        s.write("k1", b"old").unwrap();
        s.write("k1", b"new").unwrap();
        assert_eq!(s.read("k1").unwrap().unwrap(), b"new");
    }

    #[test]
    fn sqlite_list_by_prefix() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).ok();
        let path = dir.join("test.db");
        let _ = fs::remove_file(&path);
        let s = SqliteStorage::open(&path).unwrap();
        s.write("sess-1", b"a").unwrap();
        s.write("sess-2", b"b").unwrap();
        s.write("other", b"c").unwrap();
        let keys = s.list("sess").unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn storage_factory_rejects_jsonl_runtime_creation() {
        let dir = temp_dir();
        let error = match create_storage(StorageType::Jsonl { path: dir.clone() }) {
            Ok(_) => panic!("JSONL runtime storage must be retired"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("JSONL session storage is retired"));
    }

    #[test]
    fn storage_factory_sqlite() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).ok();
        let path = dir.join("test.db");
        let _ = fs::remove_file(&path);
        let s = create_storage(StorageType::Sqlite { path: path.clone() }).unwrap();
        s.write("k", b"v").unwrap();
        assert_eq!(s.read("k").unwrap().unwrap(), b"v");
    }
}
