mod sqlite;

pub use sqlite::SqliteStorage;

use crate::error::CowdError;
use std::path::PathBuf;

pub trait StorageBackend: Send + Sync {
    fn write(&self, key: &str, value: &[u8]) -> Result<(), CowdError>;
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, CowdError>;
    fn delete(&self, key: &str) -> Result<(), CowdError>;
    fn list(&self, prefix: &str) -> Result<Vec<String>, CowdError>;
    fn flush(&self) -> Result<(), CowdError>;
}

pub enum StorageType {
    Sqlite { path: PathBuf },
}

pub fn create_storage(st: StorageType) -> Result<Box<dyn StorageBackend>, CowdError> {
    match st {
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
