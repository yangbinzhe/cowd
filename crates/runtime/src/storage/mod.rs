use std::path::PathBuf;
use std::{fmt, fs, io::{BufRead, BufReader, Write}};

use crate::error::CowdError;

pub trait StorageBackend: Send + Sync {
    fn write(&self, key: &str, value: &[u8]) -> Result<(), CowdError>;
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, CowdError>;
    fn delete(&self, key: &str) -> Result<(), CowdError>;
    fn list(&self, prefix: &str) -> Result<Vec<String>, CowdError>;
    fn flush(&self) -> Result<(), CowdError>;
}

pub enum StorageType {
    Jsonl { path: PathBuf },
}

pub fn create_storage(st: StorageType) -> Result<Box<dyn StorageBackend>, CowdError> {
    match st {
        StorageType::Jsonl { path } => JsonlStorage::open(path).map(|s| Box::new(s) as Box<dyn StorageBackend>),
    }
}

pub struct JsonlStorage {
    dir: PathBuf,
}

impl JsonlStorage {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, CowdError> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(CowdError::Io)?;
        Ok(Self { dir })
    }

    fn file_path(&self, key: &str) -> PathBuf {
        self.dir.join(format!("{key}.jsonl"))
    }
}

impl StorageBackend for JsonlStorage {
    fn write(&self, key: &str, value: &[u8]) -> Result<(), CowdError> {
        let path = self.file_path(key);
        let line = String::from_utf8_lossy(value).to_string();
        let mut line = line.replace('\n', " ").replace('\r', "");
        line.push('\n');

        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(CowdError::Io)?;
        file.write_all(line.as_bytes()).map_err(CowdError::Io)
    }

    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, CowdError> {
        let path = self.file_path(key);
        if !path.exists() {
            return Ok(None);
        }
        let mut all = Vec::new();
        let file = fs::File::open(&path).map_err(CowdError::Io)?;
        for line in BufReader::new(file).lines() {
            let mut l = line.map_err(CowdError::Io)?;
            l.push('\n');
            all.extend_from_slice(l.as_bytes());
        }
        if all.is_empty() {
            Ok(None)
        } else {
            Ok(Some(all))
        }
    }

    fn delete(&self, key: &str) -> Result<(), CowdError> {
        let path = self.file_path(key);
        if path.exists() {
            fs::remove_file(&path).map_err(CowdError::Io)?;
        }
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, CowdError> {
        let mut results = Vec::new();
        for entry in fs::read_dir(&self.dir).map_err(CowdError::Io)? {
            let entry = entry.map_err(CowdError::Io)?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(prefix) && name.ends_with(".jsonl") {
                results.push(name.trim_end_matches(".jsonl").to_string());
            }
        }
        Ok(results)
    }

    fn flush(&self) -> Result<(), CowdError> {
        Ok(())
    }
}

impl fmt::Debug for JsonlStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonlStorage")
            .field("dir", &self.dir)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        s.write("session", b"{\"id\":\"abc\",\"msgs\":[]}").unwrap();
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
        assert!(s.read("no-such-key").unwrap().is_none());
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
        assert!(keys.contains(&"sess-1".to_string()));
        assert!(keys.contains(&"sess-2".to_string()));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn jsonl_flush_is_noop() {
        let dir = temp_dir();
        let s = JsonlStorage::open(&dir).unwrap();
        s.flush().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }
}
