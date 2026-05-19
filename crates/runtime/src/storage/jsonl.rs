use std::path::PathBuf;
use std::{fmt, fs, io::{BufRead, BufReader, Write}};

use crate::error::CowdError;
use crate::storage::StorageBackend;

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
        if all.is_empty() { Ok(None) } else { Ok(Some(all)) }
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
        f.debug_struct("JsonlStorage").field("dir", &self.dir).finish()
    }
}
