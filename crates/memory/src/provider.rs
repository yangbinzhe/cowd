use async_trait::async_trait;
use crate::types::{MemoryEntry, Message};

#[async_trait]
pub trait MemoryProvider: Send + Sync {
    async fn store(&self, entry: MemoryEntry) -> Result<(), String>;
    async fn search(&self, query: &str, limit: usize) -> Result<Vec<MemoryEntry>, String>;
    async fn on_turn_end(&self, _messages: &[Message]) -> Result<(), String> { Ok(()) }
    async fn shutdown(&self) -> Result<(), String> { Ok(()) }

    // Lifecycle hooks (hermes-agent inspired, all no-op by default)
    async fn on_session_start(&self, _session_id: &str) -> Result<(), String> { Ok(()) }
    async fn on_turn_start(&self, _turn_number: u32, _message: &str) -> Result<(), String> { Ok(()) }
    async fn on_pre_compress(&self, _messages: &[Message]) -> Result<String, String> { Ok(String::new()) }
    async fn on_session_switch(&self, _new_id: &str, _reset: bool) -> Result<(), String> { Ok(()) }
    async fn on_delegation(&self, _task: &str, _result: &str) -> Result<(), String> { Ok(()) }
    async fn on_memory_write(&self, _action: &str, _target: &str, _content: &str) -> Result<(), String> { Ok(()) }
}

pub struct NoopMemoryProvider;
#[async_trait]
impl MemoryProvider for NoopMemoryProvider {
    async fn store(&self, _entry: MemoryEntry) -> Result<(), String> { Ok(()) }
    async fn search(&self, _query: &str, _limit: usize) -> Result<Vec<MemoryEntry>, String> { Ok(vec![]) }
}

pub struct FileMemoryProvider { path: std::path::PathBuf }
impl FileMemoryProvider { pub fn new(path: std::path::PathBuf) -> Self { Self { path } } }
#[async_trait]
impl MemoryProvider for FileMemoryProvider {
    async fn store(&self, entry: MemoryEntry) -> Result<(), String> {
        let json = serde_json::to_string(&entry).map_err(|e| format!("{e}"))?;
        tokio::fs::write(self.path.join(format!("{}.json", entry.id)), json).await.map_err(|e| format!("{e}"))
    }
    async fn search(&self, _query: &str, _limit: usize) -> Result<Vec<MemoryEntry>, String> { Ok(vec![]) }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn m4_noop_constructs() { let _p = NoopMemoryProvider; }
    #[test] fn m4_file_constructs() { let d = tempfile::tempdir().unwrap(); let _p = FileMemoryProvider::new(d.path().to_path_buf()); }
    #[test] fn m4_trait_accepts_noop() { fn _v<P: MemoryProvider>(_: P) {} _v(NoopMemoryProvider); }
    #[test] fn m4_trait_accepts_file() { fn _v<P: MemoryProvider>(_: P) {} let d = tempfile::tempdir().unwrap(); _v(FileMemoryProvider::new(d.path().to_path_buf())); }
}
