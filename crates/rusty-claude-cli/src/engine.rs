use std::path::PathBuf;

pub struct Engine {
    pub workspace: PathBuf,
}

impl Engine {
    pub fn new(workspace: PathBuf) -> Self { Self { workspace } }

    pub fn token_count(runtime: &super::BuiltRuntime) -> u64 {
        runtime.usage().cumulative_usage().total_tokens() as u64
    }
    pub fn cost_usd(runtime: &super::BuiltRuntime) -> f64 {
        runtime.usage().cumulative_usage().estimate_cost_usd().total_cost_usd()
    }

    pub fn list_files(&self) -> Vec<FileEntry> {
        let mut files = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.workspace) {
            for entry in entries.flatten() {
                let path = entry.path();
                let name = path.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
                if name.starts_with('.') { continue; }
                files.push(FileEntry {
                    name,
                    is_dir: path.is_dir(),
                    size: if path.is_dir() { 0 } else { path.metadata().map(|m| m.len()).unwrap_or(0) },
                });
            }
        }
        files.sort_by(|a, b| b.is_dir.cmp(&a.is_dir).then(a.name.cmp(&b.name)));
        files
    }

    pub fn memory_handoff(runtime: &super::BuiltRuntime) -> Option<memory::types::HandoffData> {
        runtime.create_memory_handoff()
    }

    pub fn session_stats(runtime: &super::BuiltRuntime) -> SessionStats {
        let s = runtime.session();
        SessionStats { message_count: s.messages.len(), turn_count: runtime.usage().turns() }
    }
}

#[derive(Debug, Clone)]
pub struct SessionStats {
    pub message_count: usize,
    pub turn_count: u32,
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
}
