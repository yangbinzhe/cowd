use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::store::{MemoryEntry, MemoryLayer, MemoryStore, Priority};
use crate::extract::{TurnPayload, spawn_extractor};

pub struct IdentityLayer {
    path: PathBuf,
}

impl IdentityLayer {
    pub fn new(home_dir: &Path) -> Self {
        Self { path: home_dir.join("identity.md") }
    }

    pub fn render(&self) -> String {
        if self.path.exists() {
            std::fs::read_to_string(&self.path).unwrap_or_else(|_| Self::default_text())
        } else {
            Self::default_text()
        }
    }

    fn default_text() -> String {
        "## Identity\n\
         - Agent: Cowd AI coding assistant\n\
         - Role: Software engineering, architecture, debugging\n\
         - Style: Concise, direct, evidence-based\n\
         - Available tools: bash, read, write, edit, grep, web_search, memory\n"
            .to_string()
    }
}

pub struct EssentialLayer {
    store: Arc<MemoryStore>,
    max_tokens: usize,
    max_entries: usize,
}

impl EssentialLayer {
    pub fn new(store: Arc<MemoryStore>) -> Self {
        let max_tokens: usize = std::env::var("COWD_MEMORY_MAX_TOKENS").ok()
            .and_then(|v| v.parse().ok()).unwrap_or(800);
        let max_entries: usize = std::env::var("COWD_MEMORY_MAX_ENTRIES").ok()
            .and_then(|v| v.parse().ok()).unwrap_or(15);
        Self { store, max_tokens, max_entries }
    }

    pub fn render(&self) -> String {
        let entries = self.store.get_top_entries(MemoryLayer::L1, Priority::Normal, self.max_entries)
            .unwrap_or_default();

        let mut text = String::from("## Recent Context\n");
        let mut chars = text.len();

        for entry in &entries {
            let summary = truncate_text(&entry.content, 200);
            let line = format!("- [{}] {}\n", entry.title, summary);
            if chars + line.len() > self.max_tokens * 4 {
                text.push_str("... (use search_memory for more)\n");
                break;
            }
            text.push_str(&line);
            chars += line.len();
        }

        if entries.is_empty() {
            text.push_str("  No memories yet.\n");
        }

        let verbatim = self.store.get_top_entries(MemoryLayer::L2, Priority::Normal, 2).unwrap_or_default()
            .into_iter().filter(|e| e.tags.contains(&"verbatim".to_string())).collect::<Vec<_>>();
        if !verbatim.is_empty() {
            text.push_str("## Recent Verbatim\n");
            for v in verbatim.iter().take(2) {
                let snippet: String = v.content.lines().take(4).collect::<Vec<_>>().join("\n");
                text.push_str(&format!("```\n{}\n```\n", snippet.chars().take(400).collect::<String>()));
            }
        }

        text
    }
}

pub struct SearchLayer {
    store: Arc<MemoryStore>,
}

impl SearchLayer {
    pub fn new(store: Arc<MemoryStore>) -> Self {
        Self { store }
    }

    pub fn search(&self, query: &str, limit: usize) -> Vec<MemoryEntry> {
        let fts_results = self.store.search_fts(query, limit * 2).unwrap_or_default();
        let ranker = crate::Bm25Ranker::default();
        let ranked = ranker.rerank(query, fts_results);
        ranked.into_iter().take(limit).map(|(_, e)| e).collect()
    }
}

pub struct MemoryManager {
    l0: IdentityLayer,
    l1: EssentialLayer,
    l2: SearchLayer,
    store: Arc<MemoryStore>,
    extract_tx: mpsc::UnboundedSender<TurnPayload>,
    _extract_handle: std::thread::JoinHandle<()>,
}

impl Drop for MemoryManager {
    fn drop(&mut self) {
        drop(std::mem::replace(&mut self.extract_tx, mpsc::unbounded_channel().0));
    }
}

impl MemoryManager {
    pub fn new(home_dir: &Path) -> Result<Self, rusqlite::Error> {
        let db_path = home_dir.join("memory-light.db");
        let store = Arc::new(MemoryStore::open(db_path)?);

        let l0 = IdentityLayer::new(home_dir);
        let l1 = EssentialLayer::new(store.clone());
        let l2 = SearchLayer::new(store.clone());
        let (tx, rx) = mpsc::unbounded_channel::<TurnPayload>();

        let handle = spawn_extractor(store.clone(), rx);

        Ok(Self { l0, l1, l2, store, extract_tx: tx, _extract_handle: handle })
    }

    pub fn prepare_context(&self) -> String {
        let mut ctx = String::new();
        ctx.push_str(&self.l0.render());
        ctx.push_str("\n\n");
        ctx.push_str(&self.l1.render());
        ctx
    }

    pub fn search(&self, query: &str, limit: usize) -> Vec<MemoryEntry> {
        self.l2.search(query, limit)
    }

    pub fn after_turn(&self, user_msg: &str, assistant_msg: &str) {
        let payload = TurnPayload {
            user_msg: user_msg.to_string(),
            assistant_msg: assistant_msg.to_string(),
        };
        let _ = self.extract_tx.send(payload);
    }
}

fn truncate_text(content: &str, max_chars: usize) -> String {
    let mut result = String::new();
    for line in content.lines() {
        if result.len() + line.len() + 1 > max_chars {
            if !result.is_empty() { result.push_str("..."); }
            break;
        }
        if !result.is_empty() { result.push(' '); }
        result.push_str(line);
    }
    if result.is_empty() { content.chars().take(max_chars.min(3)).collect::<String>() + "..." }
    else { result }
}
