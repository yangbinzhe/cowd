use std::sync::Arc;
use crate::store::{MemoryStore, MemoryEntry, Priority};

pub struct MemoryCloset {
    pub entry_id: String,
    pub entities: Vec<String>,
    pub topics: Vec<String>,
    pub key_quote: String,
    pub weight: u8,
    pub flags: Vec<String>,
    pub drawer_id: String,
}

impl MemoryCloset {
    pub fn from_entry(entry: &MemoryEntry) -> Self {
        let entities: Vec<String> = entry.tags.iter().filter(|t| !t.starts_with("type:")).cloned().collect();
        let topics: Vec<String> = entry.content.split_whitespace().take(8)
            .map(|s| s.to_lowercase().replace(|c: char| !c.is_alphanumeric(), ""))
            .filter(|s| s.len() > 2).collect();
        let key_quote: String = entry.content.chars().take(80).collect();

        let flags = vec![
            if entry.priority >= Priority::High { "CORE" } else { "" },
            if entry.access_count > 5 { "HOT" } else { "" },
        ].into_iter().filter(|s| !s.is_empty()).map(|s| s.to_string()).collect();

        Self {
            entry_id: entry.id.clone(),
            entities,
            topics,
            key_quote,
            weight: match entry.priority {
                Priority::Critical => 5,
                Priority::High => 4,
                Priority::Normal => 2,
                Priority::Low => 1,
            },
            flags,
            drawer_id: entry.id.clone(),
        }
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(&self.entities.join("|"));
        out.push('|');
        out.push_str(&self.topics.join(" "));
        out.push_str(&format!("|\"{}\"|W={}|{}", self.key_quote, self.weight, self.flags.join(",")));
        out
    }

    pub fn render_compact(&self) -> String {
        format!("[{}] W={} {}", self.entities.join(","), self.weight, self.key_quote.chars().take(60).collect::<String>())
    }
}

pub struct ClosetIndex {
    store: Arc<MemoryStore>,
}

impl ClosetIndex {
    pub fn new(store: Arc<MemoryStore>) -> Self { Self { store } }

    pub fn search_closets(&self, query: &str, limit: usize) -> Vec<MemoryCloset> {
        self.store.search_fts(query, limit).unwrap_or_default()
            .iter()
            .map(MemoryCloset::from_entry)
            .collect()
    }

    pub fn render_search_results(query: &str, closets: &[MemoryCloset]) -> String {
        if closets.is_empty() {
            return format!("no memories found for: {query}");
        }
        let mut out = format!("## Memory search: {query}\n\n");
        for (i, c) in closets.iter().enumerate() {
            out.push_str(&format!("{}. {}", i + 1, c.render_compact()));
            out.push('\n');
        }
        out.push_str("\nUse search_memory detail:<id> to expand");
        out
    }
}
