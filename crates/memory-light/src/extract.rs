use std::sync::Arc;
use std::time::Duration;

use crate::store::{MemoryEntry, MemoryLayer, MemoryStore, Priority, MemoryCategory};

#[derive(Debug, Clone)]
pub struct TurnPayload {
    pub user_msg: String,
    pub assistant_msg: String,
}

pub fn spawn_extractor(
    store: Arc<MemoryStore>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<TurnPayload>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut buffer: Vec<TurnPayload> = Vec::new();
        let mut last_flush = std::time::Instant::now();
        loop {
            match rx.blocking_recv() {
                Some(payload) => buffer.push(payload),
                None => break,
            }
            if buffer.len() >= batch_size() || (!buffer.is_empty() && last_flush.elapsed() >= Duration::from_secs(30)) {
                let batch: Vec<TurnPayload> = buffer.drain(..).collect();
                process_batch(&store, &batch);
                last_flush = std::time::Instant::now();
            }
        }
    })
}

fn batch_size() -> usize {
    std::env::var("COWD_EXTRACT_BATCH_SIZE").ok().and_then(|v| v.parse().ok()).unwrap_or(3)
}

fn process_batch(store: &Arc<MemoryStore>, turns: &[TurnPayload]) {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for turn in turns {
        let _ = store.insert(&new_entry(MemoryLayer::L2, MemoryCategory::Verbatim, Priority::Normal,
            &format!("turn: {}", chrono::Utc::now().format("%H:%M")),
            &format!("user: {}\nassistant: {}", turn.user_msg, turn.assistant_msg),
            &["verbatim"]));
        if let Some(d) = detect_decision(&turn.assistant_msg) {
            let norm = d.title.to_lowercase().replace(|c: char| !c.is_alphanumeric(), "");
            if seen.insert(norm.clone()) {
                if store.search_fts(&norm, 1).unwrap_or_default().is_empty() {
                    let _ = store.insert(&new_entry(MemoryLayer::L1, MemoryCategory::Decision, Priority::High, &d.title, &d.content, &["decision"]));
                }
            }
        }
        if let Some(p) = detect_preference(&turn.user_msg) {
            let norm = p.title.to_lowercase().replace(|c: char| !c.is_alphanumeric(), "");
            if seen.insert(norm.clone()) {
                if store.search_fts(&norm, 1).unwrap_or_default().is_empty() {
                    let _ = store.insert(&new_entry(MemoryLayer::L1, MemoryCategory::Preference, Priority::High, &p.title, &p.content, &["preference"]));
                }
            }
        }
        if let Some(c) = detect_convention(&turn.assistant_msg) {
            let norm = c.title.to_lowercase().replace(|c: char| !c.is_alphanumeric(), "");
            if seen.insert(norm) {
                let _ = store.insert(&new_entry(MemoryLayer::L2, MemoryCategory::Convention, Priority::Normal, &c.title, &c.content, &["convention"]));
            }
        }
    }
}

struct Extraction { title: String, content: String }

fn detect_decision(text: &str) -> Option<Extraction> {
    let lower = text.to_lowercase();
    for s in &["i'll use", "we'll use", "we decided", "the approach is"] {
        if let Some(pos) = lower.find(s) {
            let snippet = &text[pos..];
            let line = snippet.lines().next().unwrap_or("");
            return Some(Extraction { title: line.chars().take(80).collect(), content: snippet.chars().take(400).collect() });
        }
    }
    None
}

fn detect_preference(text: &str) -> Option<Extraction> {
    let lower = text.to_lowercase();
    for s in &[
        "always", "never", "prefer", "don't", "do not", "please",
        "记住", "确保", "不要", "别用", "不能用", "禁止使用", "建议使用",
        "推荐使用", "最好用", "习惯用", "一直用", "从来不用", "以前都用",
        "以后都用", "尽量用", "必须用", "必须", "一定要", "千万别",
        "切记", "务必", "优先", "首选", "倾向于", "偏好", "不喜欢",
        "不推荐", "避免", "尽量别", "不要用",
    ] {
        if lower.contains(s) {
            let line = text.lines().next().unwrap_or("");
            return Some(Extraction { title: format!("Preference: {}", &line[..line.len().min(60)]), content: text.chars().take(300).collect() });
        }
    }
    None
}

fn detect_convention(text: &str) -> Option<Extraction> {
    for cmd in &[
        "cargo ", "npm ", "yarn ", "pip ", "pip3 ",
        "git ", "docker ", "kubectl ", "make ", "go ",
        "rustc ", "terraform ", "helm ", "poetry ",
        "conda ", "brew ", "apt ", "dnf ", "pnpm ", "npx ",
    ] {
        if text.contains(cmd) {
            let lines: Vec<&str> = text.lines().filter(|l| l.contains(cmd)).collect();
            if let Some(line) = lines.first() {
                return Some(Extraction { title: format!("Command: {}", line.trim()), content: line.trim().to_string() });
            }
        }
    }
    None
}

fn new_entry(layer: MemoryLayer, cat: MemoryCategory, prio: Priority, title: &str, content: &str, tags: &[&str]) -> MemoryEntry {
    MemoryEntry {
        id: uuid::Uuid::new_v4().to_string(), layer, category: cat, priority: prio,
        title: title.to_string(), content: content.to_string(),
        tags: tags.iter().map(|s| s.to_string()).collect(),
        created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(), access_count: 0,
    }
}
