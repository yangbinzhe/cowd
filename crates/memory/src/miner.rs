//! Multi-mode memory mining: project, conversation, and general extraction.
//!
//! Borrowed from MemPalace miner.py + convo_miner.py: three mining modes
//! with .gitignore awareness, chunk boundaries, and automatic classification.

use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::legacy_jsonl::legacy_jsonl_session_import_enabled;

/// Mining mode selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MiningMode {
    Project,       // Code/docs in a project directory
    Conversations, // Explicit legacy JSONL import/recovery files
    General,       // Free-text extraction
}

/// A mined memory entry ready for insertion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinedEntry {
    pub content: String,
    pub category: MinedCategory,
    pub source_path: Option<String>,
    pub chunk_index: usize,
    pub tokens_estimated: usize,
}

/// Category of mined content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MinedCategory {
    Decision,    // Key decisions made
    Preference,  // User preferences
    Milestone,   // Important milestones
    Context,     // General context
    CodePattern, // Code patterns
    ApiDesign,   // API design decisions
}

/// Memory miner with configurable chunk parameters.
pub struct MemoryMiner {
    pub mode: MiningMode,
    pub chunk_size: usize,    // Default 800 chars (MemPalace)
    pub chunk_overlap: usize, // Default 100 chars
    pub respect_gitignore: bool,
}

impl MemoryMiner {
    pub fn new(mode: MiningMode) -> Self {
        Self {
            mode,
            chunk_size: 800,
            chunk_overlap: 100,
            respect_gitignore: true,
        }
    }

    /// Mine a project directory for memory entries.
    /// Scans code and doc files, respecting .gitignore.
    pub async fn mine_project(&self, root: &Path) -> Result<Vec<MinedEntry>, String> {
        if !root.exists() {
            return Err(format!("Path does not exist: {}", root.display()));
        }

        let mut entries = Vec::new();
        let gitignore_patterns = if self.respect_gitignore {
            read_gitignore(root)
        } else {
            Vec::new()
        };

        // Walk the directory
        for entry_result in walkdir::WalkDir::new(root) {
            let entry = match entry_result {
                Ok(e) => e,
                Err(_) => continue,
            };
            let entry_path = entry.path();

            // Skip directories
            if !entry_path.is_file() {
                continue;
            }

            // Skip gitignored paths
            if is_ignored(entry_path, root, &gitignore_patterns) {
                continue;
            }

            // Skip hidden files
            let is_hidden = entry_path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with('.'))
                .unwrap_or(false);
            if is_hidden {
                continue;
            }

            // Only process text files
            if !is_text_file(entry_path) {
                continue;
            }

            if let Ok(content) = tokio::fs::read_to_string(entry_path).await {
                let relative = entry_path
                    .strip_prefix(root)
                    .unwrap_or(entry_path)
                    .to_string_lossy()
                    .to_string();

                let chunks = chunk_text(&content, self.chunk_size, self.chunk_overlap);
                for (i, chunk) in chunks.into_iter().enumerate() {
                    let category = classify_content(&chunk);
                    let tokens = estimate_tokens(&chunk);
                    entries.push(MinedEntry {
                        content: chunk,
                        category,
                        source_path: Some(relative.clone()),
                        chunk_index: i,
                        tokens_estimated: tokens,
                    });
                }

                // Limit total entries to prevent memory explosion
                if entries.len() >= 500 {
                    break;
                }
            }
        }

        Ok(entries)
    }

    /// Mine explicitly imported legacy conversation JSONL files.
    ///
    /// Managed runtime sessions are stored in SQLite. This path is intentionally
    /// gated and exists only for user-triggered legacy import/recovery flows.
    pub async fn mine_conversations(&self, session_dir: &Path) -> Result<Vec<MinedEntry>, String> {
        if !legacy_jsonl_session_import_enabled() {
            return Ok(Vec::new());
        }

        if !session_dir.exists() {
            return Err(format!(
                "Session dir does not exist: {}",
                session_dir.display()
            ));
        }

        let mut entries = Vec::new();

        // Read explicit legacy JSONL files.
        let mut dir_entries = tokio::fs::read_dir(session_dir)
            .await
            .map_err(|e| e.to_string())?;

        while let Some(entry) = dir_entries.next_entry().await.map_err(|e| e.to_string())? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                continue;
            }

            if let Ok(content) = tokio::fs::read_to_string(&path).await {
                let filename = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                // Parse conversation exchanges
                let exchanges = parse_conversation_exchanges(&content);
                for (i, exchange) in exchanges.into_iter().enumerate() {
                    let category = classify_conversation(&exchange);
                    entries.push(MinedEntry {
                        content: exchange,
                        category,
                        source_path: Some(filename.clone()),
                        chunk_index: i,
                        tokens_estimated: 0,
                    });
                    if let Some(last) = entries.last_mut() {
                        last.tokens_estimated = estimate_tokens(&last.content);
                    }
                }
            }
        }

        Ok(entries)
    }

    /// Mine general text content.
    pub fn mine_general(&self, text: &str) -> Vec<MinedEntry> {
        let chunks = chunk_text(text, self.chunk_size, self.chunk_overlap);
        chunks
            .into_iter()
            .enumerate()
            .map(|(i, chunk)| {
                let category = classify_content(&chunk);
                let tokens = estimate_tokens(&chunk);
                MinedEntry {
                    content: chunk,
                    category,
                    source_path: None,
                    chunk_index: i,
                    tokens_estimated: tokens,
                }
            })
            .collect()
    }
}

/// Chunk text with overlap, respecting paragraph boundaries.
fn chunk_text(text: &str, chunk_size: usize, overlap: usize) -> Vec<String> {
    if text.len() <= chunk_size {
        return if text.trim().is_empty() {
            Vec::new()
        } else {
            vec![text.to_string()]
        };
    }

    let mut chunks = Vec::new();
    let mut start = 0;

    while start < text.len() {
        let end = (start + chunk_size).min(text.len());

        // Try to break at paragraph boundary
        let mut break_point = end;
        if end < text.len() {
            // Look for newline near the end
            if let Some(pos) = text[start..end].rfind("\n\n") {
                break_point = start + pos + 2;
            } else if let Some(pos) = text[start..end].rfind('\n') {
                break_point = start + pos + 1;
            }
        }

        let chunk = text[start..break_point].trim().to_string();
        if !chunk.is_empty() {
            chunks.push(chunk);
        }

        let next_start = if break_point > overlap {
            break_point - overlap
        } else {
            break_point
        };

        // Ensure forward progress: if the overlap would push us backward,
        // advance to break_point instead (sacrificing overlap for correctness).
        start = next_start.max(break_point);

        // Safety net: if still no progress, force advance.
        if start == 0 && break_point == 0 {
            start = 1;
        }
    }

    chunks
}

/// Classify content into a mined category.
fn classify_content(text: &str) -> MinedCategory {
    let lower = text.to_lowercase();

    let decision_signals: &[&str] = &["decided", "decision", "chose", "will use", "决定", "选择"];
    let preference_signals: &[&str] =
        &["prefer", "like", "want", "always", "never", "喜欢", "偏好"];
    let milestone_signals: &[&str] = &[
        "completed",
        "finished",
        "released",
        "deployed",
        "完成",
        "上线",
    ];
    let code_signals: &[&str] = &["fn ", "func ", "def ", "class ", "impl ", "pub ", "import "];
    let api_signals: &[&str] = &["endpoint", "api", "route", "handler", "接口", "路由"];

    let decision_count = decision_signals
        .iter()
        .filter(|s| lower.contains(**s))
        .count();
    let preference_count = preference_signals
        .iter()
        .filter(|s| lower.contains(**s))
        .count();
    let milestone_count = milestone_signals
        .iter()
        .filter(|s| lower.contains(**s))
        .count();
    let code_count = code_signals.iter().filter(|s| text.contains(**s)).count();
    let api_count = api_signals.iter().filter(|s| lower.contains(**s)).count();

    if api_count >= 2 {
        MinedCategory::ApiDesign
    } else if code_count >= 2 {
        MinedCategory::CodePattern
    } else if decision_count >= preference_count && decision_count >= milestone_count {
        MinedCategory::Decision
    } else if preference_count >= milestone_count {
        MinedCategory::Preference
    } else if milestone_count > 0 {
        MinedCategory::Milestone
    } else {
        MinedCategory::Context
    }
}

/// Classify a conversation exchange.
fn classify_conversation(text: &str) -> MinedCategory {
    classify_content(text)
}

/// Parse conversation exchanges from JSONL content.
fn parse_conversation_exchanges(content: &str) -> Vec<String> {
    let mut exchanges = Vec::new();
    let mut current = String::new();

    for line in content.lines() {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
            if let Some(msg_type) = json.get("type").and_then(|t| t.as_str()) {
                if msg_type == "message" {
                    if let Some(content_val) = json.get("content") {
                        if let Some(s) = content_val.as_str() {
                            current.push_str(s);
                            current.push('\n');
                        } else if let Some(arr) = content_val.as_array() {
                            for item in arr {
                                if let Some(s) = item.get("text").and_then(|t| t.as_str()) {
                                    current.push_str(s);
                                    current.push('\n');
                                }
                            }
                        }
                    }
                }
            }
        }

        // Flush exchange every ~800 chars
        if current.len() >= 800 {
            exchanges.push(current.trim().to_string());
            current = String::new();
        }
    }

    if !current.trim().is_empty() {
        exchanges.push(current.trim().to_string());
    }

    exchanges
}

/// Estimate token count (rough: 1 token ≈ 4 chars).
fn estimate_tokens(text: &str) -> usize {
    (text.len() / 4).max(1)
}

/// Read .gitignore patterns from a project root.
fn read_gitignore(root: &Path) -> Vec<String> {
    let gitignore_path = root.join(".gitignore");
    std::fs::read_to_string(&gitignore_path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| l.trim().to_string())
        .collect()
}

/// Check if a path should be ignored based on gitignore patterns.
fn is_ignored(path: &Path, root: &Path, patterns: &[String]) -> bool {
    let relative = path.strip_prefix(root).unwrap_or(path);
    let relative_str = relative.to_string_lossy();

    for pattern in patterns {
        if pattern.ends_with('/') {
            // Directory pattern
            if relative_str.starts_with(pattern) || relative_str.contains(&format!("/{pattern}")) {
                return true;
            }
        } else if relative_str.contains(pattern) {
            return true;
        }
    }
    false
}

/// Check if a file is a text file based on extension.
fn is_text_file(path: &Path) -> bool {
    let text_extensions = [
        "rs",
        "toml",
        "json",
        "yaml",
        "yml",
        "md",
        "txt",
        "py",
        "js",
        "ts",
        "jsx",
        "tsx",
        "html",
        "css",
        "scss",
        "go",
        "java",
        "c",
        "cpp",
        "h",
        "sh",
        "bash",
        "zsh",
        "sql",
        "graphql",
        "proto",
        "dockerfile",
        "gitignore",
        "env",
        "cfg",
        "ini",
        "conf",
        "xml",
        "csv",
    ];

    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| text_extensions.contains(&e.to_lowercase().as_str()))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    struct LegacyJsonlEnvGuard(Option<String>);

    impl LegacyJsonlEnvGuard {
        fn disabled() -> Self {
            let previous = std::env::var("COWD_ENABLE_LEGACY_JSONL_SESSION_IMPORT").ok();
            std::env::remove_var("COWD_ENABLE_LEGACY_JSONL_SESSION_IMPORT");
            Self(previous)
        }
    }

    impl Drop for LegacyJsonlEnvGuard {
        fn drop(&mut self) {
            if let Some(previous) = self.0.take() {
                std::env::set_var("COWD_ENABLE_LEGACY_JSONL_SESSION_IMPORT", previous);
            }
        }
    }

    #[test]
    fn test_chunk_text_small() {
        let chunks = chunk_text("Hello world", 800, 100);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], "Hello world");
    }

    #[test]
    fn test_chunk_text_large() {
        let text = "Line 1\n\nLine 2\n\nLine 3\n\n".repeat(100);
        let chunks = chunk_text(&text, 800, 100);
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            assert!(chunk.len() <= 900); // Allow slight overflow for paragraph breaks
        }
    }

    #[test]
    fn test_classify_content() {
        assert_eq!(
            classify_content("We decided to use Rust for the backend"),
            MinedCategory::Decision
        );
        assert_eq!(
            classify_content("fn main() { println!(\"hello\"); }\nimpl Foo for Bar { }"),
            MinedCategory::CodePattern
        );
        assert_eq!(
            classify_content("I prefer dark mode for coding"),
            MinedCategory::Preference
        );
    }

    #[test]
    fn test_mine_general() {
        let miner = MemoryMiner::new(MiningMode::General);
        let text = "This is a test. We decided to use Axum for the web framework. ".repeat(20);
        let entries = miner.mine_general(&text);
        assert!(!entries.is_empty());
    }

    #[tokio::test]
    async fn test_mine_conversations_skips_legacy_jsonl_without_import_gate() {
        let _env = LegacyJsonlEnvGuard::disabled();
        let tmp = TempDir::new().unwrap();
        std::fs::write(
            tmp.path().join("legacy.jsonl"),
            r#"{"type":"message","content":"We decided to keep SQLite as source of truth."}"#,
        )
        .unwrap();

        let miner = MemoryMiner::new(MiningMode::Conversations);
        let entries = miner.mine_conversations(tmp.path()).await.unwrap();

        assert!(entries.is_empty());
    }
}
