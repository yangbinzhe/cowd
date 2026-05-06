use rusqlite::{params, Connection};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct SearchSnippet {
    pub line_start: usize,
    pub line_end: usize,
    pub content: String,
}

#[derive(Debug, Clone)]
pub struct ToolOutputSummary {
    pub full_size_bytes: usize,
    pub total_lines: usize,
    pub sample_head: String,
    pub sample_tail: String,
    pub keyword_highlights: Vec<String>,
    pub search_hint: String,
}

pub struct ToolOutputSandbox {
    conn: Connection,
}

impl ToolOutputSandbox {
    pub fn new() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("CREATE VIRTUAL TABLE IF NOT EXISTS tool_fts USING fts5(call_id, line_range, content);")?;
        Ok(Self { conn })
    }

    pub fn index(&mut self, call_id: &str, output: &str, threshold: usize) -> Option<ToolOutputSummary> {
        let est = output.len() / 4;
        if est < threshold { return None; }

        let lines: Vec<&str> = output.lines().collect();
        let total = lines.len();
        let (head, tail) = (lines.iter().take(3).copied().collect::<Vec<_>>().join("\n"),
            lines.iter().rev().take(3).copied().collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join("\n"));

        let keywords = extract_keywords(output, 8);
        let chunk = 50;
        let tx = self.conn.transaction().ok()?;
        for start in (0..total).step_by(chunk) {
            let end = (start + chunk).min(total);
            let content = lines[start..end].join("\n");
            let range = format!("L{}-L{}", start + 1, end);
            let _ = tx.execute("INSERT INTO tool_fts(call_id, line_range, content) VALUES (?1,?2,?3)", params![call_id, range, content]);
        }
        let _ = tx.commit();
        Some(ToolOutputSummary {
            full_size_bytes: output.len(), total_lines: total, sample_head: head, sample_tail: tail,
            keyword_highlights: keywords,
            search_hint: format!("Output indexed ({} lines). Use search_tool_output {} <query>", total, call_id),
        })
    }

    pub fn search(&self, call_id: &str, query: &str, limit: usize) -> Vec<SearchSnippet> {
        let mut stmt = match self.conn.prepare("SELECT line_range, content FROM tool_fts WHERE call_id=?1 AND content MATCH ?2 LIMIT ?3") {
            Ok(s) => s, Err(_) => return vec![],
        };
        stmt.query_map(params![call_id, query, limit], |row| {
            let range: String = row.get(0)?;
            let (s, e) = parse_range(&range);
            Ok(SearchSnippet { line_start: s, line_end: e, content: row.get(1)? })
        }).ok().map(|r| r.filter_map(|r| r.ok()).collect()).unwrap_or_default()
    }
}

fn parse_range(r: &str) -> (usize, usize) {
    let parts: Vec<&str> = r.trim_start_matches('L').split("-L").collect();
    (parts.first().and_then(|s| s.parse().ok()).unwrap_or(0), parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0))
}

fn extract_keywords(text: &str, n: usize) -> Vec<String> {
    let stop: &[&str] = &["the","a","an","is","are","was","were","to","of","in","for","on","and","or","not","this","that","with","from","at","by","be","as","it","its","but"];
    let mut freq: HashMap<String, usize> = HashMap::new();
    for word in text.split(|c: char| !c.is_alphanumeric() && c != '_') {
        let w = word.trim().to_lowercase();
        if w.len() < 3 || stop.contains(&w.as_str()) { continue; }
        *freq.entry(w).or_insert(0) += 1;
    }
    let mut v: Vec<_> = freq.into_iter().collect();
    v.sort_by(|a, b| b.1.cmp(&a.1));
    v.truncate(n);
    v.into_iter().map(|(w, _)| w).collect()
}
