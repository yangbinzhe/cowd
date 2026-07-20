//! State rebuild mechanism for recovering from context loss.
//!
//! When context overflows or sessions crash, this module reconstructs state
//! from various sources: session history, memory layers, and handoff data.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[cfg(test)]
use tempfile::TempDir;

use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};

use crate::legacy_jsonl::legacy_jsonl_session_import_enabled;
use crate::types::{
    Decision, DecisionStatus, HandoffData, MemoryEntry, MemoryLayer, WorkItem, WorkItemStatus,
};

/// Source of state for reconstruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StateSource {
    /// From session history file (.jsonl)
    SessionHistory,
    /// From memory layer (L0-L4)
    MemoryLayer(MemoryLayer),
    /// From handoff data
    Handoff,
    /// From session store
    SessionStore,
    /// From compressed snapshot
    CompressedSnapshot,
}

impl std::fmt::Display for StateSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateSource::SessionHistory => write!(f, "session_history"),
            StateSource::MemoryLayer(layer) => write!(f, "memory_{:?}", layer),
            StateSource::Handoff => write!(f, "handoff"),
            StateSource::SessionStore => write!(f, "session_store"),
            StateSource::CompressedSnapshot => write!(f, "compressed_snapshot"),
        }
    }
}

/// Reconstructed state item with provenance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateItem<T> {
    /// The actual state data.
    pub data: T,
    /// Source of this state.
    pub source: StateSource,
    /// Confidence score (0.0 - 1.0) of reconstruction accuracy.
    pub confidence: f32,
    /// Timestamp when this state was last modified (if known).
    pub last_modified: Option<i64>,
}

impl<T> StateItem<T> {
    /// Create a new state item.
    pub fn new(data: T, source: StateSource, confidence: f32) -> Self {
        Self {
            data,
            source,
            confidence,
            last_modified: None,
        }
    }

    /// With last modified timestamp.
    pub fn with_modified(mut self, ts: i64) -> Self {
        self.last_modified = Some(ts);
        self
    }
}

/// Rebuilt session state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RebuiltSessionState {
    /// Session identifier.
    pub session_id: Option<String>,
    /// Work items from various sources.
    pub work_items: Vec<StateItem<WorkItem>>,
    /// Decisions made.
    pub decisions: Vec<StateItem<Decision>>,
    /// Memory entries by layer.
    pub memories_by_layer: HashMap<MemoryLayer, Vec<StateItem<MemoryEntry>>>,
    /// Pending tasks.
    pub pending_tasks: Vec<StateItem<String>>,
    /// Context summary.
    pub context_summary: Option<StateItem<String>>,
    /// Confidence of overall reconstruction.
    pub overall_confidence: f32,
}

impl RebuiltSessionState {
    /// Create a new rebuilt state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a work item.
    pub fn add_work_item(&mut self, item: WorkItem, source: StateSource, confidence: f32) {
        self.work_items
            .push(StateItem::new(item, source, confidence));
    }

    /// Add a decision.
    pub fn add_decision(&mut self, decision: Decision, source: StateSource, confidence: f32) {
        self.decisions
            .push(StateItem::new(decision, source, confidence));
    }

    /// Add a memory entry.
    pub fn add_memory(&mut self, entry: MemoryEntry, layer: MemoryLayer, confidence: f32) {
        self.memories_by_layer
            .entry(layer)
            .or_default()
            .push(StateItem::new(
                entry,
                StateSource::MemoryLayer(layer),
                confidence,
            ));
    }

    /// Compute overall confidence from all items.
    pub fn compute_confidence(&mut self) {
        let all_confidences: Vec<f32> = std::iter::empty()
            .chain(self.work_items.iter().map(|i| i.confidence))
            .chain(self.decisions.iter().map(|i| i.confidence))
            .chain(
                self.memories_by_layer
                    .values()
                    .flat_map(|v| v.iter().map(|i| i.confidence)),
            )
            .chain(self.pending_tasks.iter().map(|i| i.confidence))
            .collect();

        self.overall_confidence = if all_confidences.is_empty() {
            0.0
        } else {
            all_confidences.iter().sum::<f32>() / all_confidences.len() as f32
        };
    }

    /// Get incomplete work items (Pending or InProgress).
    pub fn get_incomplete_work(&self) -> Vec<&WorkItem> {
        self.work_items
            .iter()
            .filter(|item| {
                matches!(
                    item.data.status,
                    WorkItemStatus::Pending | WorkItemStatus::InProgress
                )
            })
            .map(|item| &item.data)
            .collect()
    }

    /// Get all decisions sorted by timestamp.
    pub fn get_sorted_decisions(&self) -> Vec<&Decision> {
        let mut decisions: Vec<&Decision> = self.decisions.iter().map(|i| &i.data).collect();
        decisions.sort_by_key(|d| d.made_at);
        decisions
    }
}

/// State reconstruction options.
#[derive(Debug, Clone)]
pub struct RebuildOptions {
    /// Maximum age of state to consider (in seconds). None = no limit.
    pub max_age_seconds: Option<u64>,
    /// Minimum confidence threshold.
    pub min_confidence: f32,
    /// Include session history.
    pub include_history: bool,
    /// Include memory layers.
    pub include_memory: Vec<MemoryLayer>,
    /// Include handoff data.
    pub include_handoff: bool,
    /// Include compressed snapshots.
    pub include_snapshots: bool,
}

impl Default for RebuildOptions {
    fn default() -> Self {
        Self {
            max_age_seconds: Some(7 * 24 * 60 * 60), // 7 days
            min_confidence: 0.5,
            include_history: false,
            include_memory: vec![MemoryLayer::L0, MemoryLayer::L1, MemoryLayer::L2],
            include_handoff: true,
            include_snapshots: true,
        }
    }
}

/// State rebuild engine.
pub struct StateRebuilder {
    #[allow(dead_code)] // Derived field; cc_dir is the active path
    workspace: PathBuf,
    cc_dir: PathBuf,
}

impl StateRebuilder {
    /// Create a new state rebuild engine.
    pub fn new(workspace: impl Into<PathBuf>) -> Self {
        let workspace = workspace.into();
        let cc_dir = workspace.join(".cowd");
        Self { workspace, cc_dir }
    }

    /// Rebuild state from all available sources.
    pub async fn rebuild(&self, options: &RebuildOptions) -> RebuiltSessionState {
        let mut state = RebuiltSessionState::new();

        // Rebuild from session history
        if options.include_history && legacy_jsonl_session_import_enabled() {
            if let Some(history_state) = self.rebuild_from_history().await {
                state.session_id = history_state.session_id;
                for (item, _ts) in history_state.work_items {
                    state.add_work_item(item, StateSource::SessionHistory, 0.8);
                }
            }
        }

        // Rebuild from handoff
        if options.include_handoff {
            if let Some(handoff_state) = self.rebuild_from_handoff().await {
                if state.session_id.is_none() {
                    state.session_id = Some(handoff_state.session_id.clone());
                }
                for item in handoff_state.work_items {
                    state.add_work_item(item, StateSource::Handoff, 0.9);
                }
                for decision in handoff_state.decisions {
                    state.add_decision(decision, StateSource::Handoff, 0.9);
                }
                if !handoff_state.summary.is_empty() {
                    state.context_summary = Some(StateItem::new(
                        handoff_state.summary,
                        StateSource::Handoff,
                        0.85,
                    ));
                }
            }
        }

        // Rebuild from compressed snapshots
        if options.include_snapshots {
            let snapshots = self.rebuild_from_snapshots().await;
            for (snapshot, _ts) in snapshots {
                state.pending_tasks.push(StateItem::new(
                    snapshot,
                    StateSource::CompressedSnapshot,
                    0.6,
                ));
            }
        }

        // Rebuild from session store
        let session_state = self.rebuild_from_session_store().await;
        if let Some(session) = session_state {
            if state.session_id.is_none() {
                state.session_id = Some(session.id.clone());
            }
            if let Some(summary) = session.context_summary {
                state.context_summary = Some(
                    StateItem::new(summary, StateSource::SessionStore, 0.82)
                        .with_modified(session.updated_at),
                );
            }
            for task in session.pending_tasks {
                state
                    .pending_tasks
                    .push(StateItem::new(task, StateSource::SessionStore, 0.75));
            }
            for decision in session.decisions {
                state.add_decision(decision, StateSource::SessionStore, 0.78);
            }
        }

        // Compute overall confidence
        state.compute_confidence();

        state
    }

    /// Rebuild from session history files.
    async fn rebuild_from_history(&self) -> Option<HistoryRebuildState> {
        let history_dir = self.cc_dir.join("history");
        if !history_dir.exists() {
            return None;
        }

        let mut latest_session_id = None;
        let mut latest_mtime = std::time::SystemTime::UNIX_EPOCH;
        let mut all_work_items = Vec::new();

        // Find the most recent session
        if let Ok(entries) = fs::read_dir(&history_dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                if path.extension().map(|e| e == "jsonl").unwrap_or(false) {
                    if let Ok(metadata) = entry.metadata() {
                        if let Ok(modified) = metadata.modified() {
                            if modified > latest_mtime {
                                latest_mtime = modified;
                                latest_session_id =
                                    path.file_stem().and_then(|s| s.to_str()).map(String::from);
                            }
                        }
                    }
                }
            }
        }

        // If we have a session ID, parse it for work items
        if let Some(ref session_id) = latest_session_id {
            let session_file = history_dir.join(format!("{}.jsonl", session_id));
            if let Ok(content) = fs::read_to_string(&session_file) {
                all_work_items = self.extract_work_items_from_history(&content);
            }
        }

        Some(HistoryRebuildState {
            session_id: latest_session_id,
            work_items: all_work_items,
        })
    }

    /// Extract work items from session history content.
    fn extract_work_items_from_history(&self, content: &str) -> Vec<(WorkItem, Option<i64>)> {
        let mut items = Vec::new();

        for line in content.lines() {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
                // Look for work item mentions in the content
                if let Some(text) = json.get("content").and_then(|c| c.as_str()) {
                    // Simple pattern matching for task items
                    if text.contains("[ ]") || text.contains("- [ ]") {
                        if let Some(title) = self.extract_task_title(text) {
                            items.push((
                                WorkItem {
                                    id: format!("rebuilt-{}", items.len()),
                                    title,
                                    description: text.to_string(),
                                    status: WorkItemStatus::Pending,
                                    priority: crate::types::Priority::Normal,
                                },
                                json.get("timestamp")
                                    .and_then(|t| t.as_i64())
                                    .or_else(|| json.get("ts").and_then(|t| t.as_i64())),
                            ));
                        }
                    }
                }
            }
        }

        items
    }

    /// Extract task title from text.
    fn extract_task_title(&self, text: &str) -> Option<String> {
        // Look for markdown task format
        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("- [ ]") || trimmed.starts_with("[ ]") {
                let title = trimmed
                    .trim_start_matches("- [ ]")
                    .trim_start_matches("[ ]")
                    .trim();
                if !title.is_empty() {
                    return Some(title.chars().take(100).collect());
                }
            }
        }
        None
    }

    /// Rebuild from handoff data.
    async fn rebuild_from_handoff(&self) -> Option<HandoffData> {
        let handoff_dir = self.cc_dir.join("handoffs");
        if !handoff_dir.exists() {
            return None;
        }

        // Find the most recent handoff
        let mut latest_path: Option<PathBuf> = None;
        let mut latest_mtime = std::time::SystemTime::UNIX_EPOCH;

        if let Ok(entries) = fs::read_dir(&handoff_dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                if path.extension().map(|e| e == "json").unwrap_or(false)
                    && !path.to_string_lossy().contains(".resumed.")
                    && !path.to_string_lossy().contains(".decisions.")
                {
                    if let Ok(metadata) = entry.metadata() {
                        if let Ok(modified) = metadata.modified() {
                            if modified > latest_mtime {
                                latest_mtime = modified;
                                latest_path = Some(path);
                            }
                        }
                    }
                }
            }
        }

        // Load and return the handoff
        if let Some(path) = latest_path {
            if let Ok(content) = fs::read_to_string(&path) {
                if let Ok(data) = serde_json::from_str::<HandoffData>(&content) {
                    return Some(data);
                }
            }
        }

        None
    }

    /// Rebuild from compressed snapshots.
    async fn rebuild_from_snapshots(&self) -> Vec<(String, Option<i64>)> {
        let snapshot_dir = self.cc_dir.join("snapshots");
        let mut snapshots = Vec::new();

        if !snapshot_dir.exists() {
            return snapshots;
        }

        // Find all snapshot files
        let mut paths: Vec<(PathBuf, std::io::Result<std::time::SystemTime>)> = Vec::new();
        if let Ok(entries) = fs::read_dir(&snapshot_dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                if path.extension().map(|e| e == "json").unwrap_or(false)
                    || path.extension().map(|e| e == "gz").unwrap_or(false)
                {
                    if let Ok(metadata) = entry.metadata() {
                        paths.push((path, metadata.modified()));
                    }
                }
            }
        }

        // Sort by modification time (newest first)
        paths.sort_by(|a, b| {
            let a_time =
                a.1.as_ref()
                    .map(|t| *t)
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            let b_time =
                b.1.as_ref()
                    .map(|t| *t)
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            b_time.cmp(&a_time)
        });

        // Take the most recent snapshot
        if let Some((path, _)) = paths.first() {
            if let Ok(content) = fs::read_to_string(path) {
                // Extract context from snapshot
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) {
                    if let Some(summary) = json.get("summary").and_then(|s| s.as_str()) {
                        snapshots.push((summary.to_string(), None));
                    } else if let Some(messages) = json.get("messages").and_then(|m| m.as_array()) {
                        // Build summary from messages
                        let context: Vec<String> = messages
                            .iter()
                            .filter_map(|m| m.get("content").and_then(|c| c.as_str()))
                            .rev()
                            .take(5)
                            .map(|s| s.chars().take(200).collect())
                            .collect();
                        if !context.is_empty() {
                            snapshots.push((context.join("\n---\n"), None));
                        }
                    }
                }
            }
        }

        snapshots
    }

    /// Rebuild from session store.
    async fn rebuild_from_session_store(&self) -> Option<SessionStoreEntry> {
        let store_path = self.cc_dir.join("sessions.db");
        if !store_path.exists() {
            return None;
        }

        let conn = rusqlite::Connection::open_with_flags(
            &store_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .ok()?;
        let latest = latest_session_entry(&conn)?;
        let messages = recent_session_messages(&conn, &latest.id, 16);
        let snapshot_summary = latest_snapshot_summary(&conn, &latest.id);
        let message_summary = summarize_session_messages(&messages);
        let context_summary = snapshot_summary.or(message_summary);
        let pending_tasks = messages
            .iter()
            .flat_map(|message| extract_pending_tasks_from_text(&message.text))
            .collect::<Vec<_>>();
        let decisions = messages
            .iter()
            .flat_map(|message| extract_decisions_from_text(&message.text))
            .collect::<Vec<_>>();

        Some(SessionStoreEntry {
            context_summary,
            pending_tasks,
            decisions,
            ..latest
        })
    }

    /// Quick rebuild - get essential state only.
    pub async fn quick_rebuild(&self) -> RebuiltSessionState {
        self.rebuild(&RebuildOptions {
            max_age_seconds: Some(24 * 60 * 60), // 24 hours
            min_confidence: 0.7,
            include_history: false,
            include_memory: vec![MemoryLayer::L0, MemoryLayer::L1],
            include_handoff: true,
            include_snapshots: false,
        })
        .await
    }

    /// Export rebuilt state to a file.
    pub fn export_state(&self, state: &RebuiltSessionState, path: &Path) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(state)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        fs::write(path, json)
    }

    /// Import state from a file.
    pub fn import_state(&self, path: &Path) -> std::io::Result<RebuiltSessionState> {
        let content = fs::read_to_string(path)?;
        serde_json::from_str(&content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

// Internal types

struct HistoryRebuildState {
    session_id: Option<String>,
    work_items: Vec<(WorkItem, Option<i64>)>,
}

#[derive(Debug)]
struct SessionStoreEntry {
    id: String,
    updated_at: i64,
    context_summary: Option<String>,
    pending_tasks: Vec<String>,
    decisions: Vec<Decision>,
}

#[derive(Debug)]
struct SessionStoreMessage {
    role: String,
    text: String,
    created_at_ms: i64,
}

fn latest_session_entry(conn: &rusqlite::Connection) -> Option<SessionStoreEntry> {
    let modern = conn
        .query_row(
            "SELECT session_id, created_at_ms, updated_at_ms FROM sessions ORDER BY updated_at_ms DESC, created_at_ms DESC LIMIT 1",
            [],
            |row| {
                Ok(SessionStoreEntry {
                    id: row.get(0)?,
                    updated_at: row.get::<_, i64>(2)?,
                    context_summary: None,
                    pending_tasks: Vec::new(),
                    decisions: Vec::new(),
                })
            },
        )
        .optional()
        .ok()
        .flatten();
    if modern.is_some() {
        return modern;
    }
    conn.query_row(
        "SELECT session_id, 0, 0 FROM sessions ORDER BY last_activity DESC, created_at DESC LIMIT 1",
        [],
        |row| {
            Ok(SessionStoreEntry {
                id: row.get(0)?,
                updated_at: row.get::<_, i64>(2)?,
                context_summary: None,
                pending_tasks: Vec::new(),
                decisions: Vec::new(),
            })
        },
    )
    .optional()
    .ok()
    .flatten()
}

fn recent_session_messages(
    conn: &rusqlite::Connection,
    session_id: &str,
    limit: usize,
) -> Vec<SessionStoreMessage> {
    let Ok(mut stmt) = conn.prepare(
        "SELECT role, content_json, created_at_ms FROM messages WHERE session_id = ?1 ORDER BY sequence DESC LIMIT ?2",
    ) else {
        return Vec::new();
    };
    let Ok(rows) = stmt.query_map(rusqlite::params![session_id, limit as i64], |row| {
        let content_json: String = row.get(1)?;
        Ok(SessionStoreMessage {
            role: row.get(0)?,
            text: session_content_json_to_text(&content_json),
            created_at_ms: row.get::<_, i64>(2).unwrap_or_default(),
        })
    }) else {
        return Vec::new();
    };
    let mut messages = rows.filter_map(Result::ok).collect::<Vec<_>>();
    messages.reverse();
    messages
}

fn latest_snapshot_summary(conn: &rusqlite::Connection, session_id: &str) -> Option<String> {
    let payload = conn
        .query_row(
            "SELECT messages_json FROM session_snapshots WHERE session_id = ?1 ORDER BY event_idx DESC LIMIT 1",
            rusqlite::params![session_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .ok()
        .flatten()?;
    let value = serde_json::from_str::<serde_json::Value>(&payload).ok()?;
    let messages = value.as_array()?;
    let summary = messages
        .iter()
        .rev()
        .take(6)
        .filter_map(|message| {
            message
                .get("content")
                .and_then(serde_json::Value::as_str)
                .or_else(|| message.get("text").and_then(serde_json::Value::as_str))
        })
        .map(|text| text.chars().take(240).collect::<String>())
        .collect::<Vec<_>>();
    (!summary.is_empty()).then(|| summary.join("\n---\n"))
}

fn summarize_session_messages(messages: &[SessionStoreMessage]) -> Option<String> {
    let parts = messages
        .iter()
        .rev()
        .filter(|message| !message.text.trim().is_empty())
        .take(8)
        .map(|message| {
            format!(
                "{}@{}: {}",
                message.role,
                message.created_at_ms,
                message.text.chars().take(220).collect::<String>()
            )
        })
        .collect::<Vec<_>>();
    (!parts.is_empty()).then(|| parts.join("\n---\n"))
}

fn session_content_json_to_text(content_json: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(content_json) else {
        return content_json.to_string();
    };
    if let Some(text) = value.as_str() {
        return text.to_string();
    }
    if let Some(array) = value.as_array() {
        return array
            .iter()
            .filter_map(|block| {
                block
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .or_else(|| block.get("content").and_then(serde_json::Value::as_str))
                    .or_else(|| block.as_str())
            })
            .collect::<Vec<_>>()
            .join("\n");
    }
    value
        .get("text")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn extract_pending_tasks_from_text(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with("- [ ]") || trimmed.starts_with("[ ]") {
                let title = trimmed
                    .trim_start_matches("- [ ]")
                    .trim_start_matches("[ ]")
                    .trim();
                (!title.is_empty()).then(|| title.chars().take(160).collect())
            } else {
                None
            }
        })
        .collect()
}

fn extract_decisions_from_text(text: &str) -> Vec<Decision> {
    text.lines()
        .filter(|line| {
            let lower = line.to_lowercase();
            lower.contains("decision:")
                || lower.contains("decided")
                || lower.contains("决定")
                || lower.contains("采用")
        })
        .enumerate()
        .map(|(index, line)| Decision {
            id: format!("session-store-decision-{index}"),
            summary: line.trim().chars().take(160).collect(),
            rationale: "Recovered from session store message text".to_string(),
            status: DecisionStatus::Deferred,
            made_at: chrono::Utc::now(),
        })
        .collect()
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DecisionStatus, Priority, WorkItemStatus};
    use std::io::Write;
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

    #[tokio::test]
    async fn test_state_rebuilder_creation() {
        let tmp = TempDir::new().unwrap();
        let rebuilder = StateRebuilder::new(tmp.path());
        assert!(rebuilder.cc_dir.ends_with(".cowd"));
    }

    #[tokio::test]
    async fn test_rebuilt_state_confidence() {
        let mut state = RebuiltSessionState::new();

        state.add_work_item(
            WorkItem {
                id: "1".into(),
                title: "Test task".into(),
                description: "Test".into(),
                status: WorkItemStatus::Pending,
                priority: Priority::Normal,
            },
            StateSource::SessionHistory,
            0.8,
        );

        state.add_decision(
            Decision {
                id: "d1".into(),
                summary: "Test decision".into(),
                rationale: "Because".into(),
                status: DecisionStatus::Deferred,
                made_at: chrono::Utc::now(),
            },
            StateSource::Handoff,
            0.9,
        );

        state.compute_confidence();

        // Average of 0.8 and 0.9
        assert!((state.overall_confidence - 0.85).abs() < 0.01);
    }

    #[test]
    fn test_rebuild_options_do_not_include_legacy_history_by_default() {
        let options = RebuildOptions::default();
        assert!(!options.include_history);
    }

    #[tokio::test]
    async fn test_rebuild_skips_legacy_history_without_import_gate() {
        let _env = LegacyJsonlEnvGuard::disabled();
        let tmp = TempDir::new().unwrap();
        let history_dir = tmp.path().join(".cowd/history");
        fs::create_dir_all(&history_dir).unwrap();

        let mut file = fs::File::create(history_dir.join("legacy-session.jsonl")).unwrap();
        writeln!(
            file,
            r#"{{"content":"- [ ] imported legacy task","timestamp":1}}"#
        )
        .unwrap();

        let rebuilder = StateRebuilder::new(tmp.path());
        let state = rebuilder
            .rebuild(&RebuildOptions {
                include_history: true,
                include_handoff: false,
                include_snapshots: false,
                include_memory: Vec::new(),
                ..RebuildOptions::default()
            })
            .await;

        assert!(state
            .work_items
            .iter()
            .all(|item| item.source != StateSource::SessionHistory));
    }

    #[tokio::test]
    async fn test_rebuild_uses_session_store_context_tasks_and_decisions() {
        let tmp = TempDir::new().unwrap();
        let cowd_dir = tmp.path().join(".cowd");
        fs::create_dir_all(&cowd_dir).unwrap();
        let db_path = cowd_dir.join("sessions.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            CREATE TABLE messages (
                session_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                role TEXT NOT NULL,
                content_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            );
            CREATE TABLE session_snapshots (
                session_id TEXT NOT NULL,
                event_idx INTEGER NOT NULL,
                messages_json TEXT NOT NULL
            );
            INSERT INTO sessions VALUES ('session-a', 10, 20);
            INSERT INTO messages VALUES (
                'session-a',
                1,
                'user',
                '"- [ ] finish recovery\nDecision: use session store as recovery source"',
                11
            );
            INSERT INTO session_snapshots VALUES (
                'session-a',
                1,
                '[{"content":"snapshot summary"}]'
            );
            "#,
        )
        .unwrap();
        drop(conn);

        let rebuilder = StateRebuilder::new(tmp.path());
        let state = rebuilder
            .rebuild(&RebuildOptions {
                include_history: false,
                include_handoff: false,
                include_snapshots: false,
                include_memory: Vec::new(),
                ..RebuildOptions::default()
            })
            .await;

        assert_eq!(state.session_id.as_deref(), Some("session-a"));
        assert_eq!(
            state
                .context_summary
                .as_ref()
                .map(|summary| summary.data.as_str()),
            Some("snapshot summary")
        );
        assert!(state
            .pending_tasks
            .iter()
            .any(|item| item.source == StateSource::SessionStore
                && item.data.contains("finish recovery")));
        assert!(state
            .decisions
            .iter()
            .any(|item| item.source == StateSource::SessionStore
                && item.data.summary.contains("session store")));
    }

    #[tokio::test]
    async fn test_incomplete_work_items() {
        let mut state = RebuiltSessionState::new();

        state.add_work_item(
            WorkItem {
                id: "1".into(),
                title: "Done".into(),
                description: "Done".into(),
                status: WorkItemStatus::Done,
                priority: Priority::Normal,
            },
            StateSource::SessionHistory,
            0.8,
        );

        state.add_work_item(
            WorkItem {
                id: "2".into(),
                title: "Pending".into(),
                description: "Pending".into(),
                status: WorkItemStatus::Pending,
                priority: Priority::Normal,
            },
            StateSource::SessionHistory,
            0.8,
        );

        let incomplete = state.get_incomplete_work();
        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].id, "2");
    }

    #[tokio::test]
    async fn test_export_import_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let rebuilder = StateRebuilder::new(tmp.path());

        let mut state = RebuiltSessionState::new();
        state.session_id = Some("test-session".into());
        state.overall_confidence = 0.85;

        let export_path = tmp.path().join("state.json");
        rebuilder.export_state(&state, &export_path).unwrap();

        let imported = rebuilder.import_state(&export_path).unwrap();
        assert_eq!(imported.session_id, Some("test-session".into()));
        assert!((imported.overall_confidence - 0.85).abs() < 0.01);
    }
}

// ─── GSD-Style State Rebuilding ─────────────────────────────────────────────────
//
// GSD (Get-Shit-Done) style state rebuilding focuses on:
// 1. Minimal dependencies on compression
// 2. Direct extraction of essential state
// 3. Prioritized reconstruction for quick recovery

use crate::aaak_compression::{AaakCompressed, AaakCompressor, GsdContext, GsdState};

/// GSD-style state rebuild options - minimal compression dependency.
#[derive(Debug, Clone)]
pub struct GsdRebuildOptions {
    /// Maximum age of state to consider (in seconds)
    pub max_age_seconds: Option<u64>,
    /// Include session history
    pub include_history: bool,
    /// Include handoff data
    pub include_handoff: bool,
    /// Include AAAK compressed snapshots
    pub include_aaak: bool,
    /// Extract priority items
    pub extract_priority: bool,
    /// Build entity abbreviations for context
    pub build_abbreviations: bool,
}

impl Default for GsdRebuildOptions {
    fn default() -> Self {
        Self {
            max_age_seconds: Some(24 * 60 * 60), // 24 hours
            include_history: false,
            include_handoff: true,
            include_aaak: true,
            extract_priority: true,
            build_abbreviations: true,
        }
    }
}

/// GSD-style state rebuild result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GsdRebuiltState {
    /// Session context
    pub context: GsdContext,
    /// Work items sorted by priority
    pub prioritized_work: Vec<WorkItem>,
    /// Key decisions for context
    pub key_decisions: Vec<Decision>,
    /// Blockers that need resolution
    pub blockers: Vec<BlockerInfo>,
    /// Next actionable step
    pub next_step: String,
    /// Files referenced
    pub referenced_files: Vec<String>,
    /// Abbreviated entities for context
    pub abbreviations: Vec<AbbreviationEntry>,
    /// Rebuild confidence
    pub confidence: f32,
    /// Sources used in rebuild
    pub sources_used: Vec<StateSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockerInfo {
    pub description: String,
    pub severity: BlockerSeverity,
    pub hint: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum BlockerSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbbreviationEntry {
    pub short: String,
    pub full: String,
    pub context: String,
}

/// History extraction result for GSD rebuilding.
struct HistoryExtraction {
    task_summary: String,
    files: Vec<String>,
}

/// GSD State Rebuilder - optimized for minimal compression dependency.
pub struct GsdStateRebuilder {
    #[allow(dead_code)] // Derived field; cc_dir is the active path
    workspace: PathBuf,
    cc_dir: PathBuf,
    #[allow(dead_code)] // Design: pre-allocated compressor for future use
    compressor: AaakCompressor,
}

impl GsdStateRebuilder {
    /// Create a new GSD rebuilder.
    pub fn new(workspace: impl Into<PathBuf>) -> Self {
        let workspace = workspace.into();
        let cc_dir = workspace.join(".cowd");
        Self {
            workspace,
            cc_dir,
            compressor: AaakCompressor::default_compressor(),
        }
    }

    /// Quick rebuild - prioritize speed over completeness.
    pub async fn quick_rebuild(&self, options: &GsdRebuildOptions) -> GsdRebuiltState {
        let mut state = GsdRebuiltState {
            context: self.create_default_context(),
            prioritized_work: Vec::new(),
            key_decisions: Vec::new(),
            blockers: Vec::new(),
            next_step: String::new(),
            referenced_files: Vec::new(),
            abbreviations: Vec::new(),
            confidence: 0.0,
            sources_used: Vec::new(),
        };

        // Extract from handoff first (highest priority)
        if options.include_handoff {
            if let Some(handoff) = self.extract_from_handoff().await {
                state.context.task = handoff.summary.clone();
                state.key_decisions = handoff.decisions;
                state.blockers = handoff
                    .blockers
                    .into_iter()
                    .map(|b| BlockerInfo {
                        description: b.description,
                        severity: BlockerSeverity::Medium,
                        hint: b.resolution_hint,
                    })
                    .collect();
                state.prioritized_work = self.prioritize_work_items(handoff.work_items);
                state.confidence += 0.4;
                state.sources_used.push(StateSource::Handoff);
            }
        }

        // Extract from AAAK compressed snapshots
        if options.include_aaak {
            if let Some(compressed) = self.extract_from_aaak().await {
                let _decompressed = AaakCompressor::decompress(&compressed);
                state.abbreviations = self.extract_abbreviations(&compressed);
                state.context.abbreviations = compressed.dictionary;
                state.confidence += 0.3;
                state.sources_used.push(StateSource::CompressedSnapshot);
            }
        }

        // Extract from session history
        if options.include_history && legacy_jsonl_session_import_enabled() {
            if let Some(history) = self.extract_from_history(options.max_age_seconds).await {
                if state.context.task.is_empty() {
                    state.context.task = history.task_summary;
                }
                state.referenced_files = history.files;
                state.confidence += 0.2;
                state.sources_used.push(StateSource::SessionHistory);
            }
        }

        // Calculate next step
        state.next_step = self.calculate_next_step(&state);

        // Normalize confidence
        state.confidence = (state.confidence * 100.0).round() / 100.0;

        state
    }

    /// Full rebuild - comprehensive but slower.
    pub async fn full_rebuild(&self) -> GsdRebuiltState {
        self.quick_rebuild(&GsdRebuildOptions::default()).await
    }

    fn create_default_context(&self) -> GsdContext {
        GsdContext {
            session_id: uuid::Uuid::new_v4().to_string(),
            task: String::new(),
            state: GsdState::Planning,
            decisions: Vec::new(),
            blockers: Vec::new(),
            next_action: String::new(),
            files: Vec::new(),
            abbreviations: crate::aaak_compression::AaakDictionary::new(),
            priority_items: Vec::new(),
        }
    }

    async fn extract_from_handoff(&self) -> Option<HandoffData> {
        let handoff_dir = self.cc_dir.join("handoffs");
        if !handoff_dir.exists() {
            return None;
        }

        // Find most recent handoff
        let mut latest_path: Option<PathBuf> = None;
        let mut latest_mtime = std::time::SystemTime::UNIX_EPOCH;

        if let Ok(entries) = fs::read_dir(&handoff_dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                if path.extension().map(|e| e == "json").unwrap_or(false)
                    && !path.to_string_lossy().contains(".resumed.")
                    && !path.to_string_lossy().contains(".decisions.")
                {
                    if let Ok(metadata) = entry.metadata() {
                        if let Ok(modified) = metadata.modified() {
                            if modified > latest_mtime {
                                latest_mtime = modified;
                                latest_path = Some(path);
                            }
                        }
                    }
                }
            }
        }

        if let Some(path) = latest_path {
            if let Ok(content) = fs::read_to_string(&path) {
                return serde_json::from_str(&content).ok();
            }
        }

        None
    }

    async fn extract_from_aaak(&self) -> Option<AaakCompressed> {
        let snapshot_dir = self.cc_dir.join("snapshots");
        if !snapshot_dir.exists() {
            return None;
        }

        // Find .aaak files
        let mut paths: Vec<(PathBuf, std::io::Result<std::time::SystemTime>)> = Vec::new();

        if let Ok(entries) = fs::read_dir(&snapshot_dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                if path.extension().map(|e| e == "aaak").unwrap_or(false) {
                    if let Ok(metadata) = entry.metadata() {
                        paths.push((path, metadata.modified()));
                    }
                }
            }
        }

        // Sort by mtime (newest first)
        paths.sort_by(|a, b| {
            let a_time =
                a.1.as_ref()
                    .map(|t| *t)
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            let b_time =
                b.1.as_ref()
                    .map(|t| *t)
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            b_time.cmp(&a_time)
        });

        if let Some((path, _)) = paths.first() {
            if let Ok(data) = fs::read(path) {
                return serde_json::from_slice(&data).ok();
            }
        }

        None
    }

    async fn extract_from_history(&self, _max_age: Option<u64>) -> Option<HistoryExtraction> {
        let history_dir = self.cc_dir.join("history");
        if !history_dir.exists() {
            return None;
        }

        // Find recent session files
        let mut latest_path: Option<PathBuf> = None;
        let mut latest_mtime = std::time::SystemTime::UNIX_EPOCH;

        if let Ok(entries) = fs::read_dir(&history_dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                if path.extension().map(|e| e == "jsonl").unwrap_or(false) {
                    if let Ok(metadata) = entry.metadata() {
                        if let Ok(modified) = metadata.modified() {
                            if modified > latest_mtime {
                                latest_mtime = modified;
                                latest_path = Some(path);
                            }
                        }
                    }
                }
            }
        }

        if let Some(path) = latest_path {
            if let Ok(content) = fs::read_to_string(&path) {
                let mut files: Vec<String> = Vec::new();
                let mut last_user_msg = String::new();

                for line in content.lines().rev().take(100) {
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
                        // Extract file paths
                        if let Some(content) = json.get("content").and_then(|c| c.as_str()) {
                            // Find file paths
                            for word in content.split_whitespace() {
                                if word.contains('/')
                                    && (word.ends_with(".rs")
                                        || word.ends_with(".json")
                                        || word.ends_with(".yaml")
                                        || word.ends_with(".md")
                                        || word.ends_with(".toml"))
                                    && !files.contains(&word.to_string())
                                {
                                    files.push(word.to_string());
                                }
                            }

                            // Get last user message as task summary
                            if json.get("role").and_then(|r| r.as_str()) == Some("user") {
                                last_user_msg = content.chars().take(200).collect();
                            }
                        }
                    }
                }

                return Some(HistoryExtraction {
                    task_summary: last_user_msg,
                    files,
                });
            }
        }

        None
    }

    fn prioritize_work_items(&self, items: Vec<WorkItem>) -> Vec<WorkItem> {
        let mut sorted = items;
        sorted.sort_by(|a, b| {
            // Sort by status priority: InProgress > Pending > Blocked > Done
            let status_order = |w: &WorkItem| match w.status {
                WorkItemStatus::InProgress => 0,
                WorkItemStatus::Pending => 1,
                WorkItemStatus::Blocked => 2,
                WorkItemStatus::Done => 3,
            };
            let priority_order = |w: &WorkItem| match w.priority {
                crate::types::Priority::Critical => 0,
                crate::types::Priority::High => 1,
                crate::types::Priority::Normal => 2,
                crate::types::Priority::Low => 3,
            };

            status_order(a)
                .cmp(&status_order(b))
                .then(priority_order(a).cmp(&priority_order(b)))
        });
        sorted
    }

    fn extract_abbreviations(&self, compressed: &AaakCompressed) -> Vec<AbbreviationEntry> {
        compressed
            .dictionary
            .abbreviations
            .values()
            .map(|abbrev| AbbreviationEntry {
                short: abbrev.short.clone(),
                full: abbrev.full.clone(),
                context: format!("{:?}", abbrev.entity_type),
            })
            .collect()
    }

    fn calculate_next_step(&self, state: &GsdRebuiltState) -> String {
        // Priority 1: Blockers
        if !state.blockers.is_empty() {
            return format!(
                "Resolve blocker: {}",
                state
                    .blockers
                    .first()
                    .map(|b| b.description.as_str())
                    .unwrap_or("Unknown")
            );
        }

        // Priority 2: In-progress items
        if let Some(work) = state
            .prioritized_work
            .iter()
            .find(|w| w.status == WorkItemStatus::InProgress)
        {
            return format!("Continue: {}", work.title);
        }

        // Priority 3: Next pending item
        if let Some(work) = state
            .prioritized_work
            .iter()
            .find(|w| w.status == WorkItemStatus::Pending)
        {
            return format!("Start: {}", work.title);
        }

        // Default
        "Review and wrap up".to_string()
    }

    /// Export GSD state to file.
    pub fn export_gsd_state(&self, state: &GsdRebuiltState, path: &Path) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(state)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        fs::write(path, json)
    }

    /// Import GSD state from file.
    pub fn import_gsd_state(&self, path: &Path) -> std::io::Result<GsdRebuiltState> {
        let content = fs::read_to_string(path)?;
        serde_json::from_str(&content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

// ─── GSD Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod gsd_tests {
    use super::*;

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

    #[tokio::test]
    async fn test_gsd_rebuilder_creation() {
        let tmp = TempDir::new().unwrap();
        let rebuilder = GsdStateRebuilder::new(tmp.path());
        assert!(rebuilder.cc_dir.ends_with(".cowd"));
    }

    #[tokio::test]
    async fn test_quick_rebuild() {
        let tmp = TempDir::new().unwrap();
        let rebuilder = GsdStateRebuilder::new(tmp.path());
        let options = GsdRebuildOptions::default();
        let state = rebuilder.quick_rebuild(&options).await;
        assert!(state.confidence >= 0.0 && state.confidence <= 1.0);
    }

    #[test]
    fn test_gsd_rebuild_options_do_not_include_legacy_history_by_default() {
        let options = GsdRebuildOptions::default();
        assert!(!options.include_history);
    }

    #[tokio::test]
    async fn test_gsd_rebuild_skips_legacy_history_without_import_gate() {
        let _env = LegacyJsonlEnvGuard::disabled();
        let tmp = TempDir::new().unwrap();
        let history_dir = tmp.path().join(".cowd/history");
        fs::create_dir_all(&history_dir).unwrap();
        fs::write(
            history_dir.join("legacy-session.jsonl"),
            r#"{"role":"user","content":"Please inspect crates/memory/src/state_rebuilder.rs"}"#,
        )
        .unwrap();

        let rebuilder = GsdStateRebuilder::new(tmp.path());
        let state = rebuilder
            .quick_rebuild(&GsdRebuildOptions {
                include_history: true,
                include_handoff: false,
                include_aaak: false,
                ..GsdRebuildOptions::default()
            })
            .await;

        assert!(!state.sources_used.contains(&StateSource::SessionHistory));
        assert!(state.referenced_files.is_empty());
    }

    #[tokio::test]
    async fn test_prioritize_work_items() {
        let tmp = TempDir::new().unwrap();
        let rebuilder = GsdStateRebuilder::new(tmp.path());

        let items = vec![
            WorkItem {
                id: "1".into(),
                title: "Done task".into(),
                description: "".into(),
                status: WorkItemStatus::Done,
                priority: crate::types::Priority::Normal,
            },
            WorkItem {
                id: "2".into(),
                title: "In progress".into(),
                description: "".into(),
                status: WorkItemStatus::InProgress,
                priority: crate::types::Priority::Normal,
            },
            WorkItem {
                id: "3".into(),
                title: "Pending".into(),
                description: "".into(),
                status: WorkItemStatus::Pending,
                priority: crate::types::Priority::Normal,
            },
        ];

        let prioritized = rebuilder.prioritize_work_items(items);
        assert_eq!(prioritized[0].id, "2"); // InProgress first
        assert_eq!(prioritized[1].id, "3"); // Pending second
        assert_eq!(prioritized[2].id, "1"); // Done last
    }

    #[tokio::test]
    async fn test_gsd_state_export_import() {
        let tmp = TempDir::new().unwrap();
        let rebuilder = GsdStateRebuilder::new(tmp.path());

        let state = GsdRebuiltState {
            context: rebuilder.create_default_context(),
            prioritized_work: Vec::new(),
            key_decisions: Vec::new(),
            blockers: Vec::new(),
            next_step: "Test next step".to_string(),
            referenced_files: vec!["src/lib.rs".to_string()],
            abbreviations: Vec::new(),
            confidence: 0.85,
            sources_used: vec![StateSource::Handoff],
        };

        let path = tmp.path().join("gsd-state.json");
        rebuilder.export_gsd_state(&state, &path).unwrap();

        let imported = rebuilder.import_gsd_state(&path).unwrap();
        assert_eq!(imported.next_step, "Test next step");
        assert!((imported.confidence - 0.85).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_calculate_next_step() {
        let tmp = TempDir::new().unwrap();
        let rebuilder = GsdStateRebuilder::new(tmp.path());

        // Test with blocker
        let state_with_blocker = GsdRebuiltState {
            context: rebuilder.create_default_context(),
            prioritized_work: Vec::new(),
            key_decisions: Vec::new(),
            blockers: vec![BlockerInfo {
                description: "Need API key".to_string(),
                severity: BlockerSeverity::High,
                hint: Some("Add to environment".to_string()),
            }],
            next_step: String::new(),
            referenced_files: Vec::new(),
            abbreviations: Vec::new(),
            confidence: 0.5,
            sources_used: Vec::new(),
        };

        let next = rebuilder.calculate_next_step(&state_with_blocker);
        assert!(next.contains("Resolve blocker"));
    }
}
