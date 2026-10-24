//! State rebuild mechanism for recovering from context loss.
//!
//! When context overflows or sessions crash, this module reconstructs state
//! from various sources: session history, memory layers, and handoff data.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(test)]
use tempfile::TempDir;

use serde::{Deserialize, Serialize};
use session::{SessionHistoryReader, SessionMessage, SessionRecord};

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
    session_history: Option<Arc<SessionHistoryReader>>,
}

impl StateRebuilder {
    /// Create a new state rebuild engine.
    pub fn new(workspace: impl Into<PathBuf>) -> Self {
        let workspace = workspace.into();
        let cc_dir = workspace.join(".cowd");
        Self {
            workspace,
            cc_dir,
            session_history: None,
        }
    }

    /// Attach the selected durable session owner. State rebuilding must not
    /// infer a SQLite filename or open a database behind composition's back.
    /// This standalone reader is trusted and unscoped, not a model grant.
    /// Runtime context must use its own current-Session or authorized history path.
    #[must_use]
    pub fn with_session_history(
        workspace: impl Into<PathBuf>,
        session_history: Arc<SessionHistoryReader>,
    ) -> Self {
        let workspace = workspace.into();
        Self {
            cc_dir: workspace.join(".cowd"),
            workspace,
            session_history: Some(session_history),
        }
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
        let store = self.session_history.as_ref()?;
        let record = store
            .list_recent_sessions(1)
            .await
            .ok()?
            .into_iter()
            .next()?;
        let latest = session_store_entry(&record);
        let message_count = store.message_count(&latest.id).await.ok()?;
        let recent_offset = message_count.saturating_sub(16);
        let messages = store.messages(&latest.id, recent_offset, 16).await.ok()?;
        let snapshot_summary = store
            .latest_snapshot(&latest.id)
            .await
            .ok()
            .flatten()
            .and_then(|snapshot| snapshot_summary_from_json(&snapshot.messages_json));
        let messages = messages
            .into_iter()
            .map(session_store_message)
            .collect::<Vec<_>>();
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

fn session_store_entry(record: &SessionRecord) -> SessionStoreEntry {
    let updated_at = chrono::DateTime::parse_from_rfc3339(&record.last_activity)
        .map(|value| value.timestamp())
        .unwrap_or_default();
    SessionStoreEntry {
        id: record.session_id.clone(),
        updated_at,
        context_summary: None,
        pending_tasks: Vec::new(),
        decisions: Vec::new(),
    }
}

fn session_store_message(message: SessionMessage) -> SessionStoreMessage {
    SessionStoreMessage {
        role: message.role,
        text: session_content_json_to_text(&message.content_json),
        created_at_ms: i64::try_from(message.created_at_ms).unwrap_or(i64::MAX),
    }
}

fn snapshot_summary_from_json(payload: &str) -> Option<String> {
    let value = serde_json::from_str::<serde_json::Value>(payload).ok()?;
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
