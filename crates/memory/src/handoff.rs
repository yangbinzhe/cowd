//! Cross-session handoff protocol.
//!
//! When the context window is exhausted, the handoff module serialises the
//! current working state into a `HandoffData` packet that the next session
//! can deserialise and resume from.
//!
//! Each handoff is persisted as two files inside `handoff_dir`:
//! * `{session_id}.json` – machine-readable JSON (canonical).
//! * `{session_id}.md`   – human-readable Markdown (informational).
//!
//! Writes are atomic: content is first written to a `.tmp` file and then
//! renamed into place, mirroring the `BlobStore` approach.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use chrono::Utc;
use uuid::Uuid;

use crate::{
    error::MemoryError,
    types::{Blocker, Decision, HandoffData, TaskState, WorkItem, WorkItemStatus},
};

/// Result alias for handoff operations.
pub type Result<T> = std::result::Result<T, MemoryError>;

/// Default directory for handoff files (relative to the working directory).
const DEFAULT_HANDOFF_DIR: &str = ".cowd/handoffs";

// ─── HandoffManager ───────────────────────────────────────────────────────────

/// Manages cross-session state transfer.
///
/// Each handoff is written as a pair of files:
/// * `{dir}/{session_id}.json` – canonical JSON
/// * `{dir}/{session_id}.md`   – human-readable Markdown
pub struct HandoffManager {
    /// Directory where handoff files are stored.
    handoff_dir: PathBuf,
}

impl HandoffManager {
    /// Create a manager using the default handoff directory (`.cowd/handoffs/`).
    #[must_use]
    pub fn new() -> Self {
        Self::with_dir(PathBuf::from(DEFAULT_HANDOFF_DIR))
    }

    /// Create a manager with an explicit directory path.
    #[must_use]
    pub fn with_dir(handoff_dir: PathBuf) -> Self {
        Self { handoff_dir }
    }

    // ─── Build ───────────────────────────────────────────────────────────────

    /// Assemble a [`HandoffData`] packet from the provided components.
    ///
    /// `session_id` – stable identifier for the *current* session.  If empty a
    /// new UUID is generated.
    pub fn create_handoff(
        &self,
        session_id: &str,
        current_task: Option<TaskState>,
        completed: Vec<WorkItem>,
        remaining: Vec<WorkItem>,
        decisions: Vec<Decision>,
        blockers: Vec<Blocker>,
        next_action: &str,
        context_notes: &str,
    ) -> Result<HandoffData> {
        let session_id = if session_id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            session_id.to_owned()
        };

        // Merge completed + remaining into the unified work_items list.
        let mut work_items: Vec<WorkItem> = completed;
        for mut item in remaining {
            // Ensure items not already marked Done retain their original status.
            if item.status == WorkItemStatus::Done {
                item.status = WorkItemStatus::Pending;
            }
            work_items.push(item);
        }

        // Fold the current task into task_states.
        let task_states: Vec<TaskState> = current_task.into_iter().collect();

        // Build the summary from next_action and context_notes.
        let summary = build_summary(next_action, context_notes);

        Ok(HandoffData {
            session_id,
            timestamp: Utc::now(),
            work_items,
            decisions,
            blockers,
            task_states,
            summary,
        })
    }

    // ─── Persist ─────────────────────────────────────────────────────────────

    /// Atomically write `handoff` to `{handoff_dir}/{session_id}.json` and
    /// `{handoff_dir}/{session_id}.md`.
    pub fn save(&self, handoff: &HandoffData) -> Result<()> {
        self.ensure_dir()?;

        let json_path = self
            .handoff_dir
            .join(format!("{}.json", handoff.session_id));
        let md_path = self
            .handoff_dir
            .join(format!("{}.md", handoff.session_id));

        let json = serde_json::to_string_pretty(handoff).map_err(MemoryError::Serialisation)?;
        let markdown = self.to_markdown(handoff);

        self.atomic_write(&json_path, &json)?;
        self.atomic_write(&md_path, &markdown)?;

        Ok(())
    }

    // ─── Load ────────────────────────────────────────────────────────────────

    /// Load the handoff for `session_id`.  Returns `Ok(None)` if no such file
    /// exists.
    pub fn load(&self, session_id: &str) -> Result<Option<HandoffData>> {
        let path = self
            .handoff_dir
            .join(format!("{session_id}.json"));

        if !path.exists() {
            return Ok(None);
        }

        let text = fs::read_to_string(&path)
            .map_err(|e| MemoryError::Store(format!("read handoff {}: {e}", path.display())))?;

        let data: HandoffData =
            serde_json::from_str(&text).map_err(MemoryError::Serialisation)?;

        Ok(Some(data))
    }

    /// Load the most recently modified handoff, regardless of session ID.
    ///
    /// Returns `Ok(None)` if the directory is empty or does not exist.
    pub fn load_latest(&self) -> Result<Option<HandoffData>> {
        if !self.handoff_dir.exists() {
            return Ok(None);
        }

        let latest = fs::read_dir(&self.handoff_dir)
            .map_err(|e| {
                MemoryError::Store(format!(
                    "read handoff dir {}: {e}",
                    self.handoff_dir.display()
                ))
            })?
            .filter_map(std::result::Result::ok)
            .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
            .filter_map(|e| {
                e.metadata()
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .map(|t| (t, e.path()))
            })
            .max_by_key(|(t, _)| *t)
            .map(|(_, p)| p);

        match latest {
            None => Ok(None),
            Some(path) => {
                let text = fs::read_to_string(&path).map_err(|e| {
                    MemoryError::Store(format!("read handoff {}: {e}", path.display()))
                })?;
                let data: HandoffData =
                    serde_json::from_str(&text).map_err(MemoryError::Serialisation)?;
                Ok(Some(data))
            }
        }
    }

    // ─── Serialise / Deserialise (legacy helpers) ────────────────────────────

    /// Serialise `data` to a JSON string suitable for embedding in a system
    /// prompt or passing out-of-band.
    pub fn serialise(&self, data: &HandoffData) -> Result<String> {
        serde_json::to_string_pretty(data).map_err(MemoryError::Serialisation)
    }

    /// Deserialise a handoff packet from a raw JSON string.
    pub fn deserialise(&self, json: &str) -> Result<HandoffData> {
        serde_json::from_str(json).map_err(MemoryError::Serialisation)
    }

    // ─── Remove ──────────────────────────────────────────────────────────────

    /// Delete the handoff files for `session_id`.
    ///
    /// This is a one-shot artefact; call `remove` after the next session has
    /// successfully resumed from the data.  Missing files are silently ignored.
    pub fn remove(&self, session_id: &str) -> Result<()> {
        for ext in &["json", "md"] {
            let path = self.handoff_dir.join(format!("{session_id}.{ext}"));
            if path.exists() {
                fs::remove_file(&path).map_err(|e| {
                    MemoryError::Store(format!("remove handoff {}: {e}", path.display()))
                })?;
            }
        }
        Ok(())
    }

    // ─── Resume ──────────────────────────────────────────────────────────────

    /// Restore session state from a previously saved [`HandoffData`].
    ///
    /// Steps performed:
    /// 1. Validate data integrity (session ID non-empty, timestamp not in
    ///    the future by more than a clock-skew tolerance).
    /// 2. Normalise work-item statuses: `InProgress` items are reset to
    ///    `Pending` because they were interrupted mid-session and must be
    ///    restarted by the next session.
    /// 3. Replay decisions into a local `DecisionThreadStore` and persist the
    ///    populated threads as `{session_id}.decisions.json` so the
    ///    orchestrator can load them without re-parsing the raw handoff.
    /// 4. Write a `{session_id}.resumed.json` snapshot of the normalised
    ///    handoff to `handoff_dir`, atomically, so that other processes can
    ///    detect that the resume has been applied.
    pub async fn resume(&self, mut data: HandoffData) -> Result<()> {
        // ── 1. Validate ───────────────────────────────────────────────────
        if data.session_id.is_empty() {
            return Err(MemoryError::InvalidArgument(
                "handoff session_id must not be empty".into(),
            ));
        }

        // Reject packets that claim to be from more than 1 hour in the future
        // (likely a clock skew or corrupted file).
        let now = Utc::now();
        let skew_tolerance = chrono::Duration::hours(1);
        if data.timestamp > now + skew_tolerance {
            return Err(MemoryError::InvalidArgument(format!(
                "handoff timestamp {} is too far in the future (now: {})",
                data.timestamp.format("%Y-%m-%d %H:%M:%S UTC"),
                now.format("%Y-%m-%d %H:%M:%S UTC"),
            )));
        }

        // ── 2. Normalise work-item statuses ───────────────────────────────
        // Items that were `InProgress` when the previous session ended were
        // interrupted and must be queued again for the new session.
        for item in &mut data.work_items {
            if item.status == WorkItemStatus::InProgress {
                item.status = WorkItemStatus::Pending;
            }
        }

        // ── 3. Replay decisions into a DecisionThreadStore ────────────────
        use crate::seeds::DecisionThreadStore;

        let mut decision_store = DecisionThreadStore::new();
        for decision in &data.decisions {
            decision_store.record(
                &decision.summary,
                decision.summary.clone(),
                decision.rationale.clone(),
                vec![],
            );
        }

        // Persist the decision threads alongside the handoff files so the
        // orchestrator can read them without re-parsing the whole packet.
        if !data.decisions.is_empty() {
            let threads_path = self
                .handoff_dir
                .join(format!("{}.decisions.json", data.session_id));
            // Serialise the list of decisions (not the internal store struct,
            // which is not Serialize) – the raw Vec<Decision> is sufficient.
            let threads_json = serde_json::to_string_pretty(&data.decisions)
                .map_err(MemoryError::Serialisation)?;
            self.ensure_dir()?;
            self.atomic_write(&threads_path, &threads_json)?;
        }

        // ── 4. Write normalised resumed snapshot ──────────────────────────
        let resumed_path = self
            .handoff_dir
            .join(format!("{}.resumed.json", data.session_id));
        let resumed_json =
            serde_json::to_string_pretty(&data).map_err(MemoryError::Serialisation)?;
        self.ensure_dir()?;
        self.atomic_write(&resumed_path, &resumed_json)?;

        Ok(())
    }

    // ─── Private helpers ─────────────────────────────────────────────────────

    /// Ensure the handoff directory exists, creating it if necessary.
    fn ensure_dir(&self) -> Result<()> {
        fs::create_dir_all(&self.handoff_dir).map_err(|e| {
            MemoryError::Store(format!(
                "create handoff dir {}: {e}",
                self.handoff_dir.display()
            ))
        })
    }

    /// Atomically write `content` to `path` via a `.tmp` rename.
    fn atomic_write(&self, path: &Path, content: &str) -> Result<()> {
        // Ensure parent dir exists.
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                MemoryError::Store(format!("create dir {}: {e}", parent.display()))
            })?;
        }

        let tmp_path = path.with_extension("tmp");

        {
            let mut tmp = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)
                .map_err(|e| {
                    MemoryError::Store(format!("open tmp {}: {e}", tmp_path.display()))
                })?;

            tmp.write_all(content.as_bytes()).map_err(|e| {
                MemoryError::Store(format!("write tmp {}: {e}", tmp_path.display()))
            })?;

            tmp.flush().map_err(|e| {
                MemoryError::Store(format!("flush tmp {}: {e}", tmp_path.display()))
            })?;
        }

        fs::rename(&tmp_path, path).map_err(|e| {
            MemoryError::Store(format!(
                "rename {} → {}: {e}",
                tmp_path.display(),
                path.display()
            ))
        })
    }

    /// Render a [`HandoffData`] as human-readable Markdown.
    fn to_markdown(&self, handoff: &HandoffData) -> String {
        use std::fmt::Write as _;

        let mut md = String::with_capacity(1024);

        let _ = writeln!(md, "# Handoff – Session `{}`", handoff.session_id);
        let _ = writeln!(md);
        let _ = writeln!(
            md,
            "**Timestamp:** {}",
            handoff.timestamp.format("%Y-%m-%d %H:%M:%S UTC")
        );
        let _ = writeln!(md);

        // ── Summary ──────────────────────────────────────────────────────────
        let _ = writeln!(md, "## Summary");
        let _ = writeln!(md);
        let _ = writeln!(md, "{}", handoff.summary);
        let _ = writeln!(md);

        // ── Task states ───────────────────────────────────────────────────────
        if !handoff.task_states.is_empty() {
            let _ = writeln!(md, "## Current Task");
            let _ = writeln!(md);
            for ts in &handoff.task_states {
                let _ = writeln!(
                    md,
                    "- **{}** – {}% complete (checkpoint: `{}`)",
                    ts.task_id, ts.progress_percent, ts.last_checkpoint
                );
            }
            let _ = writeln!(md);
        }

        // ── Work items ────────────────────────────────────────────────────────
        if !handoff.work_items.is_empty() {
            let _ = writeln!(md, "## Work Items");
            let _ = writeln!(md);
            for wi in &handoff.work_items {
                let status_icon = match wi.status {
                    WorkItemStatus::Done => "✅",
                    WorkItemStatus::InProgress => "🔄",
                    WorkItemStatus::Blocked => "🚫",
                    WorkItemStatus::Pending => "⬜",
                };
                let _ = writeln!(
                    md,
                    "- {} **{}** ({:?}) – {}",
                    status_icon, wi.title, wi.priority, wi.description
                );
            }
            let _ = writeln!(md);
        }

        // ── Decisions ─────────────────────────────────────────────────────────
        if !handoff.decisions.is_empty() {
            let _ = writeln!(md, "## Key Decisions");
            let _ = writeln!(md);
            for d in &handoff.decisions {
                let _ = writeln!(
                    md,
                    "### {} (`{:?}`)",
                    d.summary, d.status
                );
                let _ = writeln!(md, "> {}", d.rationale);
                let _ = writeln!(md);
            }
        }

        // ── Blockers ──────────────────────────────────────────────────────────
        if !handoff.blockers.is_empty() {
            let _ = writeln!(md, "## Blockers");
            let _ = writeln!(md);
            for b in &handoff.blockers {
                let hint = b
                    .resolution_hint
                    .as_deref()
                    .unwrap_or("no hint available");
                let _ = writeln!(md, "- **Blocker:** {} *(hint: {})*", b.description, hint);
            }
            let _ = writeln!(md);
        }

        md
    }
}

impl Default for HandoffManager {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Private free functions ───────────────────────────────────────────────────

fn build_summary(next_action: &str, context_notes: &str) -> String {
    let mut parts = Vec::new();
    if !next_action.is_empty() {
        parts.push(format!("Next action: {next_action}"));
    }
    if !context_notes.is_empty() {
        parts.push(format!("Notes: {context_notes}"));
    }
    if parts.is_empty() {
        "Session state saved for handoff.".into()
    } else {
        parts.join("\n\n")
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Priority, WorkItemStatus};
    use tempfile::TempDir;

    fn make_manager(tmp: &TempDir) -> HandoffManager {
        HandoffManager::with_dir(tmp.path().join("handoffs"))
    }

    fn sample_handoff(session_id: &str) -> HandoffData {
        HandoffData {
            session_id: session_id.to_owned(),
            timestamp: Utc::now(),
            work_items: vec![WorkItem {
                id: "w1".into(),
                title: "Implement feature X".into(),
                description: "Write the code".into(),
                status: WorkItemStatus::InProgress,
                priority: Priority::High,
            }],
            decisions: vec![],
            blockers: vec![],
            task_states: vec![],
            summary: "Test handoff".into(),
        }
    }

    #[test]
    fn save_and_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(&tmp);
        let h = sample_handoff("test-session-1");
        mgr.save(&h).unwrap();

        let loaded = mgr.load("test-session-1").unwrap().unwrap();
        assert_eq!(loaded.session_id, "test-session-1");
        assert_eq!(loaded.work_items.len(), 1);
    }

    #[test]
    fn load_missing_returns_none() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(&tmp);
        assert!(mgr.load("nonexistent").unwrap().is_none());
    }

    #[test]
    fn load_latest_returns_most_recent() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(&tmp);
        mgr.save(&sample_handoff("s1")).unwrap();
        // Small sleep to ensure different mtime on most filesystems.
        std::thread::sleep(std::time::Duration::from_millis(10));
        mgr.save(&sample_handoff("s2")).unwrap();

        let latest = mgr.load_latest().unwrap().unwrap();
        assert_eq!(latest.session_id, "s2");
    }

    #[test]
    fn remove_deletes_both_files() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(&tmp);
        let h = sample_handoff("remove-me");
        mgr.save(&h).unwrap();
        mgr.remove("remove-me").unwrap();
        assert!(mgr.load("remove-me").unwrap().is_none());
    }

    #[test]
    fn markdown_file_is_created() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(&tmp);
        mgr.save(&sample_handoff("md-test")).unwrap();
        let md_path = tmp.path().join("handoffs").join("md-test.md");
        assert!(md_path.exists());
        let content = fs::read_to_string(md_path).unwrap();
        assert!(content.contains("md-test"));
    }

    #[test]
    fn create_handoff_builds_summary() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(&tmp);
        let h = mgr
            .create_handoff(
                "cx1",
                None,
                vec![],
                vec![],
                vec![],
                vec![],
                "Continue refactoring",
                "Focus on the storage layer",
            )
            .unwrap();
        assert!(h.summary.contains("Continue refactoring"));
        assert!(h.summary.contains("Focus on the storage layer"));
    }

    #[tokio::test]
    async fn resume_writes_resumed_json() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(&tmp);
        let h = sample_handoff("resume-test");
        mgr.resume(h).await.unwrap();

        let resumed_path = tmp
            .path()
            .join("handoffs")
            .join("resume-test.resumed.json");
        assert!(resumed_path.exists(), "resumed.json should be written");
    }

    #[tokio::test]
    async fn resume_normalises_in_progress_to_pending() {
        use crate::types::Priority;
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(&tmp);

        let mut h = sample_handoff("normalise-test");
        h.work_items.push(WorkItem {
            id: "w2".into(),
            title: "Partial work".into(),
            description: "Was running".into(),
            status: WorkItemStatus::InProgress,
            priority: Priority::Normal,
        });
        mgr.resume(h).await.unwrap();

        let resumed_path = tmp
            .path()
            .join("handoffs")
            .join("normalise-test.resumed.json");
        let content = fs::read_to_string(resumed_path).unwrap();
        // The InProgress item must have been reset to Pending in the file.
        assert!(
            content.contains("Pending"),
            "InProgress item should be normalised to Pending"
        );
        assert!(
            !content.contains("InProgress"),
            "no InProgress items should remain after normalisation"
        );
    }

    #[tokio::test]
    async fn resume_writes_decisions_json_when_decisions_present() {
        use crate::types::{Decision, DecisionStatus};
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(&tmp);

        let mut h = sample_handoff("decisions-test");
        h.decisions.push(Decision {
            id: "d1".into(),
            summary: "Use SQLite for storage".into(),
            rationale: "Simple, no extra deps".into(),
            status: DecisionStatus::Implemented,
            made_at: Utc::now(),
        });
        mgr.resume(h).await.unwrap();

        let decisions_path = tmp
            .path()
            .join("handoffs")
            .join("decisions-test.decisions.json");
        assert!(
            decisions_path.exists(),
            "decisions.json should be written when decisions are present"
        );
        let content = fs::read_to_string(decisions_path).unwrap();
        assert!(content.contains("Use SQLite for storage"));
    }

    #[tokio::test]
    async fn resume_rejects_empty_session_id() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(&tmp);
        let mut h = sample_handoff("");
        h.session_id = String::new();
        let err = mgr.resume(h).await.unwrap_err();
        assert!(
            matches!(err, MemoryError::InvalidArgument(_)),
            "expected InvalidArgument, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn resume_rejects_future_timestamp() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(&tmp);
        let mut h = sample_handoff("future-ts");
        // Set timestamp 2 hours into the future (beyond 1-hour skew tolerance).
        h.timestamp = Utc::now() + chrono::Duration::hours(2);
        let err = mgr.resume(h).await.unwrap_err();
        assert!(
            matches!(err, MemoryError::InvalidArgument(_)),
            "expected InvalidArgument for future timestamp, got: {err:?}"
        );
    }
}
