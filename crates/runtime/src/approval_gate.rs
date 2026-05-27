//! Smart Approval Gate — intelligent command approval with SOLO mode support.
//!
//! This module provides the `SmartApprovalGate` which sits between the
//! destructive pattern detector and the conversation runtime, applying
//! approval configuration (SOLO mode, auto-pass settings) and managing
//! the blocking approval flow with the frontend via SSE + oneshot channels.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, RwLock};

use crate::config::ApprovalConfig;
use crate::permission_enforcer::{
    ApprovalPersistence, ApprovalRequest, ApprovalVerdict, AutoPassReason,
    DestructivePatternDetector, SmartApprovalVerdict,
};
use crate::platform::adapter::PlatformAdapter;
use crate::platform::feishu::{ApprovalCard, FeishuAdapter};

/// Result of evaluating a command through the approval gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApprovalGateResult {
    /// Command is allowed without user interaction.
    AutoPass { reason: AutoPassReason },
    /// Command was explicitly approved by the user (with persistence info).
    Approved { persistence: ApprovalPersistence },
    /// Command was denied by the user.
    Denied { reason: String },
    /// Approval request timed out without user response.
    TimedOut,
}

// ═══════════════════════════════════════════════════════════════════════
// Approval History
// ═══════════════════════════════════════════════════════════════════════

/// Outcome of an approval decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalHistoryOutcome {
    Approved { persistence: String },
    Denied { reason: String },
    TimedOut,
}

/// A single approval history entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalHistoryEntry {
    pub id: String,
    pub request_id: String,
    pub command: String,
    pub normalized_command: String,
    pub risk_level: String,
    pub matched_patterns: Vec<String>,
    pub outcome: ApprovalHistoryOutcome,
    pub resolved_at: String,
}

const APPROVAL_HISTORY_MAX: usize = 200;

/// Persistent store for approval history.
pub struct ApprovalHistoryStore {
    entries: Arc<RwLock<Vec<ApprovalHistoryEntry>>>,
    storage_path: Option<PathBuf>,
}

impl ApprovalHistoryStore {
    pub fn new(storage_path: Option<PathBuf>) -> Self {
        let entries = storage_path
            .as_ref()
            .and_then(|p| Self::load_from_disk(p).ok())
            .unwrap_or_default();
        Self {
            entries: Arc::new(RwLock::new(entries)),
            storage_path,
        }
    }

    fn load_from_disk(path: &PathBuf) -> Result<Vec<ApprovalHistoryEntry>, String> {
        let content = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let entries: Vec<ApprovalHistoryEntry> =
            serde_json::from_str(&content).map_err(|e| e.to_string())?;
        Ok(entries)
    }

    async fn save_to_disk(&self) -> Result<(), String> {
        let path = match &self.storage_path {
            Some(p) => p,
            None => return Ok(()),
        };
        let entries = self.entries.read().await;
        let content = serde_json::to_string_pretty(&*entries).map_err(|e| e.to_string())?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        std::fs::write(path, content).map_err(|e| e.to_string())
    }

    /// Record an approval history entry and persist.
    pub async fn record(&self, entry: ApprovalHistoryEntry) {
        let mut entries = self.entries.write().await;
        entries.insert(0, entry);
        if entries.len() > APPROVAL_HISTORY_MAX {
            entries.truncate(APPROVAL_HISTORY_MAX);
        }
        drop(entries);
        if let Err(e) = self.save_to_disk().await {
            tracing::warn!("Failed to persist approval history: {e}");
        }
    }

    /// List approval history with pagination.
    pub async fn list_history(
        &self,
        limit: usize,
        offset: usize,
    ) -> (Vec<ApprovalHistoryEntry>, usize) {
        let entries = self.entries.read().await;
        let total = entries.len();
        let page: Vec<ApprovalHistoryEntry> = entries
            .iter()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect();
        (page, total)
    }
}

/// Trait for sending approval-related SSE events to the frontend.
///
/// The server implements this trait to push events through the SSE stream
/// without the gate needing direct access to the response channel.
pub trait ApprovalSseSender: Send + Sync {
    /// Push an approval request event to the frontend.
    fn send_approval_request(&self, request: &ApprovalRequest);
    /// Push an approval resolved event to the frontend.
    fn send_approval_resolved(&self, request_id: &str, verdict: &ApprovalVerdict);
    /// Push a SOLO mode changed event to the frontend.
    fn send_solo_mode_changed(&self, enabled: bool, honor_critical: bool);
}

/// The smart approval gate combines destructive pattern detection with
/// approval configuration to make intelligent decisions about which commands
/// require explicit user approval.
///
/// # Flow
/// 1. Check if the tool is read-only → AutoPass
/// 2. Run `detect_with_config()` → get `SmartApprovalVerdict`
/// 3. If AutoPass → return immediately
/// 4. If NeedsApproval → register pending request, send SSE, await oneshot
/// 5. Return the user's verdict (or TimedOut)
pub struct SmartApprovalGate {
    /// The destructive pattern detector.
    detector: Arc<DestructivePatternDetector>,
    /// Runtime-toggleable approval configuration (for SOLO mode, etc.).
    config: Arc<RwLock<ApprovalConfig>>,
    /// Pending approval requests: approval_id → (request, oneshot sender).
    pending: Arc<RwLock<HashMap<String, (ApprovalRequest, oneshot::Sender<ApprovalVerdict>)>>>,
    /// Optional SSE sender for pushing approval events to the frontend.
    sse_sender: Option<Arc<dyn ApprovalSseSender>>,
    /// Persistent approval history store.
    history: Arc<ApprovalHistoryStore>,
    session_approved: Arc<tokio::sync::Mutex<HashSet<String>>>,
    feishu_adapter: Option<Arc<FeishuAdapter>>,
    card_approval_map: Arc<RwLock<HashMap<u64, (String, String, String)>>>,
}

/// Tools that are inherently read-only and never need approval.
const READ_ONLY_TOOLS: &[&str] = &[
    "read_file",
    "grep_search",
    "glob_search",
    "list_directory",
    "web_fetch",
    "web_search",
];

impl SmartApprovalGate {
    /// Create a new smart approval gate.
    pub fn new(
        detector: Arc<DestructivePatternDetector>,
        config: ApprovalConfig,
        history_path: Option<PathBuf>,
    ) -> Self {
        Self {
            detector,
            config: Arc::new(RwLock::new(config)),
            pending: Arc::new(RwLock::new(HashMap::new())),
            sse_sender: None,
            history: Arc::new(ApprovalHistoryStore::new(history_path)),
            session_approved: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            feishu_adapter: None,
            card_approval_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set the SSE sender for pushing approval events to the frontend.
    #[must_use]
    pub fn with_sse_sender(mut self, sender: Arc<dyn ApprovalSseSender>) -> Self {
        self.sse_sender = Some(sender);
        self
    }

    /// Attach a Feishu adapter for sending interactive approval cards.
    #[must_use]
    pub fn with_feishu_adapter(mut self, adapter: Arc<FeishuAdapter>) -> Self {
        self.feishu_adapter = Some(adapter);
        self
    }

    pub fn feishu_adapter(&self) -> Option<&Arc<FeishuAdapter>> {
        self.feishu_adapter.as_ref()
    }

    pub fn card_approval_map(&self) -> &Arc<RwLock<HashMap<u64, (String, String, String)>>> {
        &self.card_approval_map
    }

    /// Get a reference to the approval config (for API endpoints).
    pub fn config(&self) -> &Arc<RwLock<ApprovalConfig>> {
        &self.config
    }

    /// Get a reference to the pending approvals map (for API endpoints).
    pub fn pending(&self) -> &Arc<RwLock<HashMap<String, (ApprovalRequest, oneshot::Sender<ApprovalVerdict>)>>> {
        &self.pending
    }

    /// Get a reference to the destructive pattern detector.
    pub fn detector(&self) -> &Arc<DestructivePatternDetector> {
        &self.detector
    }

    /// Get a reference to the approval history store (for API endpoints).
    pub fn history(&self) -> &Arc<ApprovalHistoryStore> {
        &self.history
    }

    /// Update the approval configuration at runtime (for SOLO toggle, etc.).
    pub async fn update_config(&self, new_config: ApprovalConfig) {
        let solo_changed = {
            let old = self.config.read().await;
            old.solo_mode != new_config.solo_mode
        };

        *self.config.write().await = new_config;

        // Notify frontend if SOLO mode changed
        if solo_changed {
            let config = self.config.read().await;
            if let Some(sender) = &self.sse_sender {
                sender.send_solo_mode_changed(config.solo_mode, config.solo_honor_critical);
            }
        }
    }

    /// Evaluate whether a tool invocation requires approval.
    ///
    /// For bash/shell tools, this runs the full detection pipeline.
    /// For read-only tools, it auto-passes.
    /// For other tools, it auto-passes (they have their own permission checks).
    pub async fn evaluate(&self, tool_name: &str, input: &str) -> ApprovalGateResult {
        // Step 0: Same-session auto-approve
        let key = format!("{tool_name}:{}", &input[..input.len().min(80)]);
        if self.session_approved.lock().await.contains(&key) {
            return ApprovalGateResult::AutoPass { reason: AutoPassReason::ReadOnlyCommand };
        }
        // Step 1: Read-only tools always auto-pass
        if READ_ONLY_TOOLS.contains(&tool_name) {
            return ApprovalGateResult::AutoPass {
                reason: AutoPassReason::ReadOnlyCommand,
            };
        }

        // Step 2: Only bash-like tools go through destructive detection
        if !Self::is_bash_tool(tool_name) {
            return ApprovalGateResult::AutoPass {
                reason: AutoPassReason::ReadOnlyCommand,
            };
        }

        // Step 3: Extract command string from JSON input
        let command = Self::extract_command(input);

        // Step 4: Run smart detection with current config
        let config = self.config.read().await;
        let verdict = self.detector.detect_with_config(&command, &config);
        drop(config); // Release read lock before potential await

        match verdict {
            SmartApprovalVerdict::AutoPass { reason } => {
                if !matches!(reason, AutoPassReason::NoPatternMatch) {
                    tracing::info!(
                        tool = %tool_name,
                        command = %command,
                        reason = ?reason,
                        "Command auto-passed by smart approval gate"
                    );
                }
                ApprovalGateResult::AutoPass { reason }
            }
            SmartApprovalVerdict::NeedsApproval(request) => {
                self.request_approval(request).await
            }
        }
    }

    /// Submit an approval request and wait for the user's response.
    ///
    /// This creates a oneshot channel, registers the request in the pending
    /// map, sends an SSE event to the frontend, and blocks until:
    /// - The user responds (via POST /api/approval/respond)
    /// - The timeout expires (120 seconds)
    async fn request_approval(&self, request: ApprovalRequest) -> ApprovalGateResult {
        let request_id = request.id.clone();
        let timeout_secs = request.timeout_secs;

        // Create oneshot channel for the verdict
        let (tx, rx) = oneshot::channel();

        // Register in pending map
        self.pending
            .write()
            .await
            .insert(request_id.clone(), (request.clone(), tx));

        // Send SSE event to frontend
        if let Some(sender) = &self.sse_sender {
            sender.send_approval_request(&request);
        }



        // Wait for response with timeout
        match tokio::time::timeout(Duration::from_secs(timeout_secs), rx).await {
            Ok(Ok(verdict)) => match verdict {
                ApprovalVerdict::Approved => {
                    // Resolve event will be sent by the respond handler
                    ApprovalGateResult::Approved {
                        persistence: ApprovalPersistence::Once, // Default; actual persistence recorded by handler
                    }
                }
                ApprovalVerdict::Denied { reason } => ApprovalGateResult::Denied { reason },
                ApprovalVerdict::TimedOut => ApprovalGateResult::TimedOut,
            },
            Ok(Err(_)) => {
                // Channel closed without response
                self.pending.write().await.remove(&request_id);
                // Record history: denied due to channel closed
                let entry = ApprovalHistoryEntry {
                    id: format!("ah_{}", rand::random::<u32>()),
                    request_id: request.id.clone(),
                    command: request.command.clone(),
                    normalized_command: request.normalized_command.clone(),
                    risk_level: format!("{:?}", request.risk_level).to_lowercase(),
                    matched_patterns: request.matched_patterns.clone(),
                    outcome: ApprovalHistoryOutcome::Denied {
                        reason: "Approval channel closed".to_string(),
                    },
                    resolved_at: chrono::Utc::now().to_rfc3339(),
                };
                let history = self.history.clone();
                tokio::spawn(async move {
                    history.record(entry).await;
                });
                ApprovalGateResult::Denied {
                    reason: "Approval channel closed".to_string(),
                }
            }
            Err(_) => {
                // Timeout
                self.pending.write().await.remove(&request_id);
                if let Some(sender) = &self.sse_sender {
                    sender.send_approval_resolved(&request_id, &ApprovalVerdict::TimedOut);
                }
                // Record history: timed out
                let entry = ApprovalHistoryEntry {
                    id: format!("ah_{}", rand::random::<u32>()),
                    request_id: request.id.clone(),
                    command: request.command.clone(),
                    normalized_command: request.normalized_command.clone(),
                    risk_level: format!("{:?}", request.risk_level).to_lowercase(),
                    matched_patterns: request.matched_patterns.clone(),
                    outcome: ApprovalHistoryOutcome::TimedOut,
                    resolved_at: chrono::Utc::now().to_rfc3339(),
                };
                let history = self.history.clone();
                tokio::spawn(async move {
                    history.record(entry).await;
                });
                ApprovalGateResult::TimedOut
            }
        }
    }

    /// Check if a tool name is a bash/shell execution tool.
    fn is_bash_tool(tool_name: &str) -> bool {
        matches!(
            tool_name,
            "bash" | "execute_bash" | "shell" | "execute_shell" | "run_command"
        )
    }

    /// Extract the command string from a tool's JSON input.
    ///
    /// Handles both `{"command": "..."}` format and raw string input.
    fn extract_command(input: &str) -> String {
        // Try JSON format first
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(input) {
            if let Some(cmd) = parsed.get("command").and_then(|v| v.as_str()) {
                return cmd.to_string();
            }
        }
        // Fall back to raw input
        input.to_string()
    }

    /// Send an interactive approval card via the Feishu adapter.
    ///
    /// Returns the Feishu message ID and the card's approval ID on success.
    /// The caller should store the `card_approval_id` for later resolution
    /// via [`resolve_approval_by_card`].
    pub async fn send_approval_card(
        &self,
        chat_id: &str,
        request_id: &str,
    ) -> Option<(u64, String)> {
        let adapter = self.feishu_adapter.as_ref()?;
        let pending = self.pending.read().await;
        let (request, _tx) = pending.get(request_id)?;
        let command = request.command.clone();
        let risk = format!("{:?}", request.risk_level);
        drop(pending);

        let approval_id = adapter.next_approval_id();
        let card = ApprovalCard::new(approval_id, &command)
            .with_description(&format!("Risk level: {}", risk));
        let card_json = card.build();

        match adapter.send_card(chat_id, &card_json).await {
            Ok(message_id) => {
                let mid: String = message_id.clone();
                self.card_approval_map.write().await.insert(
                    approval_id,
                    (request_id.to_string(), mid, chat_id.to_string()),
                );
                tracing::info!(
                    approval_id,
                    request_id,
                    %message_id,
                    "Approval card sent via Feishu"
                );
                Some((approval_id, message_id))
            }
            Err(e) => {
                tracing::error!(%e, "Failed to send approval card via Feishu");
                None
            }
        }
    }

    /// Resolve a pending approval triggered by a Feishu card button callback.
    ///
    /// Maps `hermes_action` to an [`ApprovalVerdict`], resolves the pending request,
    /// updates the card to its resolved state, and returns the resolution result.
    pub async fn resolve_approval_by_card(
        &self,
        card_approval_id: u64,
        hermes_action: &str,
        operator_name: &str,
    ) -> Option<ApprovalVerdict> {
        let (request_id, message_id, _chat_id) = {
            let map = self.card_approval_map.read().await;
            map.get(&card_approval_id)?.clone()
        };

        let verdict = match hermes_action {
            "approve_once" | "approve_session" | "approve_always" | "approved" => {
                ApprovalVerdict::Approved
            }
            "deny" | "denied" | "reject" | "rejected" => ApprovalVerdict::Denied {
                reason: format!("Denied by {} via Feishu", operator_name),
            },
            _ => ApprovalVerdict::Denied {
                reason: format!("Unknown action '{}' from {}", hermes_action, operator_name),
            },
        };

        let persistence = match hermes_action {
            "approve_session" => ApprovalPersistence::Session,
            "approve_always" => ApprovalPersistence::Always,
            _ => ApprovalPersistence::Once,
        };

        self.resolve_approval(&request_id, verdict.clone(), persistence)
            .await;

        if let Some(adapter) = &self.feishu_adapter {
            if let Err(e) = adapter
                .update_approval_card(&message_id, hermes_action, operator_name)
                .await
            {
                tracing::warn!(%e, "Failed to update approval card to resolved state");
            }
        }

        self.card_approval_map.write().await.remove(&card_approval_id);

        Some(verdict)
    }

    /// Resolve a pending approval request (called by the API endpoint handler).
    ///
    /// Returns the ApprovalRequest if found, or None if expired.
    pub async fn resolve_approval(
        &self,
        request_id: &str,
        verdict: ApprovalVerdict,
        persistence: ApprovalPersistence,
    ) -> Option<ApprovalRequest> {
        // Capture persistence string before it's moved into record_approval
        let persistence_str = format!("{:?}", persistence).to_lowercase();

        let mut pending = self.pending.write().await;
        if let Some((request, sender)) = pending.remove(request_id) {
            // Record the approval decision in the detector's cache
            self.detector
                .record_approval(&request.command, persistence)
                .await;

            // Send the verdict through the oneshot channel
            let _ = sender.send(verdict.clone());

            // Notify frontend of resolution
            if let Some(sse_sender) = &self.sse_sender {
                sse_sender.send_approval_resolved(request_id, &verdict);
            }

            // Record approval history
            let history_outcome = match &verdict {
                ApprovalVerdict::Approved => ApprovalHistoryOutcome::Approved {
                    persistence: persistence_str,
                },
                ApprovalVerdict::Denied { reason } => ApprovalHistoryOutcome::Denied {
                    reason: reason.clone(),
                },
                ApprovalVerdict::TimedOut => ApprovalHistoryOutcome::TimedOut,
            };
            let entry = ApprovalHistoryEntry {
                id: format!("ah_{}", rand::random::<u32>()),
                request_id: request.id.clone(),
                command: request.command.clone(),
                normalized_command: request.normalized_command.clone(),
                risk_level: format!("{:?}", request.risk_level).to_lowercase(),
                matched_patterns: request.matched_patterns.clone(),
                outcome: history_outcome,
                resolved_at: chrono::Utc::now().to_rfc3339(),
            };
            let history = self.history.clone();
            tokio::spawn(async move {
                history.record(entry).await;
            });

            Some(request)
        } else {
            None
        }
    }

    /// Get all pending approval requests (for GET /api/approval/pending).
    pub async fn get_pending_requests(&self) -> Vec<ApprovalRequest> {
        let pending = self.pending.read().await;
        pending
            .values()
            .map(|(req, _)| req.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_gate(config: ApprovalConfig) -> SmartApprovalGate {
        let detector = Arc::new(DestructivePatternDetector::new(PathBuf::from("/tmp")));
        SmartApprovalGate::new(detector, config, None)
    }

    #[tokio::test]
    async fn read_only_tools_auto_pass() {
        let gate = make_gate(ApprovalConfig::default());
        let result = gate.evaluate("read_file", r#"{"path": "/etc/passwd"}"#).await;
        assert!(matches!(
            result,
            ApprovalGateResult::AutoPass {
                reason: AutoPassReason::ReadOnlyCommand
            }
        ));
    }

    #[tokio::test]
    async fn unknown_tools_auto_pass() {
        let gate = make_gate(ApprovalConfig::default());
        let result = gate.evaluate("custom_tool", r#"{"data": "test"}"#).await;
        assert!(matches!(
            result,
            ApprovalGateResult::AutoPass {
                reason: AutoPassReason::ReadOnlyCommand
            }
        ));
    }

    #[tokio::test]
    async fn bash_read_only_command_auto_passes() {
        let gate = make_gate(ApprovalConfig::default());
        let result = gate
            .evaluate("bash", r#"{"command": "ls -la"}"#)
            .await;
        assert!(matches!(
            result,
            ApprovalGateResult::AutoPass {
                reason: AutoPassReason::ReadOnlyCommand
            }
        ));
    }

    #[tokio::test]
    async fn bash_safe_command_auto_passes() {
        let gate = make_gate(ApprovalConfig::default());
        let result = gate
            .evaluate("bash", r#"{"command": "echo hello"}"#)
            .await;
        // echo is in read-only list
        assert!(matches!(result, ApprovalGateResult::AutoPass { .. }));
    }

    #[tokio::test]
    async fn bash_destructive_command_needs_approval() {
        let _gate = make_gate(ApprovalConfig::default());
        // This will try to send SSE and block — since no SSE sender, it will
        // register in pending and timeout. For unit testing, we just verify
        // it doesn't auto-pass.
        // Note: In a real test, we'd need to handle the pending approval flow.
        // For now, verify the detect_with_config logic directly.
        let config = ApprovalConfig::default();
        let detector = DestructivePatternDetector::new(PathBuf::from("/tmp"));
        let verdict = detector.detect_with_config("rm -rf /tmp/test", &config);
        assert!(matches!(verdict, SmartApprovalVerdict::NeedsApproval(_)));
    }

    #[tokio::test]
    async fn solo_mode_bypasses_high_risk() {
        let config = ApprovalConfig::default().with_solo_mode(true);
        let detector = DestructivePatternDetector::new(PathBuf::from("/tmp"));
        // Use git push --force which is High risk, not caught by is_read_only_command
        let verdict = detector.detect_with_config("git push --force origin main", &config);
        assert!(matches!(
            verdict,
            SmartApprovalVerdict::AutoPass {
                reason: AutoPassReason::SoloBypass
            }
        ));
    }

    #[tokio::test]
    async fn solo_mode_honors_critical() {
        let config = ApprovalConfig::default().with_solo_mode(true);
        let detector = DestructivePatternDetector::new(PathBuf::from("/tmp"));
        let verdict = detector.detect_with_config("rm -rf /", &config);
        // rm -rf / is Critical, and solo_honor_critical defaults to true
        assert!(matches!(verdict, SmartApprovalVerdict::NeedsApproval(_)));
    }

    #[tokio::test]
    async fn solo_mode_critical_bypass_when_not_honored() {
        let config = ApprovalConfig {
            solo_mode: true,
            solo_honor_critical: false,
            auto_pass_read_only: true,
            auto_pass_low_risk: true,
        };
        let detector = DestructivePatternDetector::new(PathBuf::from("/tmp"));
        let verdict = detector.detect_with_config("rm -rf /", &config);
        assert!(matches!(
            verdict,
            SmartApprovalVerdict::AutoPass {
                reason: AutoPassReason::SoloBypass
            }
        ));
    }

    #[tokio::test]
    async fn low_risk_auto_pass() {
        let config = ApprovalConfig::default();
        let detector = DestructivePatternDetector::new(PathBuf::from("/tmp"));
        // make clean matches Low risk pattern, "make" not in read-only list
        let verdict = detector.detect_with_config("make clean", &config);
        assert!(matches!(
            verdict,
            SmartApprovalVerdict::AutoPass {
                reason: AutoPassReason::LowRiskAutoPass
            }
        ));
    }

    #[tokio::test]
    async fn low_risk_needs_approval_when_disabled() {
        let config = ApprovalConfig {
            auto_pass_low_risk: false,
            ..ApprovalConfig::default()
        };
        let detector = DestructivePatternDetector::new(PathBuf::from("/tmp"));
        let verdict = detector.detect_with_config("make clean", &config);
        assert!(matches!(verdict, SmartApprovalVerdict::NeedsApproval(_)));
    }

    #[test]
    fn is_bash_tool_recognizes_known_names() {
        assert!(SmartApprovalGate::is_bash_tool("bash"));
        assert!(SmartApprovalGate::is_bash_tool("execute_bash"));
        assert!(SmartApprovalGate::is_bash_tool("shell"));
        assert!(!SmartApprovalGate::is_bash_tool("read_file"));
        assert!(!SmartApprovalGate::is_bash_tool("write_file"));
    }

    #[test]
    fn extract_command_from_json() {
        let cmd =
            SmartApprovalGate::extract_command(r#"{"command": "rm -rf /tmp"}"#);
        assert_eq!(cmd, "rm -rf /tmp");
    }

    #[test]
    fn extract_command_fallback_to_raw() {
        let cmd = SmartApprovalGate::extract_command("just a raw command");
        assert_eq!(cmd, "just a raw command");
    }
}
