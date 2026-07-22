//! Smart Approval Gate — intelligent command approval with SOLO mode support.
//!
//! This module provides the `SmartApprovalGate` which sits between the
//! destructive pattern detector and the conversation runtime, applying
//! approval configuration (SOLO mode, auto-pass settings) and managing
//! the blocking approval flow with the frontend via SSE + oneshot channels.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, RwLock};

#[cfg(test)]
use approval::SqliteApprovalHistoryLedger;
use approval::{ApprovalHistoryEntry, ApprovalHistoryOutcome, SharedApprovalHistoryLedger};

use harness_contract::policy::{
    PermissionOperation, PermissionResource, PermissionScope,
    PolicyDecisionKind as KernelPolicyDecisionKind, RiskAssessment, RiskGateReceipt,
    RiskLevel as KernelRiskLevel,
};

use crate::config::ApprovalConfig;
use crate::permission_enforcer::{
    ApprovalPersistence, ApprovalRequest, ApprovalVerdict, AutoPassReason,
    DestructivePatternDetector, RiskLevel as RuntimeRiskLevel, SmartApprovalVerdict,
};
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
    history: SharedApprovalHistoryLedger,
    session_approved: Arc<tokio::sync::Mutex<HashSet<String>>>,
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
        history: SharedApprovalHistoryLedger,
    ) -> Self {
        Self {
            detector,
            config: Arc::new(RwLock::new(config)),
            pending: Arc::new(RwLock::new(HashMap::new())),
            sse_sender: None,
            history,
            session_approved: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
        }
    }

    /// Set the SSE sender for pushing approval events to the frontend.
    #[must_use]
    pub fn with_sse_sender(mut self, sender: Arc<dyn ApprovalSseSender>) -> Self {
        self.sse_sender = Some(sender);
        self
    }

    /// Get a reference to the approval config (for API endpoints).
    pub fn config(&self) -> &Arc<RwLock<ApprovalConfig>> {
        &self.config
    }

    /// Get a reference to the pending approvals map (for API endpoints).
    pub fn pending(
        &self,
    ) -> &Arc<RwLock<HashMap<String, (ApprovalRequest, oneshot::Sender<ApprovalVerdict>)>>> {
        &self.pending
    }

    /// Get a reference to the destructive pattern detector.
    pub fn detector(&self) -> &Arc<DestructivePatternDetector> {
        &self.detector
    }

    /// Get a reference to the approval history store (for API endpoints).
    pub fn history(&self) -> &SharedApprovalHistoryLedger {
        &self.history
    }

    fn history_entry(
        request: &ApprovalRequest,
        outcome: ApprovalHistoryOutcome,
    ) -> ApprovalHistoryEntry {
        ApprovalHistoryEntry {
            id: format!("ah_{}", uuid::Uuid::new_v4()),
            request_id: request.id.clone(),
            command: request.command.clone(),
            normalized_command: request.normalized_command.clone(),
            risk_level: format!("{:?}", request.risk_level).to_lowercase(),
            matched_patterns: request.matched_patterns.clone(),
            outcome,
            resolved_at: chrono::Utc::now().to_rfc3339(),
        }
    }

    fn record_history(&self, entry: ApprovalHistoryEntry) -> Result<(), String> {
        self.history
            .append(entry)
            .map_err(|error| error.to_string())
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
        let input_preview: String = input.chars().take(80).collect();
        let key = format!("{tool_name}:{input_preview}");
        if self.session_approved.lock().await.contains(&key) {
            return ApprovalGateResult::AutoPass {
                reason: AutoPassReason::ReadOnlyCommand,
            };
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
            SmartApprovalVerdict::NeedsApproval(request) => self.request_approval(request).await,
        }
    }

    /// Require a concrete user approval for a strategy-leased critical action.
    ///
    /// Unlike [`Self::evaluate`], this does not infer safety from the tool name.
    /// The strategy engine has already classified the whole operation as
    /// critical, so an auto-pass is only valid when SOLO mode explicitly opts
    /// out of honoring critical approvals.
    pub async fn require_explicit_approval(&self, action: &str, input: &str) -> ApprovalGateResult {
        let approval_key = explicit_strategy_approval_key(action, input);
        if self.session_approved.lock().await.contains(&approval_key) {
            return ApprovalGateResult::AutoPass {
                reason: AutoPassReason::CachedApproval {
                    persistence: ApprovalPersistence::Session,
                },
            };
        }
        let config = self.config.read().await;
        if config.solo_mode && !config.solo_honor_critical {
            return ApprovalGateResult::AutoPass {
                reason: AutoPassReason::SoloBypass,
            };
        }
        drop(config);

        let command = input.chars().take(2_000).collect::<String>();
        let request = ApprovalRequest {
            id: format!("strategy-approval-{}", uuid::Uuid::new_v4()),
            command: command.clone(),
            normalized_command: command,
            risk_level: RuntimeRiskLevel::Critical,
            matched_patterns: vec![format!("runtime_strategy_critical_operation:{action}")],
            description: format!("Critical runtime strategy action requires approval: {action}"),
            timestamp: chrono::Utc::now(),
            timeout_secs: 120,
        };
        self.request_approval(request).await
    }

    /// Return a kernel-level risk receipt without blocking for user approval.
    pub async fn policy_receipt(&self, tool_name: &str, input: &str) -> RiskGateReceipt {
        let scope = approval_permission_scope(tool_name, input);
        if READ_ONLY_TOOLS.contains(&tool_name) {
            return risk_gate_receipt(
                scope,
                KernelRiskLevel::Low,
                KernelPolicyDecisionKind::Allow,
                false,
                "read-only tool auto-pass",
            );
        }
        if !Self::is_bash_tool(tool_name) {
            return risk_gate_receipt(
                scope,
                KernelRiskLevel::Low,
                KernelPolicyDecisionKind::Allow,
                false,
                "non-shell tool delegates to its permission boundary",
            );
        }

        let command = Self::extract_command(input);
        let config = self.config.read().await;
        let verdict = self.detector.detect_with_config(&command, &config);
        drop(config);

        match verdict {
            SmartApprovalVerdict::AutoPass { reason } => risk_gate_receipt(
                scope,
                KernelRiskLevel::Low,
                KernelPolicyDecisionKind::Allow,
                false,
                format!("approval gate auto-pass: {reason:?}"),
            ),
            SmartApprovalVerdict::NeedsApproval(request) => {
                let risk = runtime_risk_to_kernel(request.risk_level);
                let decision = if risk == KernelRiskLevel::Critical {
                    KernelPolicyDecisionKind::Escalate
                } else {
                    KernelPolicyDecisionKind::Ask
                };
                risk_gate_receipt(
                    scope,
                    risk,
                    decision,
                    true,
                    format!(
                        "approval required for patterns: {}",
                        request.matched_patterns.join(", ")
                    ),
                )
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
                let entry = Self::history_entry(
                    &request,
                    ApprovalHistoryOutcome::Denied {
                        reason: "Approval channel closed".to_string(),
                    },
                );
                if let Err(error) = self.record_history(entry) {
                    tracing::error!(%request_id, %error, "approval channel-close denial was not persisted");
                }
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
                let entry = Self::history_entry(&request, ApprovalHistoryOutcome::TimedOut);
                if let Err(error) = self.record_history(entry) {
                    tracing::error!(%request_id, %error, "approval timeout was not persisted");
                }
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

    /// Resolve a pending approval request (called by the API endpoint handler).
    ///
    /// Returns the ApprovalRequest if found, or None if expired.
    pub async fn resolve_approval(
        &self,
        request_id: &str,
        verdict: ApprovalVerdict,
        persistence: ApprovalPersistence,
    ) -> Option<ApprovalRequest> {
        // Claim the one-shot request under the short-lived map lock, then do
        // durable I/O and policy work after releasing it.  A history backend
        // stall must not serialize unrelated approval decisions in this
        // Gateway process.
        let pending = { self.pending.write().await.remove(request_id) };
        if let Some((request, sender)) = pending {
            let history_outcome = match &verdict {
                ApprovalVerdict::Approved => ApprovalHistoryOutcome::Approved {
                    persistence: format!("{:?}", persistence).to_lowercase(),
                },
                ApprovalVerdict::Denied { reason } => ApprovalHistoryOutcome::Denied {
                    reason: reason.clone(),
                },
                ApprovalVerdict::TimedOut => ApprovalHistoryOutcome::TimedOut,
            };
            let mut effective_verdict = verdict;
            if let Err(error) = self.record_history(Self::history_entry(&request, history_outcome))
            {
                tracing::error!(%request_id, %error, "approval resolution receipt persistence failed");
                if matches!(effective_verdict, ApprovalVerdict::Approved) {
                    effective_verdict = ApprovalVerdict::Denied {
                        reason: "approval decision persistence failed; action was not approved"
                            .to_string(),
                    };
                }
            }

            if matches!(effective_verdict, ApprovalVerdict::Approved) {
                if let Err(error) = self
                    .detector
                    .record_approval(&request.command, persistence.clone())
                    .await
                {
                    // The manual decision is already durably recorded; an
                    // unavailable "always" policy artifact must not create a
                    // hidden bypass. The current action remains one explicit
                    // approval and the persistence failure is observable.
                    tracing::error!(%request_id, %error, "approval persistence policy was not applied");
                }
                if let Some(action) = request
                    .matched_patterns
                    .iter()
                    .find_map(|pattern| {
                        pattern.strip_prefix("runtime_strategy_critical_operation:")
                    })
                    .filter(|_| !matches!(&persistence, ApprovalPersistence::Once))
                {
                    self.session_approved
                        .lock()
                        .await
                        .insert(explicit_strategy_approval_key(action, &request.command));
                }
            }

            let _ = sender.send(effective_verdict.clone());
            if let Some(sse_sender) = &self.sse_sender {
                sse_sender.send_approval_resolved(request_id, &effective_verdict);
            }

            Some(request)
        } else {
            None
        }
    }

    /// Get all pending approval requests (for GET /api/approval/pending).
    pub async fn get_pending_requests(&self) -> Vec<ApprovalRequest> {
        let pending = self.pending.read().await;
        pending.values().map(|(req, _)| req.clone()).collect()
    }
}

fn explicit_strategy_approval_key(action: &str, input: &str) -> String {
    format!("{action}:{}", input.chars().take(512).collect::<String>())
}

fn approval_permission_scope(tool_name: &str, input: &str) -> PermissionScope {
    let mut scope = if SmartApprovalGate::is_bash_tool(tool_name) {
        PermissionScope::new(PermissionResource::Shell, PermissionOperation::Execute)
    } else if READ_ONLY_TOOLS.contains(&tool_name)
        && (tool_name.contains("file")
            || tool_name.contains("grep")
            || tool_name.contains("glob")
            || tool_name.contains("directory"))
    {
        PermissionScope::new(PermissionResource::File, PermissionOperation::Read)
    } else if READ_ONLY_TOOLS.contains(&tool_name) && tool_name.contains("web") {
        PermissionScope::new(PermissionResource::Network, PermissionOperation::Read)
    } else if tool_name.contains("file") {
        PermissionScope::new(PermissionResource::File, PermissionOperation::Write)
    } else {
        PermissionScope::new(PermissionResource::Tool, PermissionOperation::Call)
    };
    scope.target = Some(SmartApprovalGate::extract_command(input));
    scope
}

fn runtime_risk_to_kernel(risk: RuntimeRiskLevel) -> KernelRiskLevel {
    match risk {
        RuntimeRiskLevel::Low => KernelRiskLevel::Low,
        RuntimeRiskLevel::Medium => KernelRiskLevel::Medium,
        RuntimeRiskLevel::High => KernelRiskLevel::High,
        RuntimeRiskLevel::Critical => KernelRiskLevel::Critical,
    }
}

fn risk_gate_receipt(
    scope: PermissionScope,
    risk: KernelRiskLevel,
    decision: KernelPolicyDecisionKind,
    approval_required: bool,
    reason: impl Into<String>,
) -> RiskGateReceipt {
    RiskGateReceipt {
        scope,
        risk: RiskAssessment {
            level: risk,
            reasons: vec![reason.into()],
            assessed_at: chrono::Utc::now(),
        },
        decision,
        approval_required,
        issued_at: chrono::Utc::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approval::{ApprovalHistoryError, ApprovalHistoryLedger, ApprovalHistoryResult};
    use std::path::PathBuf;

    #[derive(Debug)]
    struct FailingHistoryLedger;

    impl ApprovalHistoryLedger for FailingHistoryLedger {
        fn list(
            &self,
            _limit: usize,
            _offset: usize,
        ) -> ApprovalHistoryResult<(Vec<ApprovalHistoryEntry>, usize)> {
            Err(ApprovalHistoryError::Backend(
                "test ledger unavailable".to_string(),
            ))
        }

        fn get(&self, _id: &str) -> ApprovalHistoryResult<Option<ApprovalHistoryEntry>> {
            Err(ApprovalHistoryError::Backend(
                "test ledger unavailable".to_string(),
            ))
        }

        fn append(&self, _entry: ApprovalHistoryEntry) -> ApprovalHistoryResult<()> {
            Err(ApprovalHistoryError::Backend(
                "test ledger unavailable".to_string(),
            ))
        }
    }

    fn make_gate(config: ApprovalConfig) -> SmartApprovalGate {
        let detector = Arc::new(DestructivePatternDetector::new(std::path::PathBuf::from(
            "/tmp",
        )));
        let history: SharedApprovalHistoryLedger =
            Arc::new(SqliteApprovalHistoryLedger::in_memory().expect("in-memory approval ledger"));
        SmartApprovalGate::new(detector, config, history)
    }

    #[tokio::test]
    async fn read_only_tools_auto_pass() {
        let gate = make_gate(ApprovalConfig::default());
        let result = gate
            .evaluate("read_file", r#"{"path": "/etc/passwd"}"#)
            .await;
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
    async fn explicit_strategy_approval_waits_for_a_real_decision() {
        let gate = Arc::new(make_gate(ApprovalConfig::default()));
        let pending_gate = Arc::clone(&gate);
        let approval = tokio::spawn(async move {
            pending_gate
                .require_explicit_approval(
                    "runtime_strategy_tool_batch",
                    r#"{"tool":"write_file"}"#,
                )
                .await
        });

        let request = loop {
            if let Some(request) = gate.get_pending_requests().await.into_iter().next() {
                break request;
            }
            tokio::task::yield_now().await;
        };
        gate.resolve_approval(
            &request.id,
            ApprovalVerdict::Approved,
            ApprovalPersistence::Session,
        )
        .await
        .expect("pending strategy approval should resolve");

        assert!(matches!(
            approval.await.expect("approval task should join"),
            ApprovalGateResult::Approved { .. }
        ));
        assert!(matches!(
            gate.require_explicit_approval(
                "runtime_strategy_tool_batch",
                r#"{"tool":"write_file"}"#,
            )
            .await,
            ApprovalGateResult::AutoPass {
                reason: AutoPassReason::CachedApproval { .. }
            }
        ));
    }

    #[tokio::test]
    async fn denied_approval_is_never_cached_as_allowed() {
        let gate = Arc::new(make_gate(ApprovalConfig::default()));
        let pending_gate = Arc::clone(&gate);
        let approval = tokio::spawn(async move {
            pending_gate
                .evaluate("bash", r#"{"command":"rm -rf /tmp/cowd-denied"}"#)
                .await
        });
        let request = loop {
            if let Some(request) = gate.get_pending_requests().await.into_iter().next() {
                break request;
            }
            tokio::task::yield_now().await;
        };
        gate.resolve_approval(
            &request.id,
            ApprovalVerdict::Denied {
                reason: "operator denied".to_string(),
            },
            ApprovalPersistence::Once,
        )
        .await
        .expect("pending approval should resolve");
        assert!(matches!(
            approval.await.expect("approval task should join"),
            ApprovalGateResult::Denied { .. }
        ));

        let verdict = gate
            .detector()
            .detect_with_config("rm -rf /tmp/cowd-denied", &ApprovalConfig::default());
        assert!(matches!(verdict, SmartApprovalVerdict::NeedsApproval(_)));
    }

    #[tokio::test]
    async fn read_only_tool_policy_receipt_allows_without_approval() {
        let gate = make_gate(ApprovalConfig::default());
        let receipt = gate
            .policy_receipt("read_file", r#"{"path": "/tmp/plan.md"}"#)
            .await;

        assert_eq!(receipt.decision, KernelPolicyDecisionKind::Allow);
        assert_eq!(receipt.risk.level, KernelRiskLevel::Low);
        assert!(!receipt.approval_required);
        assert_eq!(receipt.scope.resource, PermissionResource::File);
    }

    #[tokio::test]
    async fn destructive_shell_policy_receipt_requires_approval() {
        let gate = make_gate(ApprovalConfig {
            auto_pass_low_risk: false,
            solo_mode: false,
            ..ApprovalConfig::default()
        });
        let receipt = gate
            .policy_receipt("bash", r#"{"command": "rm -rf target"}"#)
            .await;

        assert!(matches!(
            receipt.decision,
            KernelPolicyDecisionKind::Ask | KernelPolicyDecisionKind::Escalate
        ));
        assert!(receipt.approval_required);
        assert_eq!(receipt.scope.resource, PermissionResource::Shell);
    }

    #[tokio::test]
    async fn bash_read_only_command_auto_passes() {
        let gate = make_gate(ApprovalConfig::default());
        let result = gate.evaluate("bash", r#"{"command": "ls -la"}"#).await;
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
        let result = gate.evaluate("bash", r#"{"command": "echo hello"}"#).await;
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
        let cmd = SmartApprovalGate::extract_command(r#"{"command": "rm -rf /tmp"}"#);
        assert_eq!(cmd, "rm -rf /tmp");
    }

    #[test]
    fn extract_command_fallback_to_raw() {
        let cmd = SmartApprovalGate::extract_command("just a raw command");
        assert_eq!(cmd, "just a raw command");
    }

    #[tokio::test]
    async fn approved_action_fails_closed_when_history_receipt_cannot_persist() {
        let history: SharedApprovalHistoryLedger = Arc::new(FailingHistoryLedger);
        let gate = Arc::new(SmartApprovalGate::new(
            Arc::new(DestructivePatternDetector::new(PathBuf::from("/tmp"))),
            ApprovalConfig::default(),
            history,
        ));
        let evaluating_gate = Arc::clone(&gate);
        let awaiting = tokio::spawn(async move {
            evaluating_gate
                .evaluate("bash", r#"{"command":"rm -rf /tmp/cowd-ledger-failure"}"#)
                .await
        });
        let request = loop {
            if let Some(request) = gate.get_pending_requests().await.into_iter().next() {
                break request;
            }
            tokio::task::yield_now().await;
        };
        gate.resolve_approval(
            &request.id,
            ApprovalVerdict::Approved,
            ApprovalPersistence::Once,
        )
        .await
        .expect("pending approval resolves to a durable fail-closed verdict");
        assert!(matches!(
            awaiting.await.expect("approval task joins"),
            ApprovalGateResult::Denied { .. }
        ));
    }
}
