// ── Daemon Prompter ────────────────────────────────────────────
// SocketPrompter bridges tool approval between daemon and TUI via Unix socket.

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::{mpsc, oneshot};

use runtime::permissions::{PermissionPromptDecision, PermissionPrompter, PermissionRequest};

/// Bridges tool approval between daemon and TUI via Unix socket.
pub struct SocketPrompter {
    tx: mpsc::UnboundedSender<String>,
    pending: Mutex<HashMap<String, oneshot::Sender<PermissionPromptDecision>>>,
}

impl SocketPrompter {
    pub fn new(tx: mpsc::UnboundedSender<String>) -> Self {
        Self {
            tx,
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Called by daemon's handle_unix_client when TUI sends tool_approve/deny
    pub fn handle_response(&self, id: &str, approved: bool) {
        if let Some(tx) = self.pending.lock().unwrap().remove(id) {
            let decision = if approved {
                PermissionPromptDecision::Allow
            } else {
                PermissionPromptDecision::Deny {
                    reason: "denied by user".into(),
                }
            };
            let _ = tx.send(decision);
        }
    }
}

impl PermissionPrompter for SocketPrompter {
    fn decide(&mut self, request: &PermissionRequest) -> PermissionPromptDecision {
        let id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        self.pending.lock().unwrap().insert(id.clone(), tx);

        // Send approval request to TUI via socket
        let msg = serde_json::json!({
            "type": "ApprovalRequested",
            "tool": request.tool_name,
            "params": request.input,
            "required_mode": format!("{:?}", request.required_mode),
            "id": id
        });
        let _ = self.tx.send(msg.to_string());

        // Block waiting for TUI response (safe: called from spawn_blocking context)
        rx.blocking_recv()
            .unwrap_or(PermissionPromptDecision::Deny {
                reason: "prompter dropped".into(),
            })
    }
}
