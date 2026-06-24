use std::sync::Arc;

use ai_kernel::policy::RiskGateReceipt;
use approval::{ApprovalRepository, FileApprovalRepository};
use runtime::{approval_gate::SmartApprovalGate, ApprovalConfig};

use super::ServiceEnvelope;

#[derive(Clone)]
pub(crate) struct ApprovalService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    gate: Option<Arc<SmartApprovalGate>>,
    repository: Option<FileApprovalRepository>,
}

impl ApprovalService {
    pub(crate) fn new() -> Self {
        Self {
            label: "approval",
            owner: "0.9.296 Approval service boundary",
            gate: None,
            repository: None,
        }
    }

    pub(crate) fn with_gate_and_repository(
        gate: Arc<SmartApprovalGate>,
        repository: FileApprovalRepository,
    ) -> Self {
        Self {
            gate: Some(gate),
            repository: Some(repository),
            ..Self::new()
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: if self.gate.is_some() {
                "service_ready"
            } else {
                "service_boundary_ready"
            },
            owner: self.owner,
            boundary_status: "0620_final_boundary",
        }
    }

    pub(crate) fn is_configured(&self) -> bool {
        self.gate.is_some()
    }

    pub(crate) async fn pending(&self) -> serde_json::Value {
        let projection = runtime::global_approval_queue().projection();
        let pending = runtime::global_approval_queue().pending();
        serde_json::json!({
            "kind": "gateway.unified_approval_pending",
            "pending": pending,
            "approvals": projection,
        })
    }

    pub(crate) async fn config(&self) -> ApprovalConfig {
        match &self.gate {
            Some(gate) => gate.config().read().await.clone(),
            None => ApprovalConfig::default(),
        }
    }

    pub(crate) async fn update_config(&self, config: ApprovalConfig) -> ApprovalConfig {
        if let Some(gate) = &self.gate {
            gate.update_config(config.clone()).await;
        }
        config
    }

    pub(crate) async fn toggle_solo(&self) -> ApprovalConfig {
        let mut cfg = self.config().await;
        cfg.solo_mode = !cfg.solo_mode;
        self.update_config(cfg).await
    }

    pub(crate) async fn history(&self, limit: usize, offset: usize) -> serde_json::Value {
        if let Some(repository) = &self.repository {
            if let Ok((history, _total)) = repository.list_history(limit, offset) {
                if !history.is_empty() {
                    return serde_json::json!(history);
                }
            }
        }
        let history = match &self.gate {
            Some(gate) => gate.history().list_history(limit, offset).await.0,
            None => Vec::new(),
        };
        serde_json::json!(history)
    }

    pub(crate) async fn respond(
        &self,
        id: &str,
        approved: bool,
        reason: Option<String>,
    ) -> Result<serde_json::Value, String> {
        let receipt = runtime::global_approval_queue().decide(runtime::GlobalApprovalDecision {
            approval_id: id.to_string(),
            approved,
            decided_by: "human".to_string(),
            reason: reason.unwrap_or_else(|| {
                if approved {
                    "approved via gateway approval API".to_string()
                } else {
                    "denied via gateway approval API".to_string()
                }
            }),
        })?;
        Ok(serde_json::json!({
            "id": id,
            "resolved": true,
            "approved": approved,
            "receipt": receipt,
            "approvals": runtime::global_approval_queue().projection(),
        }))
    }

    pub(crate) async fn risk_receipt(
        &self,
        tool_name: &str,
        input: &str,
    ) -> Result<RiskGateReceipt, String> {
        let gate = self
            .gate
            .as_ref()
            .ok_or_else(|| "approval gate not configured".to_string())?;
        Ok(gate.policy_receipt(tool_name, input).await)
    }
}
