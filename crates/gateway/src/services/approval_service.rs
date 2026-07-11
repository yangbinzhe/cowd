use std::sync::Arc;

use approval::{ApprovalRepository, FileApprovalRepository};
use harness_contract::execution_graph::{ExecutionGraphCommand, ExecutionNodeStatus};
use harness_contract::policy::RiskGateReceipt;
use runtime::{approval_gate::SmartApprovalGate, ApprovalConfig};

use super::ServiceEnvelope;

#[derive(Clone)]
pub(crate) struct ApprovalService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    gate: Option<Arc<SmartApprovalGate>>,
    repository: Option<FileApprovalRepository>,
    runtime_services: Option<Arc<runtime::RuntimeServices>>,
}

impl ApprovalService {
    pub(crate) fn new() -> Self {
        Self {
            label: "approval",
            owner: "0.9.296 Approval service boundary",
            gate: None,
            repository: None,
            runtime_services: None,
        }
    }

    pub(crate) fn with_runtime_services(
        mut self,
        runtime_services: Arc<runtime::RuntimeServices>,
    ) -> Self {
        self.runtime_services = Some(Arc::clone(&runtime_services));
        self
    }

    fn runtime_services(&self) -> Result<&runtime::RuntimeServices, String> {
        self.runtime_services
            .as_deref()
            .ok_or_else(|| "runtime services are not configured".to_string())
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
        if let Some(services) = self.runtime_services.as_deref() {
            services.approval_queue().refresh();
        }
        let (projection, pending) = self.runtime_services.as_deref().map_or_else(
            || (serde_json::Value::Null, Vec::new()),
            |services| {
                (
                    services.approval_queue().projection(),
                    services.approval_queue().pending(),
                )
            },
        );
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
        let services = self.runtime_services()?;
        let graph_target = canonical_graph_approval_target(id);
        let graph_before = if let Some((graph_id, node_id)) = &graph_target {
            let graph = services
                .graph_state_store()
                .load_async(graph_id.clone())
                .await
                .map_err(|error| format!("approval_graph_not_found: {error}"))?;
            match graph.node_statuses.get(node_id) {
                Some(ExecutionNodeStatus::WaitingApproval) => {}
                Some(ExecutionNodeStatus::Completed) if approved => {
                    return Ok(serde_json::json!({
                        "id": id,
                        "resolved": true,
                        "approved": true,
                        "status": "already_applied",
                        "graph_id": graph_id,
                        "node_id": node_id,
                        "graph_revision": graph.revision,
                    }));
                }
                Some(ExecutionNodeStatus::Cancelled) if !approved => {
                    return Ok(serde_json::json!({
                        "id": id,
                        "resolved": true,
                        "approved": false,
                        "status": "already_applied",
                        "graph_id": graph_id,
                        "node_id": node_id,
                        "graph_revision": graph.revision,
                    }));
                }
                Some(status) => {
                    return Err(format!(
                        "approval_invalid_state: graph `{graph_id}` node `{node_id}` is {status:?}"
                    ));
                }
                None => {
                    return Err(format!(
                        "approval_node_not_found: graph `{graph_id}` has no node `{node_id}`"
                    ));
                }
            }
            Some(graph)
        } else {
            None
        };
        let decision_reason = reason.unwrap_or_else(|| {
            if approved {
                "approved via gateway approval API".to_string()
            } else {
                "denied via gateway approval API".to_string()
            }
        });
        let graph_receipt =
            if let (Some((graph_id, node_id)), Some(graph)) = (graph_target, graph_before) {
                let command = ExecutionGraphCommand::SubmitApproval {
                    expected_revision: graph.revision,
                    node_id: node_id.clone(),
                    approved,
                    decision_ref: format!("approval-decision:{id}"),
                };
                let graph = services
                    .graph_runner()
                    .command(&graph_id, command)
                    .await
                    .map_err(|error| format!("approval_graph_command_failed: {error}"))?;
                services.approval_queue().refresh();
                Some(serde_json::json!({
                    "graph_id": graph_id,
                    "node_id": node_id,
                    "revision": graph.revision,
                    "node_status": graph.node_statuses.get(&node_id),
                }))
            } else {
                None
            };
        let receipt = if graph_receipt.is_some() {
            services.approval_queue().refresh();
            serde_json::to_value(
                services
                    .approval_queue()
                    .get(id)
                    .ok_or_else(|| format!("approval_projection_missing: {id}"))?,
            )
            .map_err(|error| error.to_string())?
        } else {
            serde_json::to_value(services.approval_queue().decide(
                runtime::GlobalApprovalDecision {
                    approval_id: id.to_string(),
                    approved,
                    decided_by: "human".to_string(),
                    reason: decision_reason,
                },
            )?)
            .map_err(|error| error.to_string())?
        };
        Ok(serde_json::json!({
            "id": id,
            "resolved": true,
            "approved": approved,
            "receipt": receipt,
            "execution_graph": graph_receipt,
            "approvals": services.approval_queue().projection(),
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

fn canonical_graph_approval_target(approval_id: &str) -> Option<(String, String)> {
    let rest = approval_id.strip_prefix("approval:")?;
    let (graph_id, node_id) = rest.split_once(':')?;
    Some((graph_id.to_string(), node_id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::execution_graph::{ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec};
    use runtime::ExecutionGraphHost;

    #[tokio::test]
    async fn approval_decision_commits_graph_and_approval_stream_together() {
        let services = runtime::RuntimeServices::in_memory().unwrap();
        let mut graph = ExecutionGraph::new("approval reconciliation");
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::Approval,
            "approval",
            serde_json::json!({
                "action": "channel.send",
                "summary": "approve dispatch",
            })
            .to_string(),
        );
        let node_id = node.id.clone();
        graph.nodes.push(node);
        let graph_id = graph.id.clone();
        let waiting = services
            .graph_runner()
            .submit_graph(
                graph,
                ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .unwrap()
            .graph;
        assert_eq!(
            waiting
                .nodes
                .iter()
                .find(|node| node.node_id == node_id)
                .map(|node| node.status),
            Some(ExecutionNodeStatus::WaitingApproval)
        );
        let approval_id = format!("approval:{graph_id}:{node_id}");
        services
            .graph_runner()
            .command(
                &graph_id,
                ExecutionGraphCommand::SubmitApproval {
                    expected_revision: waiting.revision,
                    node_id: node_id.clone(),
                    approved: true,
                    decision_ref: format!("approval-decision:{approval_id}"),
                },
            )
            .await
            .unwrap();
        services.approval_queue().refresh();
        let reconciled = services
            .graph_state_store()
            .load_async(graph_id)
            .await
            .unwrap();
        assert_eq!(
            reconciled.node_statuses.get(&node_id),
            Some(&ExecutionNodeStatus::Completed)
        );
        assert_eq!(
            services.approval_queue().get(&approval_id).unwrap().status,
            runtime::GlobalApprovalStatus::Approved
        );
    }
}
