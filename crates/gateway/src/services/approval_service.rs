use std::sync::Arc;

use approval::{ApprovalRepository, FileApprovalRepository};
use harness_contract::execution_graph::{ExecutionGraphCommand, ExecutionNodeStatus};
use harness_contract::policy::RiskGateReceipt;
use runtime::{ApprovalConfig, approval_gate::SmartApprovalGate};

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

    pub(crate) async fn pending(
        &self,
        principal: &runtime::VerifiedPrincipal,
    ) -> serde_json::Value {
        if let Some(services) = self.runtime_services.as_deref() {
            services.approval_queue().refresh();
        }
        let (projection, pending) = self.runtime_services.as_deref().map_or_else(
            || (serde_json::Value::Null, Vec::new()),
            |services| {
                let requests = services
                    .approval_queue()
                    .list()
                    .into_iter()
                    .filter(|request| approval_visible_to(request, principal))
                    .collect::<Vec<_>>();
                let pending = requests
                    .iter()
                    .filter(|request| request.status == runtime::GlobalApprovalStatus::Pending)
                    .cloned()
                    .collect::<Vec<_>>();
                (
                    serde_json::json!({
                        "kind": "runtime.global_approvals",
                        "count": requests.len(),
                        "pending_count": pending.len(),
                        "requests": requests,
                    }),
                    pending,
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

    pub(crate) async fn history(
        &self,
        limit: usize,
        offset: usize,
        principal: &runtime::VerifiedPrincipal,
    ) -> serde_json::Value {
        let mut combined = Vec::new();
        if let Some(services) = self.runtime_services.as_deref() {
            services.approval_queue().refresh();
            combined.extend(
                services
                    .approval_queue()
                    .list()
                    .into_iter()
                    .filter(|request| request.status != runtime::GlobalApprovalStatus::Pending)
                    .filter(|request| approval_visible_to(request, principal))
                    .filter_map(|request| serde_json::to_value(request).ok()),
            );
        }
        if let Some(repository) = &self.repository {
            if let Ok((history, _total)) = repository.list_history(limit.saturating_add(offset), 0)
            {
                if !history.is_empty() {
                    combined.extend(
                        history
                            .into_iter()
                            .filter_map(|entry| serde_json::to_value(entry).ok()),
                    );
                }
            }
        }
        if combined.is_empty() {
            combined.extend(
                match &self.gate {
                    Some(gate) => {
                        gate.history()
                            .list_history(limit.saturating_add(offset), 0)
                            .await
                            .0
                    }
                    None => Vec::new(),
                }
                .into_iter()
                .filter_map(|entry| serde_json::to_value(entry).ok()),
            );
        }
        combined.sort_by(|left, right| {
            let left_time = left
                .get("resolved_at_ms")
                .and_then(serde_json::Value::as_u64)
                .or_else(|| left.get("timestamp_ms").and_then(serde_json::Value::as_u64))
                .unwrap_or_default();
            let right_time = right
                .get("resolved_at_ms")
                .and_then(serde_json::Value::as_u64)
                .or_else(|| {
                    right
                        .get("timestamp_ms")
                        .and_then(serde_json::Value::as_u64)
                })
                .unwrap_or_default();
            right_time.cmp(&left_time)
        });
        serde_json::Value::Array(combined.into_iter().skip(offset).take(limit).collect())
    }

    /// Resolve one approval by canonical id without imposing the UI history
    /// page size.  Backlinks are durable identities, not a request to inspect
    /// only the most recent 200 records.
    pub(crate) async fn exact(
        &self,
        id: &str,
        principal: &runtime::VerifiedPrincipal,
    ) -> Option<serde_json::Value> {
        if let Some(services) = self.runtime_services.as_deref() {
            services.approval_queue().refresh();
            if let Some(request) = services
                .approval_queue()
                .list()
                .into_iter()
                .filter(|request| approval_visible_to(request, principal))
                .find(|request| request.approval_id == id)
            {
                return serde_json::to_value(request).ok();
            }
        }
        if let Some(repository) = &self.repository {
            if let Ok((history, _)) = repository.list_history(usize::MAX, 0) {
                if let Some(entry) = history
                    .into_iter()
                    .find(|entry| entry.id == id || entry.request_id == id)
                {
                    return serde_json::to_value(entry).ok();
                }
            }
        }
        if let Some(gate) = &self.gate {
            let (history, _) = gate.history().list_history(usize::MAX, 0).await;
            return history
                .into_iter()
                .find(|entry| entry.id == id || entry.request_id == id)
                .and_then(|entry| serde_json::to_value(entry).ok());
        }
        None
    }

    pub(crate) async fn respond(
        &self,
        id: &str,
        approved: bool,
        reason: Option<String>,
        principal: &runtime::VerifiedPrincipal,
    ) -> Result<serde_json::Value, String> {
        if !principal.is_human_interactive() || !principal.has_capability("approval.respond") {
            return Err("approval_human_interactive_capability_required".to_string());
        }
        let services = self.runtime_services()?;
        if services
            .approval_queue()
            .get(id)
            .is_some_and(|request| request.source.kind == runtime::ApprovalSourceKind::Mfg)
        {
            return Err("mfg_review_requires_typed_decision_service".to_string());
        }
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
                principal,
                runtime::ApprovalDecisionCommand {
                    approval_id: id.to_string(),
                    approved,
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

fn approval_visible_to(
    request: &runtime::GlobalApprovalRequest,
    principal: &runtime::VerifiedPrincipal,
) -> bool {
    if request.source.kind != runtime::ApprovalSourceKind::Mfg {
        return true;
    }
    principal.is_human_interactive()
        && principal.has_capability("approval.respond")
        && principal.has_capability("mfg.report.review")
        && request
            .source
            .resource_ref
            .as_deref()
            .is_some_and(|resource| {
                principal.claims().scopes.iter().any(|scope| {
                    scope == "gateway"
                        || scope == resource
                        || resource.starts_with(&format!("{scope}:"))
                })
            })
}

fn canonical_graph_approval_target(approval_id: &str) -> Option<(String, String)> {
    runtime::execution_core::graph::executors::parse_graph_approval_id(approval_id)
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
        let approval_id =
            runtime::execution_core::graph::executors::graph_approval_id(&graph_id, &node_id);
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
        let approval_events = services
            .event_reader()
            .list_scope(runtime::RuntimeEventScope::Approval, 20)
            .unwrap();
        assert!(
            approval_events
                .iter()
                .any(|event| event.kind == "approval.submitted")
        );
        assert!(
            approval_events
                .iter()
                .any(|event| event.kind == "approval.decided")
        );
        assert_eq!(
            services
                .approval_queue()
                .get(&approval_id)
                .map(|request| request.status),
            Some(runtime::GlobalApprovalStatus::Approved)
        );
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

    fn review_principal(capabilities: &[&str]) -> runtime::VerifiedPrincipal {
        runtime::VerifiedPrincipal::from_test_claims(harness_contract::security::PrincipalClaims {
            principal_id: "reviewer".to_string(),
            kind: harness_contract::security::PrincipalKind::Human,
            scopes: vec!["gateway".to_string()],
            capabilities: capabilities
                .iter()
                .map(|capability| (*capability).to_string())
                .collect(),
            assurance: harness_contract::security::PrincipalAssurance::HumanInteractive,
            issuer: "test".to_string(),
            issued_at_ms: 1,
            expires_at_ms: None,
            credential_fingerprint: "test".to_string(),
            credential_epoch: 1,
            profile_revision: 1,
        })
    }

    #[tokio::test]
    async fn mfg_approval_is_cropped_and_generic_response_fails_before_decision_write() {
        let services = runtime::RuntimeServices::in_memory().unwrap();
        let request = services
            .approval_queue()
            .submit_scoped(
                "mfg-approval:crop",
                runtime::SubmitGlobalApprovalRequest {
                    source: runtime::ApprovalSource {
                        kind: runtime::ApprovalSourceKind::Mfg,
                        session_id: None,
                        agent_id: None,
                        team_id: None,
                        mission_id: None,
                        resource_ref: Some("mfg:cockpit-report:report-crop".to_string()),
                        review_ref: Some("review-crop".to_string()),
                    },
                    action: "mfg.report.review.typed_decision".to_string(),
                    summary: "review report".to_string(),
                    risk: harness_contract::core::TaskRisk::High,
                    evidence_refs: vec!["digest:crop".to_string()],
                    timeout_policy: runtime::ApprovalTimeoutPolicy::Pending,
                },
            )
            .unwrap();
        let service = ApprovalService::new().with_runtime_services(Arc::clone(&services));
        let operator = review_principal(&["approval.respond"]);
        let reviewer = review_principal(&["approval.respond", "mfg.report.review"]);
        assert_eq!(
            service.pending(&operator).await["pending"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            service.pending(&operator).await["approvals"]["requests"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            service.pending(&reviewer).await["pending"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            service
                .respond(&request.approval_id, true, None, &reviewer)
                .await
                .unwrap_err(),
            "mfg_review_requires_typed_decision_service"
        );
        assert_eq!(
            services
                .approval_queue()
                .get(&request.approval_id)
                .unwrap()
                .status,
            runtime::GlobalApprovalStatus::Pending
        );
    }
}
